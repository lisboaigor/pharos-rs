//! Concurrency regression tests for `SagaRunner` and `PgSagaStore` against
//! a real PostgreSQL store: the optimistic-concurrency check on `save`, the
//! terminal-status guard in `SagaRunner::handle`, and `claim_due`'s lease.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use chrono::Utc;
use pharos_postgres::{PgSagaStore, Pool, connect_pool, migrate_postgres_saga_schema};
use pharos_saga::{
    CommandDispatcher, Saga, SagaInstance, SagaRunner, SagaSaveError, SagaStatus, SagaStore,
    SagaTransition,
};
use serde::{Deserialize, Serialize};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::{ContainerAsync, GenericImage, ImageExt, runners::AsyncRunner};

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

async fn start_postgres()
-> Result<(ContainerAsync<GenericImage>, Pool), Box<dyn std::error::Error + Send + Sync>> {
    let container = GenericImage::new("postgres", "16-alpine")
        .with_exposed_port(5432.tcp())
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_PASSWORD", "postgres")
        .start()
        .await?;
    let host = container.get_host().await?.to_string();
    let port = container.get_host_port_ipv4(5432).await?;
    let pool = connect_pool(
        &format!("postgres://postgres:postgres@{host}:{port}/postgres"),
        16,
    )?;
    Ok((container, pool))
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct EscrowState {
    credited: u64,
}

#[derive(Debug, Clone)]
struct Credited {
    saga_id: String,
    amount: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Payout(u64);

#[derive(Debug, thiserror::Error)]
#[error("saga error")]
struct SagaFailure;

/// Reacts by adding to the running total. `gate` makes both concurrent
/// reactions read the same state before either writes on their *first*
/// attempt — the interleaving a two-consumer deployment produces on its
/// own, made deterministic. Only the first call waits: `SagaRunner::handle`
/// retries `react` against freshly reloaded state after a lost
/// compare-and-swap, and only one of the two racers ever retries, so a
/// second wait would block forever waiting for a partner that never
/// arrives.
struct EscrowSaga {
    gate: Arc<tokio::sync::Barrier>,
    first_call: std::sync::atomic::AtomicBool,
}

impl EscrowSaga {
    fn new(gate: Arc<tokio::sync::Barrier>) -> Self {
        Self {
            gate,
            first_call: std::sync::atomic::AtomicBool::new(true),
        }
    }
}

impl Saga for EscrowSaga {
    type Id = String;
    type State = EscrowState;
    type Event = Credited;
    type Command = Payout;
    type Error = SagaFailure;

    fn id_for(&self, event: &Self::Event) -> Option<Self::Id> {
        Some(event.saga_id.clone())
    }

    async fn react(
        &self,
        state: Option<&SagaInstance<Self::Id, Self::State>>,
        event: &Self::Event,
    ) -> Result<SagaTransition<Self::State, Self::Command>, Self::Error> {
        let current = state.map(|instance| instance.state.credited).unwrap_or(0);
        if self.first_call.swap(false, Ordering::SeqCst) {
            // Both handlers have now loaded; neither has saved.
            self.gate.wait().await;
        }
        Ok(SagaTransition::Advance {
            state: EscrowState {
                credited: current + event.amount,
            },
            commands: vec![Payout(event.amount)],
            deadline: None,
        })
    }
}

#[derive(Default)]
struct CountingDispatcher {
    dispatched: AtomicU64,
}

impl CommandDispatcher<Payout> for CountingDispatcher {
    type Error = SagaFailure;

    async fn dispatch(&self, _command: Payout) -> Result<(), SagaFailure> {
        self.dispatched.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

/// `CommandDispatcher` has no blanket impl for `Arc<D>` either.
struct SharedDispatcher(Arc<CountingDispatcher>);

impl CommandDispatcher<Payout> for SharedDispatcher {
    type Error = SagaFailure;

    async fn dispatch(&self, command: Payout) -> Result<(), SagaFailure> {
        self.0.dispatch(command).await
    }
}

struct SharedStore(Arc<PgSagaStore<String, EscrowState>>);

impl SagaStore<String, EscrowState> for SharedStore {
    type Error = pharos_postgres::PostgresSagaStoreError;

    async fn load(
        &self,
        id: &String,
    ) -> Result<Option<SagaInstance<String, EscrowState>>, Self::Error> {
        self.0.load(id).await
    }

    async fn save(
        &self,
        instance: SagaInstance<String, EscrowState>,
    ) -> Result<(), SagaSaveError<Self::Error>> {
        self.0.save(instance).await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_concurrent_events_for_one_saga_both_land_via_compare_and_swap() -> TestResult {
    let (_container, pool) = start_postgres().await?;
    migrate_postgres_saga_schema(&pool).await?;

    let store = Arc::new(PgSagaStore::<String, EscrowState>::with_saga_type(
        pool.clone(),
        "escrow",
    ));
    let saga_id = "escrow-1".to_string();

    // Seed a running instance, the way a first event would have.
    store
        .save(SagaInstance::running(
            saga_id.clone(),
            EscrowState { credited: 0 },
        ))
        .await?;

    let gate = Arc::new(tokio::sync::Barrier::new(2));
    let dispatcher = Arc::new(CountingDispatcher::default());

    let mut handles = Vec::new();
    for amount in [10u64, 5u64] {
        let runner = SagaRunner::new(
            EscrowSaga::new(Arc::clone(&gate)),
            SharedStore(Arc::clone(&store)),
            SharedDispatcher(Arc::clone(&dispatcher)),
        );
        let saga_id = saga_id.clone();
        handles.push(tokio::spawn(async move {
            runner
                .handle(&Credited { saga_id, amount })
                .await
                .map_err(|e| e.to_string())
        }));
    }
    for handle in handles {
        handle
            .await?
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { e.into() })?;
    }

    let Some(final_state) = store.load(&saga_id).await? else {
        panic!("the saga instance should exist");
    };
    println!(
        "credited={} (expected 15) status={:?} payouts_dispatched={}",
        final_state.state.credited,
        final_state.status,
        dispatcher.dispatched.load(Ordering::SeqCst)
    );

    assert_eq!(
        final_state.state.credited, 15,
        "both events must be reflected in the saga state: `save`'s compare-and-swap makes \
         the losing writer's `handle` reload and retry against the winner's committed state \
         instead of silently overwriting it"
    );
    assert_eq!(
        dispatcher.dispatched.load(Ordering::SeqCst),
        2,
        "both Payout commands were dispatched exactly once each"
    );
    Ok(())
}

/// The timeout sweeper's claim bumps `version` in the same statement as
/// `save`, so both participate in the same compare-and-swap: a concurrent
/// event handler's stale save cannot silently clobber the claim's lease.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_event_handlers_stale_save_conflicts_with_a_timeout_sweepers_claim() -> TestResult {
    use pharos_saga::SagaTimeoutStore;

    let (_container, pool) = start_postgres().await?;
    migrate_postgres_saga_schema(&pool).await?;

    let store = PgSagaStore::<String, EscrowState>::with_saga_type(pool.clone(), "escrow");
    let saga_id = "escrow-2".to_string();

    store
        .save(SagaInstance::running_until(
            saga_id.clone(),
            EscrowState { credited: 1 },
            Utc::now() - chrono::Duration::seconds(10),
        ))
        .await?;

    // A sweeper claims the due instance with a 60s lease. `claim_due` bumps
    // `version` in the same statement, exactly like `save` does, so it
    // participates in the same compare-and-swap.
    let claimed = store
        .claim_due(Utc::now(), chrono::Duration::seconds(60), 10)
        .await?;
    assert_eq!(claimed.len(), 1, "the sweeper claimed the due instance");

    // Meanwhile an event for the same saga is handled elsewhere, working off
    // a snapshot loaded *before* the claim (the realistic race: the event
    // handler's own `load` ran concurrently with the sweeper's claim and saw
    // the pre-claim version). Saving against that stale version must not
    // silently clobber the claim's lease.
    let stale = SagaInstance {
        version: claimed[0].version - 1,
        ..SagaInstance::running(saga_id.clone(), EscrowState { credited: 99 })
    };
    let result = store.save(stale).await;
    let Err(SagaSaveError::ConcurrencyConflict { expected, actual }) = result else {
        panic!("expected a ConcurrencyConflict, got {result:?}");
    };
    println!("stale save correctly rejected: expected={expected} actual={actual:?}");
    assert_eq!(actual, Some(claimed[0].version));

    // The lease still holds: nothing is due again while the sweeper is still
    // processing it.
    let claimed_again = store
        .claim_due(Utc::now(), chrono::Duration::seconds(60), 10)
        .await?;
    assert!(
        claimed_again.is_empty(),
        "the sweeper's lease must still be in force"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_late_duplicate_event_cannot_resurrect_a_completed_saga() -> TestResult {
    let (_container, pool) = start_postgres().await?;
    migrate_postgres_saga_schema(&pool).await?;

    let store = PgSagaStore::<String, EscrowState>::with_saga_type(pool.clone(), "escrow");
    let saga_id = "escrow-3".to_string();

    let mut done = SagaInstance::running(saga_id.clone(), EscrowState { credited: 100 });
    done.status = SagaStatus::Completed;
    store.save(done).await?;

    // A duplicate delivery of an earlier event reaches a runner: `react`
    // (which does not itself check `status`) is handed the completed
    // instance and computes an ordinary `Advance` — the runner must refuse
    // to apply it rather than reviving a terminal saga.
    let dispatcher = Arc::new(CountingDispatcher::default());
    let runner = SagaRunner::new(
        EscrowSaga::new(Arc::new(tokio::sync::Barrier::new(1))),
        SharedStore(Arc::new(PgSagaStore::with_saga_type(
            pool.clone(),
            "escrow",
        ))),
        SharedDispatcher(Arc::clone(&dispatcher)),
    );
    runner
        .handle(&Credited {
            saga_id: saga_id.clone(),
            amount: 7,
        })
        .await
        .map_err(|e| e.to_string())?;

    let Some(after) = store.load(&saga_id).await? else {
        panic!("the saga instance should exist");
    };
    println!(
        "after a duplicate event on a completed saga: status={:?} credited={}",
        after.status, after.state.credited
    );
    assert_eq!(
        after.status,
        SagaStatus::Completed,
        "a redelivered event must not move a Completed saga back to Running"
    );
    assert_eq!(
        after.state.credited, 100,
        "the terminal state must be untouched"
    );
    assert_eq!(
        dispatcher.dispatched.load(Ordering::SeqCst),
        0,
        "no Payout should be dispatched for a transition the runner refused"
    );
    Ok(())
}
