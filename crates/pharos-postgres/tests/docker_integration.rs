use chrono::{DateTime, Utc};
use pharos_app::{
    DeadLetterMessage, DeadLetterQueue, IdempotencyDecision, InboxStore, Message, OutboxMessage,
    OutboxRepository, TenantContext,
};
use pharos_core::{
    AggregateEvents, AggregateRoot, DomainEvent, Entity, Repository, RepositoryError,
};
use pharos_es::{EventSourced, EventSourcedRepository, EventStore, Snapshot, SnapshotStore};
use pharos_postgres::{
    PgEventStore, PgSagaStore, PgSnapshotStore, Pool, PostgresDeadLetterQueue, PostgresInboxStore,
    PostgresJsonRepository, PostgresOutboxRepository, PostgresTransactionError, PostgresUnitOfWork,
    SaveAndEnqueueError,
    TenantJsonRepository, connect_pool, migrate_postgres_aggregate_schema,
    migrate_postgres_dead_letter_schema, migrate_postgres_eventing_schema,
    migrate_postgres_tenant_aggregate_schema, save_aggregate_and_enqueue, save_and_enqueue_in,
};
use pharos_saga::{SagaInstance, SagaStatus, SagaStore, SagaTimeoutStore};
use serde::{Deserialize, Serialize};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::{ContainerAsync, GenericImage, ImageExt, runners::AsyncRunner};
use uuid::Uuid;

const POSTGRES_IMAGE: &str = "postgres";
const POSTGRES_TAG: &str = "16-alpine";

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TestAggregate {
    id: String,
    name: String,
    #[serde(default)]
    version: u64,
    #[serde(skip)]
    events: Vec<TestEvent>,
}

impl Entity for TestAggregate {
    type Id = String;
    fn id(&self) -> &Self::Id {
        &self.id
    }
}

impl AggregateRoot for TestAggregate {
    type Event = TestEvent;
    fn pending_events(&self) -> &[Self::Event] {
        &self.events
    }
    fn drain_events(&mut self) -> Vec<Self::Event> {
        std::mem::take(&mut self.events)
    }
    fn version(&self) -> u64 {
        self.version
    }
    fn set_version(&mut self, version: u64) {
        self.version = version;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TestEvent {
    aggregate_id: String,
    occurred_at: DateTime<Utc>,
}

impl DomainEvent for TestEvent {
    fn event_type(&self) -> &'static str {
        "TestEvent"
    }
    fn occurred_at(&self) -> DateTime<Utc> {
        self.occurred_at
    }
    fn aggregate_id(&self) -> &str {
        &self.aggregate_id
    }
}

#[tokio::test]
async fn postgres_outbox_repository_works_against_real_database() -> TestResult {
    let (_container, pool) = start_postgres().await?;
    let repo = PostgresOutboxRepository::new(pool);
    repo.migrate().await?;

    let outbox = OutboxMessage::new(
        Message::new(
            "orders",
            br#"{"order_id":"order-1"}"#.to_vec(),
            "application/json",
        )
        .with_key("order-1")
        .with_header("correlation_id", "corr-1"),
    );
    let outbox_id = outbox.id;

    repo.insert(outbox).await?;

    let pending = repo.pending(10).await?;
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].id, outbox_id);
    assert_eq!(pending[0].message.topic, "orders");
    assert_eq!(pending[0].message.key.as_deref(), Some("order-1"));
    assert_eq!(
        pending[0]
            .message
            .headers
            .get("correlation_id")
            .map(String::as_str),
        Some("corr-1")
    );

    repo.record_attempt(outbox_id).await?;
    repo.mark_published(outbox_id).await?;
    assert!(repo.pending(10).await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn postgres_inbox_store_enforces_idempotency_against_real_database() -> TestResult {
    let (_container, pool) = start_postgres().await?;
    let store = PostgresInboxStore::new(pool);
    store.migrate().await?;
    let message_id = Uuid::now_v7();

    assert_eq!(
        store.begin_processing(message_id, "billing").await?,
        IdempotencyDecision::StartProcessing
    );
    assert_eq!(
        store.begin_processing(message_id, "billing").await?,
        IdempotencyDecision::AlreadyProcessing
    );
    store
        .mark_failed(message_id, "billing", "temporary failure".to_string())
        .await?;
    assert_eq!(
        store.begin_processing(message_id, "billing").await?,
        IdempotencyDecision::RetryPreviousFailure
    );
    store.mark_completed(message_id, "billing").await?;
    assert_eq!(
        store.begin_processing(message_id, "billing").await?,
        IdempotencyDecision::AlreadyCompleted
    );
    Ok(())
}

#[tokio::test]
async fn postgres_json_repository_persists_aggregates_against_real_database() -> TestResult {
    let (_container, pool) = start_postgres().await?;
    let repo = PostgresJsonRepository::<TestAggregate>::with_aggregate_type(pool, "test_aggregate");
    repo.migrate().await?;
    let mut aggregate = TestAggregate {
        id: "aggregate-1".to_string(),
        name: "First aggregate".to_string(),
        version: 0,
        events: vec![TestEvent {
            aggregate_id: "aggregate-1".to_string(),
            occurred_at: Utc::now(),
        }],
    };

    repo.save(&mut aggregate).await?;
    assert_eq!(aggregate.version(), 1);

    let found = repo.find_by_id(&"aggregate-1".to_string()).await?;
    assert_eq!(found.map(|a| a.name), Some("First aggregate".to_string()));

    repo.delete(&"aggregate-1".to_string()).await?;
    assert!(repo.find_by_id(&"aggregate-1".to_string()).await?.is_none());
    Ok(())
}

#[tokio::test]
async fn save_and_enqueue_commits_aggregate_and_outbox_atomically() -> TestResult {
    let (_container, pool) = start_postgres().await?;
    migrate_postgres_aggregate_schema(&pool).await?;
    migrate_postgres_eventing_schema(&pool).await?;

    let mut aggregate = TestAggregate {
        id: "agg-atomic".to_string(),
        name: "Atomic aggregate".to_string(),
        version: 0,
        events: vec![
            TestEvent {
                aggregate_id: "agg-atomic".to_string(),
                occurred_at: Utc::now(),
            },
            TestEvent {
                aggregate_id: "agg-atomic".to_string(),
                occurred_at: Utc::now(),
            },
        ],
    };

    save_aggregate_and_enqueue(&pool, "test_aggregate", &mut aggregate, |event| {
        Message::new(
            "orders",
            event.aggregate_id.as_bytes().to_vec(),
            "text/plain",
        )
        .with_key(&event.aggregate_id)
    })
    .await?;

    assert_eq!(aggregate.version(), 1);

    let repo = PostgresJsonRepository::<TestAggregate>::with_aggregate_type(
        pool.clone(),
        "test_aggregate",
    );
    let found = repo.find_by_id(&"agg-atomic".to_string()).await?;
    assert_eq!(found.map(|a| a.name), Some("Atomic aggregate".to_string()));

    let outbox = PostgresOutboxRepository::new(pool);
    let pending = outbox.pending(10).await?;
    assert_eq!(pending.len(), 2);
    Ok(())
}

#[tokio::test]
async fn save_and_enqueue_rolls_back_outbox_on_concurrency_conflict() -> TestResult {
    let (_container, pool) = start_postgres().await?;
    migrate_postgres_aggregate_schema(&pool).await?;
    migrate_postgres_eventing_schema(&pool).await?;

    let mut aggregate = TestAggregate {
        id: "agg-occ".to_string(),
        name: "Original".to_string(),
        version: 0,
        events: vec![],
    };
    save_aggregate_and_enqueue(&pool, "test_aggregate", &mut aggregate, map_to_message).await?;

    let mut stale = TestAggregate {
        id: "agg-occ".to_string(),
        name: "Stale".to_string(),
        version: 0,
        events: vec![TestEvent {
            aggregate_id: "agg-occ".to_string(),
            occurred_at: Utc::now(),
        }],
    };
    let result =
        save_aggregate_and_enqueue(&pool, "test_aggregate", &mut stale, map_to_message).await;
    assert!(matches!(
        result,
        Err(PostgresTransactionError::ConcurrencyConflict { expected: 0, .. })
    ));

    let outbox = PostgresOutboxRepository::new(pool.clone());
    assert!(outbox.pending(10).await?.is_empty());

    let repo = PostgresJsonRepository::<TestAggregate>::with_aggregate_type(pool, "test_aggregate");
    let found = repo.find_by_id(&"agg-occ".to_string()).await?;
    assert_eq!(found.map(|a| a.name), Some("Original".to_string()));
    Ok(())
}

#[tokio::test]
async fn postgres_unit_of_work_commits_and_rolls_back() -> TestResult {
    let (_container, pool) = start_postgres().await?;
    // Create a scratch table directly via sqlx
    sqlx::query("CREATE TABLE uow_scratch (note TEXT NOT NULL)")
        .execute(&pool)
        .await?;

    let uow = PostgresUnitOfWork::new(pool.clone());

    // A committed transaction persists its writes.
    uow.transaction(|conn| {
        Box::pin(async move {
            sqlx::query("INSERT INTO uow_scratch (note) VALUES ($1)")
                .bind("kept")
                .execute(conn)
                .await?;
            Ok::<(), sqlx::Error>(())
        })
    })
    .await?;

    // A failing transaction rolls its writes back.
    let rolled_back = uow
        .transaction(|conn| {
            Box::pin(async move {
                sqlx::query("INSERT INTO uow_scratch (note) VALUES ($1)")
                    .bind("dropped")
                    .execute(&mut *conn)
                    .await?;
                // Execute against a missing column to force an error
                sqlx::query("INSERT INTO uow_scratch (missing) VALUES ($1)")
                    .bind("x")
                    .execute(&mut *conn)
                    .await?;
                Ok::<(), sqlx::Error>(())
            })
        })
        .await;
    assert!(rolled_back.is_err());

    let notes: i64 = sqlx::query_scalar("SELECT count(*) FROM uow_scratch")
        .fetch_one(&pool)
        .await?;
    assert_eq!(notes, 1, "only the committed row should remain");
    Ok(())
}

#[tokio::test]
async fn tenant_json_repository_isolates_rows_by_tenant() -> TestResult {
    let (_container, pool) = start_postgres().await?;
    migrate_postgres_tenant_aggregate_schema(&pool).await?;

    let tenant_a = TenantContext::new(Uuid::now_v7());
    let tenant_b = TenantContext::new(Uuid::now_v7());
    let repo_a =
        TenantJsonRepository::<TestAggregate>::new(pool.clone(), &tenant_a, "test_aggregate");
    let repo_b =
        TenantJsonRepository::<TestAggregate>::new(pool.clone(), &tenant_b, "test_aggregate");

    // Same aggregate id for both tenants — no conflict, tenant_id is part of
    // the primary key. The id must render as a UUID.
    let shared_id = Uuid::now_v7().to_string();
    let mut a = TestAggregate {
        id: shared_id.clone(),
        name: "A's data".to_string(),
        version: 0,
        events: vec![],
    };
    repo_a.save(&mut a).await?;
    let mut b = TestAggregate {
        id: shared_id.clone(),
        name: "B's data".to_string(),
        version: 0,
        events: vec![],
    };
    repo_b.save(&mut b).await?;

    let from_a = repo_a.find_by_id(&shared_id).await?;
    let from_b = repo_b.find_by_id(&shared_id).await?;
    assert_eq!(from_a.map(|x| x.name), Some("A's data".to_string()));
    assert_eq!(from_b.map(|x| x.name), Some("B's data".to_string()));

    repo_a.delete(&shared_id).await?;
    assert!(repo_a.find_by_id(&shared_id).await?.is_none());
    assert!(repo_b.find_by_id(&shared_id).await?.is_some());
    Ok(())
}

#[tokio::test]
async fn save_and_enqueue_in_commits_tenant_aggregate_and_outbox_atomically() -> TestResult {
    let (_container, pool) = start_postgres().await?;
    migrate_postgres_tenant_aggregate_schema(&pool).await?;
    migrate_postgres_eventing_schema(&pool).await?;

    let tenant = TenantContext::new(Uuid::now_v7());
    let repo = TenantJsonRepository::<TestAggregate>::new(pool.clone(), &tenant, "test_aggregate");

    let agg_id = Uuid::now_v7().to_string();
    let mut aggregate = TestAggregate {
        id: agg_id.clone(),
        name: "Tenant aggregate".to_string(),
        version: 0,
        events: vec![
            TestEvent {
                aggregate_id: agg_id.clone(),
                occurred_at: Utc::now(),
            },
            TestEvent {
                aggregate_id: agg_id.clone(),
                occurred_at: Utc::now(),
            },
        ],
    };

    // Atomic: the tenant-scoped snapshot and both outbox rows commit together.
    save_and_enqueue_in(&pool, &repo, &mut aggregate, map_to_message).await?;
    assert_eq!(aggregate.version(), 1);

    let found = repo.find_by_id(&agg_id).await?;
    assert_eq!(found.map(|a| a.name), Some("Tenant aggregate".to_string()));

    let outbox = PostgresOutboxRepository::new(pool.clone());
    assert_eq!(outbox.pending(10).await?.len(), 2);

    // A stale write conflicts and leaves no new outbox rows (rolled back).
    let mut stale = TestAggregate {
        id: agg_id.clone(),
        name: "Stale".to_string(),
        version: 0,
        events: vec![TestEvent {
            aggregate_id: agg_id.clone(),
            occurred_at: Utc::now(),
        }],
    };
    let result = save_and_enqueue_in(&pool, &repo, &mut stale, map_to_message).await;
    assert!(matches!(
        result,
        Err(SaveAndEnqueueError::Repository(
            RepositoryError::ConcurrencyConflict { expected: 0, .. }
        ))
    ));
    // Still only the two rows from the successful commit; the conflict rolled
    // back its outbox insert. Count directly — `pending()` above already leased
    // the rows, so a second `pending()` would report zero regardless.
    let total: i64 = sqlx::query_scalar("SELECT count(*) FROM pharos_outbox")
        .fetch_one(&pool)
        .await?;
    assert_eq!(total, 2);
    Ok(())
}

#[tokio::test]
async fn postgres_dead_letter_queue_persists_messages_against_real_database() -> TestResult {
    let (_container, pool) = start_postgres().await?;
    migrate_postgres_dead_letter_schema(&pool).await?;
    let queue = PostgresDeadLetterQueue::new(pool);

    let message = Message::new(
        "orders",
        br#"{"order_id":"order-dead"}"#.to_vec(),
        "application/json",
    )
    .with_key("order-dead")
    .with_header("correlation_id", "corr-dead");
    queue
        .dead_letter(DeadLetterMessage::new(message, "poison message", 5))
        .await?;

    let listed = queue.list(10).await?;
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].reason, "poison message");
    assert_eq!(listed[0].attempts, 5);
    assert_eq!(listed[0].message.topic, "orders");
    assert_eq!(listed[0].message.key.as_deref(), Some("order-dead"));
    Ok(())
}

#[tokio::test]
async fn outbox_cleanup_removes_old_published_rows() -> TestResult {
    let (_container, pool) = start_postgres().await?;
    let repo = PostgresOutboxRepository::new(pool);
    repo.migrate().await?;

    let msg = OutboxMessage::new(Message::new("test", b"payload".to_vec(), "text/plain"));
    let id = msg.id;
    repo.insert(msg).await?;
    repo.mark_published(id).await?;

    // Zero-duration cutoff to delete everything published/failed immediately
    let deleted = repo
        .cleanup_older_than(std::time::Duration::from_secs(0))
        .await?;
    assert_eq!(deleted, 1);
    Ok(())
}

fn map_to_message(event: &TestEvent) -> Message {
    Message::new(
        "orders",
        event.aggregate_id.as_bytes().to_vec(),
        "text/plain",
    )
    .with_key(&event.aggregate_id)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct LedgerEntryPosted {
    ledger_id: String,
    amount_minor: i64,
    occurred_at: DateTime<Utc>,
}

impl DomainEvent for LedgerEntryPosted {
    fn event_type(&self) -> &'static str {
        "LedgerEntryPosted"
    }
    fn occurred_at(&self) -> DateTime<Utc> {
        self.occurred_at
    }
    fn aggregate_id(&self) -> &str {
        &self.ledger_id
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct Ledger {
    id: String,
    balance_minor: i64,
    version: u64,
    #[serde(skip)]
    events: AggregateEvents<LedgerEntryPosted>,
}

impl Ledger {
    fn post(&mut self, id: &str, amount_minor: i64) {
        self.events.raise(LedgerEntryPosted {
            ledger_id: id.to_string(),
            amount_minor,
            occurred_at: Utc::now(),
        });
        self.id = id.to_string();
        self.balance_minor += amount_minor;
    }
}

impl Entity for Ledger {
    type Id = String;
    fn id(&self) -> &Self::Id {
        &self.id
    }
}

impl AggregateRoot for Ledger {
    type Event = LedgerEntryPosted;
    fn pending_events(&self) -> &[Self::Event] {
        self.events.pending()
    }
    fn drain_events(&mut self) -> Vec<Self::Event> {
        self.events.drain()
    }
    fn version(&self) -> u64 {
        self.version
    }
    fn set_version(&mut self, version: u64) {
        self.version = version;
    }
}

impl EventSourced for Ledger {
    fn apply(&mut self, event: &Self::Event) {
        self.id = event.ledger_id.clone();
        self.balance_minor += event.amount_minor;
    }
}

fn posted(ledger_id: &str, amount_minor: i64) -> LedgerEntryPosted {
    LedgerEntryPosted {
        ledger_id: ledger_id.to_string(),
        amount_minor,
        occurred_at: Utc::now(),
    }
}

#[tokio::test]
async fn pg_event_store_appends_loads_and_enforces_occ() -> TestResult {
    let (_container, pool) = start_postgres().await?;
    let store: PgEventStore<String, LedgerEntryPosted> =
        PgEventStore::with_stream_type(pool, "ledger");
    store.migrate().await?;

    let id = "ledger-1".to_string();
    store
        .append(&id, 0, vec![posted(&id, 1_000), posted(&id, -250)])
        .await?;

    let events = store.load(&id).await?;
    assert_eq!(events.len(), 2);
    assert_eq!(
        events.iter().map(|e| e.sequence).collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(events[1].event.amount_minor, -250);

    // load_after replays only the tail.
    let tail = store.load_after(&id, 1).await?;
    assert_eq!(tail.len(), 1);
    assert_eq!(tail[0].sequence, 2);

    // A stale expected_version is a concurrency conflict, both below and
    // above the current head.
    let Err(RepositoryError::ConcurrencyConflict { expected, actual }) =
        store.append(&id, 0, vec![posted(&id, 1)]).await
    else {
        panic!("stale append must conflict");
    };
    assert_eq!(expected, 0);
    assert_eq!(actual, Some(2));
    assert!(matches!(
        store.append(&id, 5, vec![posted(&id, 1)]).await,
        Err(RepositoryError::ConcurrencyConflict { .. })
    ));

    // The stream is untouched by the failed appends.
    assert_eq!(store.load(&id).await?.len(), 2);

    // Unknown streams load empty; empty appends are no-ops.
    assert!(store.load(&"missing".to_string()).await?.is_empty());
    store.append(&id, 2, vec![]).await?;
    Ok(())
}

#[tokio::test]
async fn pg_snapshot_store_upserts_and_delete_stream_removes_both() -> TestResult {
    let (_container, pool) = start_postgres().await?;
    let store: PgEventStore<String, LedgerEntryPosted> =
        PgEventStore::with_stream_type(pool.clone(), "ledger");
    store.migrate().await?;
    let snapshots: PgSnapshotStore<String, Ledger> =
        PgSnapshotStore::with_stream_type(pool, "ledger");

    let id = "ledger-2".to_string();
    store.append(&id, 0, vec![posted(&id, 500)]).await?;

    assert!(snapshots.load(&id).await?.is_none());
    let ledger = Ledger {
        id: id.clone(),
        balance_minor: 500,
        version: 1,
        events: AggregateEvents::default(),
    };
    snapshots
        .save(&id, Snapshot::new(ledger.clone(), 1))
        .await?;

    // Upsert replaces the previous snapshot.
    let newer = Ledger {
        balance_minor: 750,
        version: 2,
        ..ledger
    };
    snapshots.save(&id, Snapshot::new(newer, 2)).await?;
    let loaded = snapshots.load(&id).await?.ok_or("snapshot must exist")?;
    assert_eq!(loaded.version, 2);
    assert_eq!(loaded.state.balance_minor, 750);

    // Deleting the stream removes the events and the snapshot together.
    store.delete_stream(&id).await?;
    assert!(store.load(&id).await?.is_empty());
    assert!(snapshots.load(&id).await?.is_none());
    Ok(())
}

#[tokio::test]
async fn event_sourced_repository_rehydrates_against_postgres() -> TestResult {
    let (_container, pool) = start_postgres().await?;
    let store: PgEventStore<String, LedgerEntryPosted> =
        PgEventStore::with_stream_type(pool, "ledger");
    store.migrate().await?;
    let repo = EventSourcedRepository::<Ledger, _>::new(store);

    let mut ledger = Ledger::default();
    ledger.post("ledger-3", 10_000);
    ledger.post("ledger-3", -3_500);
    repo.save(&mut ledger).await?;
    assert_eq!(ledger.version(), 2);

    let loaded = repo
        .find_by_id(&"ledger-3".to_string())
        .await?
        .ok_or("ledger not found")?;
    assert_eq!(loaded.balance_minor, 6_500);
    assert_eq!(loaded.version(), 2);
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
enum PaymentSagaState {
    AwaitingConfirmation { amount_minor: i64 },
    Confirmed,
}

#[tokio::test]
async fn pg_saga_store_roundtrips_instances_and_claims_due_deadlines() -> TestResult {
    let (_container, pool) = start_postgres().await?;
    let store: PgSagaStore<String, PaymentSagaState> = PgSagaStore::with_saga_type(pool, "payment");
    store.migrate().await?;

    // Roundtrip: state, status, and deadline survive persistence.
    let deadline = Utc::now() - chrono::Duration::minutes(10);
    store
        .save(SagaInstance::running_until(
            "pay-due".to_string(),
            PaymentSagaState::AwaitingConfirmation { amount_minor: 900 },
            deadline,
        ))
        .await?;
    let loaded = store
        .load(&"pay-due".to_string())
        .await?
        .ok_or("instance must exist")?;
    assert_eq!(loaded.status, SagaStatus::Running);
    assert_eq!(
        loaded.state,
        PaymentSagaState::AwaitingConfirmation { amount_minor: 900 }
    );
    assert_eq!(
        loaded.deadline.map(|d| d.timestamp_millis()),
        Some(deadline.timestamp_millis())
    );
    assert!(store.load(&"missing".to_string()).await?.is_none());

    // claim_due skips future deadlines, terminal instances, and no-deadline
    // instances, and respects the limit ordered by soonest deadline.
    store
        .save(SagaInstance::running_until(
            "pay-due-later".to_string(),
            PaymentSagaState::AwaitingConfirmation { amount_minor: 100 },
            Utc::now() - chrono::Duration::minutes(1),
        ))
        .await?;
    store
        .save(SagaInstance::running_until(
            "pay-future".to_string(),
            PaymentSagaState::AwaitingConfirmation { amount_minor: 100 },
            Utc::now() + chrono::Duration::hours(1),
        ))
        .await?;
    store
        .save(SagaInstance::running(
            "pay-no-deadline".to_string(),
            PaymentSagaState::AwaitingConfirmation { amount_minor: 100 },
        ))
        .await?;
    let mut completed = SagaInstance::running_until(
        "pay-completed".to_string(),
        PaymentSagaState::Confirmed,
        Utc::now() - chrono::Duration::minutes(30),
    );
    completed.status = SagaStatus::Completed;
    store.save(completed).await?;

    let lease = chrono::Duration::minutes(5);

    // The limit claims only the soonest deadline...
    let limited = store.claim_due(Utc::now(), lease, 1).await?;
    assert_eq!(limited.len(), 1);
    assert_eq!(limited[0].id, "pay-due");
    // ...and the claimed instance carries its original (elapsed) deadline.
    assert_eq!(
        limited[0].deadline.map(|d| d.timestamp_millis()),
        Some(deadline.timestamp_millis())
    );

    // The claim postponed the stored deadline by the lease, so a second
    // sweep only sees the remaining due instance — never a double delivery.
    let due = store.claim_due(Utc::now(), lease, 10).await?;
    assert_eq!(
        due.iter().map(|i| i.id.as_str()).collect::<Vec<_>>(),
        vec!["pay-due-later"]
    );
    assert!(store.claim_due(Utc::now(), lease, 10).await?.is_empty());

    // Once the lease expires the unprocessed instances become due again.
    let after_lease = Utc::now() + lease + chrono::Duration::seconds(1);
    let redue = store.claim_due(after_lease, lease, 10).await?;
    assert_eq!(
        redue.iter().map(|i| i.id.as_str()).collect::<Vec<_>>(),
        vec!["pay-due", "pay-due-later"]
    );

    // Upsert: completing an instance takes it out of the sweep for good.
    let mut confirmed = loaded;
    confirmed.state = PaymentSagaState::Confirmed;
    confirmed.status = SagaStatus::Completed;
    confirmed.deadline = None;
    store.save(confirmed).await?;
    let after_second_lease = after_lease + lease + chrono::Duration::seconds(1);
    let due = store.claim_due(after_second_lease, lease, 10).await?;
    assert_eq!(due.len(), 1);
    assert_eq!(due[0].id, "pay-due-later");
    Ok(())
}

#[tokio::test]
async fn concurrent_sweepers_never_claim_the_same_saga() -> TestResult {
    let (_container, pool) = start_postgres().await?;
    let store: PgSagaStore<String, PaymentSagaState> =
        PgSagaStore::with_saga_type(pool.clone(), "payment");
    store.migrate().await?;

    for index in 0..20 {
        store
            .save(SagaInstance::running_until(
                format!("pay-{index:02}"),
                PaymentSagaState::AwaitingConfirmation { amount_minor: 100 },
                Utc::now() - chrono::Duration::minutes(1),
            ))
            .await?;
    }

    // Two sweepers race over the same due set; SKIP LOCKED + the lease must
    // hand every instance to exactly one of them.
    let second_store: PgSagaStore<String, PaymentSagaState> =
        PgSagaStore::with_saga_type(pool, "payment");
    let now = Utc::now();
    let lease = chrono::Duration::minutes(5);
    let (first, second) = tokio::join!(
        store.claim_due(now, lease, 20),
        second_store.claim_due(now, lease, 20),
    );
    let (first, second) = (first?, second?);

    let mut all: Vec<String> = first
        .iter()
        .chain(second.iter())
        .map(|i| i.id.clone())
        .collect();
    all.sort();
    all.dedup();
    assert_eq!(
        first.len() + second.len(),
        all.len(),
        "a saga was claimed by both sweepers"
    );
    assert_eq!(all.len(), 20, "every due saga must be claimed exactly once");
    Ok(())
}

#[tokio::test]
async fn inbox_cleanup_deletes_terminal_rows_and_reopens_idempotency() -> TestResult {
    let (_container, pool) = start_postgres().await?;
    let store = PostgresInboxStore::new(pool);
    store.migrate().await?;

    let completed_id = Uuid::now_v7();
    let failed_id = Uuid::now_v7();
    let processing_id = Uuid::now_v7();
    store.begin_processing(completed_id, "billing").await?;
    store.mark_completed(completed_id, "billing").await?;
    store.begin_processing(failed_id, "billing").await?;
    store
        .mark_failed(failed_id, "billing", "boom".to_string())
        .await?;
    store.begin_processing(processing_id, "billing").await?;

    // Zero-duration cutoff deletes completed/failed immediately, but never
    // an in-flight `processing` record.
    let deleted = store
        .cleanup_older_than(std::time::Duration::from_secs(0))
        .await?;
    assert_eq!(deleted, 2);
    assert_eq!(
        store.begin_processing(processing_id, "billing").await?,
        IdempotencyDecision::AlreadyProcessing
    );

    // Cleaned-up messages are new again: the idempotency window shrank.
    assert_eq!(
        store.begin_processing(completed_id, "billing").await?,
        IdempotencyDecision::StartProcessing
    );
    Ok(())
}

async fn start_postgres()
-> Result<(ContainerAsync<GenericImage>, Pool), Box<dyn std::error::Error + Send + Sync>> {
    let container = GenericImage::new(POSTGRES_IMAGE, POSTGRES_TAG)
        .with_exposed_port(5432.tcp())
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_PASSWORD", "postgres")
        .start()
        .await?;

    let host = container.get_host().await?.to_string();
    let port = container.get_host_port_ipv4(5432).await?;
    let connection_string = format!("postgres://postgres:postgres@{host}:{port}/postgres");

    let pool = connect_pool(&connection_string, 8)?;

    Ok((container, pool))
}
