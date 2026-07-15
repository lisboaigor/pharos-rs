//! Saga and process-manager building blocks for Pharos.
//!
//! A saga reacts to domain or integration events, persists a long-lived state
//! machine, and emits follow-up commands. The runner in this crate keeps that
//! flow explicit and testable without imposing transport or storage details.

use std::error::Error;
use std::future::Future;

use chrono::{DateTime, Utc};
use thiserror::Error;

/// Lifecycle of a saga instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SagaStatus {
    /// The saga is waiting for more events.
    Running,
    /// The saga has reached a terminal success state.
    Completed,
    /// The saga failed irrecoverably.
    Failed,
}

/// Persisted state for one saga instance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SagaInstance<I, S> {
    /// Stable saga identifier.
    pub id: I,
    /// Current state machine payload.
    pub state: S,
    /// Current lifecycle status.
    pub status: SagaStatus,
    /// Instant after which the saga times out, when set.
    ///
    /// Only meaningful while the saga is [`SagaStatus::Running`]; terminal
    /// transitions clear it. [`SagaRunner::run_due_timeouts`] fires
    /// [`Saga::on_timeout`] for running instances past this instant.
    pub deadline: Option<DateTime<Utc>>,
    /// Last update timestamp.
    pub updated_at: DateTime<Utc>,
}

impl<I, S> SagaInstance<I, S> {
    /// Creates a running saga instance with no deadline.
    pub fn running(id: I, state: S) -> Self {
        Self {
            id,
            state,
            status: SagaStatus::Running,
            deadline: None,
            updated_at: Utc::now(),
        }
    }

    /// Creates a running saga instance that times out at `deadline`.
    pub fn running_until(id: I, state: S, deadline: DateTime<Utc>) -> Self {
        Self {
            deadline: Some(deadline),
            ..Self::running(id, state)
        }
    }
}

/// Transition produced by a saga in response to an event or timeout.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SagaTransition<S, C> {
    /// The saga is not interested in the event.
    ///
    /// On the timeout path this clears the elapsed deadline and keeps the
    /// saga running, so an ignored timeout never refires.
    Ignore,
    /// Start a new saga instance, optionally with a timeout deadline.
    Start {
        state: S,
        commands: Vec<C>,
        deadline: Option<DateTime<Utc>>,
    },
    /// Update an already-running saga; `deadline` replaces the previous one
    /// (`None` cancels any pending timeout).
    Advance {
        state: S,
        commands: Vec<C>,
        deadline: Option<DateTime<Utc>>,
    },
    /// Complete the saga. Clears any pending deadline.
    Complete { state: S, commands: Vec<C> },
    /// Fail the saga with a reason. Clears any pending deadline.
    Fail { reason: String },
}

/// Pure saga state machine.
pub trait Saga: Send + Sync + 'static {
    /// Saga identifier type.
    type Id: Clone + Send + Sync + 'static;
    /// Persisted state machine payload.
    type State: Clone + Send + Sync + 'static;
    /// Event this saga reacts to.
    type Event: Send + Sync + 'static;
    /// Follow-up command emitted by the saga.
    type Command: Clone + Send + Sync + 'static;
    /// User-defined error returned while computing a transition.
    type Error: Error + Send + Sync + 'static;

    /// Extracts the saga id that should handle `event`.
    fn id_for(&self, event: &Self::Event) -> Option<Self::Id>;

    /// Computes the transition for `event`, given the current persisted state.
    fn react(
        &self,
        state: Option<&SagaInstance<Self::Id, Self::State>>,
        event: &Self::Event,
    ) -> impl Future<Output = Result<SagaTransition<Self::State, Self::Command>, Self::Error>> + Send;

    /// Computes the transition for an elapsed deadline.
    ///
    /// Called by [`SagaRunner::run_due_timeouts`] for running instances whose
    /// [`SagaInstance::deadline`] has passed. The default fails the saga,
    /// which is the safe outcome: the instance is marked
    /// [`SagaStatus::Failed`] and the timeout never refires. Sagas that
    /// compensate on timeout (expire a payment, release a reservation)
    /// override this and return the appropriate transition.
    fn on_timeout(
        &self,
        instance: &SagaInstance<Self::Id, Self::State>,
    ) -> impl Future<Output = Result<SagaTransition<Self::State, Self::Command>, Self::Error>> + Send
    {
        let _ = instance;
        async {
            Ok(SagaTransition::Fail {
                reason: "saga deadline elapsed".to_string(),
            })
        }
    }
}

/// Persistence boundary for saga instances.
pub trait SagaStore<I, S>: Send + Sync + 'static {
    /// Concrete storage error.
    type Error: Error + Send + Sync + 'static;

    /// Loads the current instance for `id`, when it exists.
    fn load(
        &self,
        id: &I,
    ) -> impl Future<Output = Result<Option<SagaInstance<I, S>>, Self::Error>> + Send;

    /// Upserts an instance.
    fn save(
        &self,
        instance: SagaInstance<I, S>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// Saga store that can also query instances with an elapsed deadline.
///
/// Implement this in addition to [`SagaStore`] to drive timeouts through
/// [`SagaRunner::run_due_timeouts`].
pub trait SagaTimeoutStore<I, S>: SagaStore<I, S> {
    /// Loads up to `limit` [`SagaStatus::Running`] instances whose deadline
    /// is at or before `now`, soonest deadline first.
    fn find_due(
        &self,
        now: DateTime<Utc>,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<SagaInstance<I, S>>, Self::Error>> + Send;
}

/// Command dispatch boundary used by the runner.
pub trait CommandDispatcher<C>: Send + Sync + 'static {
    /// Concrete dispatch error.
    type Error: Error + Send + Sync + 'static;

    /// Dispatches one command emitted by a saga.
    fn dispatch(&self, command: C) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// Error returned by [`SagaRunner`].
#[derive(Debug, Error)]
pub enum SagaRunnerError<SE, StoreE, DispatchE>
where
    SE: Error + 'static,
    StoreE: Error + 'static,
    DispatchE: Error + 'static,
{
    /// The saga state machine failed.
    #[error("saga transition failed: {0}")]
    Saga(#[source] SE),
    /// Loading or saving the persisted state failed.
    #[error("saga store failed: {0}")]
    Store(#[source] StoreE),
    /// Dispatching an emitted command failed.
    #[error("command dispatch failed: {0}")]
    Dispatch(#[source] DispatchE),
    /// The saga reached [`SagaTransition::Fail`]: a terminal business failure.
    ///
    /// The instance (when one exists) has already been persisted with
    /// [`SagaStatus::Failed`] before this error is returned.
    #[error("saga failed: {reason}")]
    Failed {
        /// Reason supplied by the saga's transition.
        reason: String,
    },
}

/// Drives a saga end-to-end: load state, react, save, dispatch commands.
pub struct SagaRunner<SG, Store, Dispatcher> {
    saga: SG,
    store: Store,
    dispatcher: Dispatcher,
}

impl<SG, Store, Dispatcher> SagaRunner<SG, Store, Dispatcher> {
    /// Creates a runner.
    pub fn new(saga: SG, store: Store, dispatcher: Dispatcher) -> Self {
        Self {
            saga,
            store,
            dispatcher,
        }
    }
}

impl<SG, Store, Dispatcher> SagaRunner<SG, Store, Dispatcher>
where
    SG: Saga,
    Store: SagaStore<SG::Id, SG::State>,
    Dispatcher: CommandDispatcher<SG::Command>,
{
    /// Handles an event from start to finish.
    pub async fn handle(
        &self,
        event: &SG::Event,
    ) -> Result<(), SagaRunnerError<SG::Error, Store::Error, Dispatcher::Error>> {
        let Some(id) = self.saga.id_for(event) else {
            return Ok(());
        };

        let current = self.store.load(&id).await.map_err(SagaRunnerError::Store)?;
        let transition = self
            .saga
            .react(current.as_ref(), event)
            .await
            .map_err(SagaRunnerError::Saga)?;

        if matches!(transition, SagaTransition::Ignore) {
            return Ok(());
        }
        self.apply_transition(id, current, transition).await
    }

    async fn apply_transition(
        &self,
        id: SG::Id,
        current: Option<SagaInstance<SG::Id, SG::State>>,
        transition: SagaTransition<SG::State, SG::Command>,
    ) -> Result<(), SagaRunnerError<SG::Error, Store::Error, Dispatcher::Error>> {
        match transition {
            // Ignore is resolved by the caller: a no-op for events, a
            // deadline clear on the timeout path.
            SagaTransition::Ignore => Ok(()),
            SagaTransition::Start {
                state,
                commands,
                deadline,
            } => {
                let mut instance = SagaInstance::running(id, state);
                instance.deadline = deadline;
                self.store
                    .save(instance)
                    .await
                    .map_err(SagaRunnerError::Store)?;
                self.dispatch_all(commands).await
            }
            SagaTransition::Advance {
                state,
                commands,
                deadline,
            } => {
                let mut instance =
                    current.unwrap_or_else(|| SagaInstance::running(id, state.clone()));
                instance.state = state;
                instance.status = SagaStatus::Running;
                instance.deadline = deadline;
                instance.updated_at = Utc::now();
                self.store
                    .save(instance)
                    .await
                    .map_err(SagaRunnerError::Store)?;
                self.dispatch_all(commands).await
            }
            SagaTransition::Complete { state, commands } => {
                let mut instance =
                    current.unwrap_or_else(|| SagaInstance::running(id, state.clone()));
                instance.state = state;
                instance.status = SagaStatus::Completed;
                instance.deadline = None;
                instance.updated_at = Utc::now();
                self.store
                    .save(instance)
                    .await
                    .map_err(SagaRunnerError::Store)?;
                self.dispatch_all(commands).await
            }
            SagaTransition::Fail { reason } => {
                // A saga that fails before any instance was persisted has no
                // state to mark; the error itself is the only record.
                if let Some(mut instance) = current {
                    instance.status = SagaStatus::Failed;
                    instance.deadline = None;
                    instance.updated_at = Utc::now();
                    self.store
                        .save(instance)
                        .await
                        .map_err(SagaRunnerError::Store)?;
                }
                Err(SagaRunnerError::Failed { reason })
            }
        }
    }

    /// Fires [`Saga::on_timeout`] for up to `limit` running instances whose
    /// deadline elapsed at `now`, and returns how many were processed.
    ///
    /// A [`SagaTransition::Fail`] returned by `on_timeout` is a normal
    /// business outcome here (an expired payment, an abandoned checkout):
    /// the instance is persisted as [`SagaStatus::Failed`] and the sweep
    /// continues. Only saga, store, or dispatch errors abort the sweep.
    ///
    /// Pharos provides the mechanism, not the scheduler: call this from a
    /// periodic task in the application, e.g. a `tokio::time::interval` loop.
    pub async fn run_due_timeouts(
        &self,
        now: DateTime<Utc>,
        limit: usize,
    ) -> Result<usize, SagaRunnerError<SG::Error, Store::Error, Dispatcher::Error>>
    where
        Store: SagaTimeoutStore<SG::Id, SG::State>,
    {
        let due = self
            .store
            .find_due(now, limit)
            .await
            .map_err(SagaRunnerError::Store)?;

        let mut processed = 0;
        for instance in due {
            let transition = self
                .saga
                .on_timeout(&instance)
                .await
                .map_err(SagaRunnerError::Saga)?;

            if matches!(transition, SagaTransition::Ignore) {
                // Clear the elapsed deadline so the timeout never refires.
                let mut instance = instance;
                instance.deadline = None;
                instance.updated_at = Utc::now();
                self.store
                    .save(instance)
                    .await
                    .map_err(SagaRunnerError::Store)?;
                processed += 1;
                continue;
            }

            let id = instance.id.clone();
            match self.apply_transition(id, Some(instance), transition).await {
                Ok(()) => {}
                Err(SagaRunnerError::Failed { reason }) => {
                    tracing::info!(reason, "saga failed on timeout");
                }
                Err(error) => return Err(error),
            }
            processed += 1;
        }
        Ok(processed)
    }

    async fn dispatch_all(
        &self,
        commands: Vec<SG::Command>,
    ) -> Result<(), SagaRunnerError<SG::Error, Store::Error, Dispatcher::Error>> {
        for command in commands {
            self.dispatcher
                .dispatch(command)
                .await
                .map_err(SagaRunnerError::Dispatch)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::convert::Infallible;
    use std::sync::{Arc, Mutex};

    use super::*;

    #[derive(Clone)]
    struct OrderPlaced {
        order_id: String,
        amount_cents: u32,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum BillingCommand {
        ReserveFunds { order_id: String, amount_cents: u32 },
        FinalizeOrder { order_id: String },
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum BillingState {
        AwaitingReservation { amount_cents: u32 },
        Reserved,
    }

    struct BillingSaga;

    impl Saga for BillingSaga {
        type Id = String;
        type State = BillingState;
        type Event = OrderPlaced;
        type Command = BillingCommand;
        type Error = Infallible;

        fn id_for(&self, event: &Self::Event) -> Option<Self::Id> {
            Some(event.order_id.clone())
        }

        async fn react(
            &self,
            state: Option<&SagaInstance<Self::Id, Self::State>>,
            event: &Self::Event,
        ) -> Result<SagaTransition<Self::State, Self::Command>, Self::Error> {
            Ok(match state {
                None => SagaTransition::Start {
                    state: BillingState::AwaitingReservation {
                        amount_cents: event.amount_cents,
                    },
                    commands: vec![BillingCommand::ReserveFunds {
                        order_id: event.order_id.clone(),
                        amount_cents: event.amount_cents,
                    }],
                    deadline: None,
                },
                Some(_) => SagaTransition::Complete {
                    state: BillingState::Reserved,
                    commands: vec![BillingCommand::FinalizeOrder {
                        order_id: event.order_id.clone(),
                    }],
                },
            })
        }
    }

    #[derive(Default)]
    struct InMemorySagaStore {
        instances: Mutex<HashMap<String, SagaInstance<String, BillingState>>>,
    }

    impl SagaStore<String, BillingState> for InMemorySagaStore {
        type Error = Infallible;

        async fn load(
            &self,
            id: &String,
        ) -> Result<Option<SagaInstance<String, BillingState>>, Self::Error> {
            Ok(self
                .instances
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .get(id)
                .cloned())
        }

        async fn save(
            &self,
            instance: SagaInstance<String, BillingState>,
        ) -> Result<(), Self::Error> {
            self.instances
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .insert(instance.id.clone(), instance);
            Ok(())
        }
    }

    impl SagaTimeoutStore<String, BillingState> for InMemorySagaStore {
        async fn find_due(
            &self,
            now: DateTime<Utc>,
            limit: usize,
        ) -> Result<Vec<SagaInstance<String, BillingState>>, Self::Error> {
            let mut due: Vec<_> = self
                .instances
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .values()
                .filter(|i| {
                    i.status == SagaStatus::Running
                        && i.deadline.is_some_and(|deadline| deadline <= now)
                })
                .cloned()
                .collect();
            due.sort_by_key(|i| i.deadline);
            due.truncate(limit);
            Ok(due)
        }
    }

    #[derive(Default, Clone)]
    struct VecDispatcher {
        commands: Arc<Mutex<Vec<BillingCommand>>>,
    }

    impl CommandDispatcher<BillingCommand> for VecDispatcher {
        type Error = Infallible;

        async fn dispatch(&self, command: BillingCommand) -> Result<(), Self::Error> {
            self.commands
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .push(command);
            Ok(())
        }
    }

    struct AlwaysFailingSaga;

    impl Saga for AlwaysFailingSaga {
        type Id = String;
        type State = BillingState;
        type Event = OrderPlaced;
        type Command = BillingCommand;
        type Error = Infallible;

        fn id_for(&self, event: &Self::Event) -> Option<Self::Id> {
            Some(event.order_id.clone())
        }

        async fn react(
            &self,
            _state: Option<&SagaInstance<Self::Id, Self::State>>,
            _event: &Self::Event,
        ) -> Result<SagaTransition<Self::State, Self::Command>, Self::Error> {
            Ok(SagaTransition::Fail {
                reason: "funds could not be reserved".to_string(),
            })
        }
    }

    #[tokio::test]
    async fn fail_transition_returns_error_instead_of_panicking()
    -> Result<(), Box<dyn std::error::Error>> {
        let runner = SagaRunner::new(
            AlwaysFailingSaga,
            InMemorySagaStore::default(),
            VecDispatcher::default(),
        );
        let event = OrderPlaced {
            order_id: "order-9".into(),
            amount_cents: 100,
        };

        let Err(SagaRunnerError::Failed { reason }) = runner.handle(&event).await else {
            panic!("expected SagaRunnerError::Failed");
        };
        assert_eq!(reason, "funds could not be reserved");
        Ok(())
    }

    #[tokio::test]
    async fn fail_transition_marks_existing_instance_as_failed()
    -> Result<(), Box<dyn std::error::Error>> {
        let store = InMemorySagaStore::default();
        store
            .save(SagaInstance::running(
                "order-9".to_string(),
                BillingState::AwaitingReservation { amount_cents: 100 },
            ))
            .await?;
        let runner = SagaRunner::new(AlwaysFailingSaga, store, VecDispatcher::default());
        let event = OrderPlaced {
            order_id: "order-9".into(),
            amount_cents: 100,
        };

        assert!(matches!(
            runner.handle(&event).await,
            Err(SagaRunnerError::Failed { .. })
        ));
        let stored = runner
            .store
            .instances
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get("order-9")
            .cloned()
            .ok_or("instance must exist")?;
        assert_eq!(stored.status, SagaStatus::Failed);
        Ok(())
    }

    /// Saga that compensates on timeout: emits a cancel command and completes.
    struct ExpiringSaga;

    impl Saga for ExpiringSaga {
        type Id = String;
        type State = BillingState;
        type Event = OrderPlaced;
        type Command = BillingCommand;
        type Error = Infallible;

        fn id_for(&self, event: &Self::Event) -> Option<Self::Id> {
            Some(event.order_id.clone())
        }

        async fn react(
            &self,
            _state: Option<&SagaInstance<Self::Id, Self::State>>,
            _event: &Self::Event,
        ) -> Result<SagaTransition<Self::State, Self::Command>, Self::Error> {
            Ok(SagaTransition::Ignore)
        }

        async fn on_timeout(
            &self,
            instance: &SagaInstance<Self::Id, Self::State>,
        ) -> Result<SagaTransition<Self::State, Self::Command>, Self::Error> {
            Ok(SagaTransition::Complete {
                state: instance.state.clone(),
                commands: vec![BillingCommand::FinalizeOrder {
                    order_id: instance.id.clone(),
                }],
            })
        }
    }

    /// Saga whose timeout is not interesting: the runner must clear the
    /// deadline so it never refires.
    struct SnoozingSaga;

    impl Saga for SnoozingSaga {
        type Id = String;
        type State = BillingState;
        type Event = OrderPlaced;
        type Command = BillingCommand;
        type Error = Infallible;

        fn id_for(&self, event: &Self::Event) -> Option<Self::Id> {
            Some(event.order_id.clone())
        }

        async fn react(
            &self,
            _state: Option<&SagaInstance<Self::Id, Self::State>>,
            _event: &Self::Event,
        ) -> Result<SagaTransition<Self::State, Self::Command>, Self::Error> {
            Ok(SagaTransition::Ignore)
        }

        async fn on_timeout(
            &self,
            _instance: &SagaInstance<Self::Id, Self::State>,
        ) -> Result<SagaTransition<Self::State, Self::Command>, Self::Error> {
            Ok(SagaTransition::Ignore)
        }
    }

    fn expired_instance(id: &str) -> SagaInstance<String, BillingState> {
        SagaInstance::running_until(
            id.to_string(),
            BillingState::AwaitingReservation { amount_cents: 100 },
            Utc::now() - chrono::Duration::minutes(5),
        )
    }

    #[tokio::test]
    async fn default_on_timeout_fails_the_saga_and_clears_the_deadline()
    -> Result<(), Box<dyn std::error::Error>> {
        let store = InMemorySagaStore::default();
        store.save(expired_instance("order-1")).await?;
        let runner = SagaRunner::new(BillingSaga, store, VecDispatcher::default());

        let processed = runner.run_due_timeouts(Utc::now(), 10).await?;
        assert_eq!(processed, 1);

        let stored = runner
            .store
            .load(&"order-1".to_string())
            .await?
            .ok_or("instance must exist")?;
        assert_eq!(stored.status, SagaStatus::Failed);
        assert_eq!(stored.deadline, None);

        // The failed instance is terminal: nothing is due anymore.
        assert_eq!(runner.run_due_timeouts(Utc::now(), 10).await?, 0);
        Ok(())
    }

    #[tokio::test]
    async fn on_timeout_override_compensates_and_completes()
    -> Result<(), Box<dyn std::error::Error>> {
        let store = InMemorySagaStore::default();
        store.save(expired_instance("order-2")).await?;
        let dispatcher = VecDispatcher::default();
        let runner = SagaRunner::new(ExpiringSaga, store, dispatcher.clone());

        assert_eq!(runner.run_due_timeouts(Utc::now(), 10).await?, 1);

        let stored = runner
            .store
            .load(&"order-2".to_string())
            .await?
            .ok_or("instance must exist")?;
        assert_eq!(stored.status, SagaStatus::Completed);
        assert_eq!(stored.deadline, None);
        let commands = dispatcher
            .commands
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        assert_eq!(
            commands,
            vec![BillingCommand::FinalizeOrder {
                order_id: "order-2".into(),
            }]
        );
        Ok(())
    }

    #[tokio::test]
    async fn ignored_timeout_clears_the_deadline_and_keeps_running()
    -> Result<(), Box<dyn std::error::Error>> {
        let store = InMemorySagaStore::default();
        store.save(expired_instance("order-3")).await?;
        let runner = SagaRunner::new(SnoozingSaga, store, VecDispatcher::default());

        assert_eq!(runner.run_due_timeouts(Utc::now(), 10).await?, 1);

        let stored = runner
            .store
            .load(&"order-3".to_string())
            .await?
            .ok_or("instance must exist")?;
        assert_eq!(stored.status, SagaStatus::Running);
        assert_eq!(stored.deadline, None);
        assert_eq!(runner.run_due_timeouts(Utc::now(), 10).await?, 0);
        Ok(())
    }

    #[tokio::test]
    async fn run_due_timeouts_respects_future_deadlines_and_limit()
    -> Result<(), Box<dyn std::error::Error>> {
        let store = InMemorySagaStore::default();
        store.save(expired_instance("order-4")).await?;
        store.save(expired_instance("order-5")).await?;
        store
            .save(SagaInstance::running_until(
                "order-future".to_string(),
                BillingState::Reserved,
                Utc::now() + chrono::Duration::hours(1),
            ))
            .await?;
        let runner = SagaRunner::new(BillingSaga, store, VecDispatcher::default());

        // Only one of the two due instances fits the limit.
        assert_eq!(runner.run_due_timeouts(Utc::now(), 1).await?, 1);
        assert_eq!(runner.run_due_timeouts(Utc::now(), 10).await?, 1);

        // The future deadline stays untouched.
        let future = runner
            .store
            .load(&"order-future".to_string())
            .await?
            .ok_or("instance must exist")?;
        assert_eq!(future.status, SagaStatus::Running);
        assert!(future.deadline.is_some());
        Ok(())
    }

    #[tokio::test]
    async fn runner_starts_and_then_completes_a_saga() -> Result<(), Box<dyn std::error::Error>> {
        let store = InMemorySagaStore::default();
        let dispatcher = VecDispatcher::default();
        let runner = SagaRunner::new(BillingSaga, store, dispatcher.clone());

        let event = OrderPlaced {
            order_id: "order-1".into(),
            amount_cents: 1500,
        };

        runner.handle(&event).await?;
        runner.handle(&event).await?;

        let commands = dispatcher
            .commands
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        assert_eq!(
            commands,
            vec![
                BillingCommand::ReserveFunds {
                    order_id: "order-1".into(),
                    amount_cents: 1500,
                },
                BillingCommand::FinalizeOrder {
                    order_id: "order-1".into(),
                },
            ]
        );
        Ok(())
    }
}
