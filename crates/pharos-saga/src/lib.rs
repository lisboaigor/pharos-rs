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
    /// Last update timestamp.
    pub updated_at: DateTime<Utc>,
}

impl<I, S> SagaInstance<I, S> {
    /// Creates a running saga instance.
    pub fn running(id: I, state: S) -> Self {
        Self {
            id,
            state,
            status: SagaStatus::Running,
            updated_at: Utc::now(),
        }
    }
}

/// Transition produced by a saga in response to an event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SagaTransition<S, C> {
    /// The saga is not interested in the event.
    Ignore,
    /// Start a new saga instance.
    Start { state: S, commands: Vec<C> },
    /// Update an already-running saga.
    Advance { state: S, commands: Vec<C> },
    /// Complete the saga.
    Complete { state: S, commands: Vec<C> },
    /// Fail the saga with a reason.
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

        match transition {
            SagaTransition::Ignore => Ok(()),
            SagaTransition::Start { state, commands } => {
                let instance = SagaInstance::running(id, state);
                self.store
                    .save(instance)
                    .await
                    .map_err(SagaRunnerError::Store)?;
                self.dispatch_all(commands).await
            }
            SagaTransition::Advance { state, commands } => {
                let mut instance =
                    current.unwrap_or_else(|| SagaInstance::running(id, state.clone()));
                instance.state = state;
                instance.status = SagaStatus::Running;
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
