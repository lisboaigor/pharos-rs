//! Proves the confirmation-saga pattern documented on
//! [`pharos_chain::ConfirmationState`]: the domain fact is emitted **only after
//! finality**, so a reorg before finality never produces a fact.
//!
//! The saga is wired against `pharos-saga`'s real `SagaRunner` with an in-memory
//! store. It reacts to confirmation observations and reorgs, advancing its state
//! until the finality depth is reached and only then dispatching the fact.

use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::{Arc, Mutex};

use pharos_chain::{Confirmations, FinalityPolicy};
use pharos_saga::{CommandDispatcher, Saga, SagaInstance, SagaRunner, SagaStore, SagaTransition};

const REQUIRED: FinalityPolicy = FinalityPolicy::Depth(6);

/// What the saga hears from the chain.
#[derive(Clone)]
enum ChainSignal {
    Observed { depth: u64 },
    Reorged,
}

/// The domain fact, emitted only once the transaction is final.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Fact {
    TransactionSettled,
}

#[derive(Clone)]
struct ConfirmState {
    depth: Confirmations,
}

struct ConfirmationSaga;

impl Saga for ConfirmationSaga {
    type Id = String;
    type State = ConfirmState;
    type Event = ChainSignal;
    type Command = Fact;
    type Error = Infallible;

    fn id_for(&self, _event: &Self::Event) -> Option<Self::Id> {
        Some("tx-1".to_string())
    }

    async fn react(
        &self,
        state: Option<&SagaInstance<Self::Id, Self::State>>,
        event: &Self::Event,
    ) -> Result<SagaTransition<Self::State, Self::Command>, Self::Error> {
        Ok(match (state, event) {
            // First sighting: start tracking, emit nothing.
            (None, ChainSignal::Observed { depth }) => SagaTransition::Start {
                state: ConfirmState {
                    depth: Confirmations(*depth),
                },
                commands: vec![],
                deadline: None,
            },
            // A reorg before we were even running is uninteresting.
            (None, ChainSignal::Reorged) => SagaTransition::Ignore,
            // A new confirmation depth: complete (emit the fact) at finality,
            // otherwise just advance — no fact yet.
            (Some(_), ChainSignal::Observed { depth }) => {
                let depth = Confirmations(*depth);
                if REQUIRED.is_final(depth) {
                    SagaTransition::Complete {
                        state: ConfirmState { depth },
                        commands: vec![Fact::TransactionSettled],
                    }
                } else {
                    SagaTransition::Advance {
                        state: ConfirmState { depth },
                        commands: vec![],
                        deadline: None,
                    }
                }
            }
            // A reorg before finality rolls confirmations back to zero. Because
            // the fact was never emitted, nothing needs compensating.
            (Some(_), ChainSignal::Reorged) => SagaTransition::Advance {
                state: ConfirmState {
                    depth: Confirmations(0),
                },
                commands: vec![],
                deadline: None,
            },
        })
    }
}

#[derive(Default, Clone)]
struct InMemorySagaStore {
    instances: Arc<Mutex<HashMap<String, SagaInstance<String, ConfirmState>>>>,
}

impl SagaStore<String, ConfirmState> for InMemorySagaStore {
    type Error = Infallible;

    async fn load(
        &self,
        id: &String,
    ) -> Result<Option<SagaInstance<String, ConfirmState>>, Self::Error> {
        let Ok(guard) = self.instances.lock() else {
            panic!("saga store mutex poisoned");
        };
        Ok(guard.get(id).cloned())
    }

    async fn save(&self, instance: SagaInstance<String, ConfirmState>) -> Result<(), Self::Error> {
        let Ok(mut guard) = self.instances.lock() else {
            panic!("saga store mutex poisoned");
        };
        guard.insert(instance.id.clone(), instance);
        Ok(())
    }
}

#[derive(Default, Clone)]
struct FactDispatcher {
    facts: Arc<Mutex<Vec<Fact>>>,
}

impl CommandDispatcher<Fact> for FactDispatcher {
    type Error = Infallible;

    async fn dispatch(&self, command: Fact) -> Result<(), Self::Error> {
        let Ok(mut guard) = self.facts.lock() else {
            panic!("dispatcher mutex poisoned");
        };
        guard.push(command);
        Ok(())
    }
}

#[tokio::test]
async fn fact_is_emitted_only_after_finality() -> Result<(), Box<dyn std::error::Error>> {
    let dispatcher = FactDispatcher::default();
    let store = InMemorySagaStore::default();
    let runner = SagaRunner::new(ConfirmationSaga, store.clone(), dispatcher.clone());

    let facts = || {
        let Ok(guard) = dispatcher.facts.lock() else {
            panic!("dispatcher mutex poisoned");
        };
        guard.clone()
    };

    // Shallow confirmations and a reorg — no fact must be emitted.
    runner.handle(&ChainSignal::Observed { depth: 1 }).await?;
    runner.handle(&ChainSignal::Observed { depth: 3 }).await?;
    assert!(facts().is_empty(), "no fact before finality");

    runner.handle(&ChainSignal::Reorged).await?;
    assert!(facts().is_empty(), "reorg before finality emits nothing");

    // Re-confirm and reach finality — now the fact is emitted, exactly once.
    runner.handle(&ChainSignal::Observed { depth: 4 }).await?;
    assert!(facts().is_empty(), "still short of the finality depth");

    runner.handle(&ChainSignal::Observed { depth: 6 }).await?;
    assert_eq!(facts(), vec![Fact::TransactionSettled]);

    // The saga persisted the finality depth it settled at.
    let instance = store
        .load(&"tx-1".to_string())
        .await?
        .ok_or("saga instance must exist")?;
    assert_eq!(instance.state.depth, Confirmations(6));
    Ok(())
}
