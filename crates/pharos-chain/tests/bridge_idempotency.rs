//! Proves the chain bridge deduplicates a re-observed transaction through the
//! framework's *real* idempotent-consumer seam, with no changes to it.
//!
//! A fake in-memory `ChainSource` emits the same `ChainEvent` twice (a chain
//! layer legitimately re-observes a transaction after a restart or an
//! overlapping poll). Each observation is mapped to a `Message` by
//! `chain_event_to_message` and fed through `process_idempotent` backed by
//! `pharos-memory`'s in-memory inbox and broker. The first is processed; the
//! second is skipped as a duplicate — because the bridge assigns a deterministic
//! message id.

use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use pharos_app::{Delivery, ProcessOutcome, process_idempotent};
use pharos_chain::{
    ChainCursor, ChainEvent, ChainSource, Confirmations, OutPoint, chain_event_to_message,
};
use pharos_memory::{InMemoryInboxStore, InMemoryMessageBroker};

/// A `ChainSource` that always reports the same confirmation event — standing in
/// for an indexer that re-observes the same transaction.
struct RepeatingSource {
    event: ChainEvent,
}

impl ChainSource for RepeatingSource {
    type Ext = ();
    type Error = Infallible;

    async fn tip(&self) -> Result<ChainCursor, Self::Error> {
        Ok(ChainCursor::new(pharos_chain::BlockHeight(1), "block-1"))
    }

    async fn next_from(&self, _cursor: &ChainCursor) -> Result<Option<ChainEvent>, Self::Error> {
        Ok(Some(self.event.clone()))
    }
}

#[tokio::test]
async fn re_observed_transaction_is_processed_once() -> Result<(), Box<dyn std::error::Error>> {
    let source = RepeatingSource {
        event: ChainEvent::Confirmed {
            outpoint: OutPoint::new("tx-42", 0),
            depth: Confirmations(1),
        },
    };
    let inbox = InMemoryInboxStore::new();
    let broker = InMemoryMessageBroker::new();
    let handled = Arc::new(AtomicU32::new(0));

    // Observe the same event twice and run each through the real seam.
    let cursor = source.tip().await?;
    let mut outcomes = Vec::new();
    for _ in 0..2 {
        let event = source
            .next_from(&cursor)
            .await?
            .ok_or("source always yields an event")?;
        let message = chain_event_to_message(&event, "chain.events")?;
        let delivery = Delivery::new(message);

        let handled = Arc::clone(&handled);
        let outcome = process_idempotent(&inbox, &broker, "projection", &delivery, |_d| {
            let handled = Arc::clone(&handled);
            async move {
                handled.fetch_add(1, Ordering::SeqCst);
                Ok::<(), Infallible>(())
            }
        })
        .await?;
        outcomes.push(outcome);
    }

    assert_eq!(outcomes[0], ProcessOutcome::Processed);
    assert_eq!(outcomes[1], ProcessOutcome::SkippedDuplicate);
    // The business handler ran exactly once despite two observations.
    assert_eq!(handled.load(Ordering::SeqCst), 1);
    Ok(())
}
