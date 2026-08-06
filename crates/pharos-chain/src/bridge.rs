//! The glue that connects chain observation to Pharos's existing seams.
//!
//! `pharos-chain` deliberately reuses the framework's messaging, inbox, saga,
//! and outbox machinery rather than inventing a parallel stack. This module is
//! the thin layer that maps chain concepts onto those seams:
//!
//! - [`chain_event_to_message`] turns a [`ChainEvent`] into a broker
//!   [`Message`] with a **deterministic id**, so the existing idempotent
//!   consumer ([`process_idempotent`](pharos_app::process_idempotent))
//!   deduplicates a re-observed transaction with no changes.
//! - [`ConfirmationState`] and its documentation describe the confirmation saga
//!   pattern built on `pharos-saga`.

use pharos_app::Message;
use uuid::Uuid;

use crate::Confirmations;
use crate::errors::ChainError;
use crate::finality::{ChainEvent, FinalityPolicy};
use crate::outpoint::OutPoint;

/// Stable namespace for deriving deterministic v5 message ids from chain events.
///
/// A fixed namespace means the same logical event always hashes to the same
/// UUID, on any machine, across restarts.
const CHAIN_EVENT_NAMESPACE: Uuid = Uuid::from_u128(0x9f2b_7c1e_4a56_4d38_8b0a_1c2d_3e4f_5061);

/// A stable string that identifies a chain event, so re-observing the same
/// event yields the same message id.
///
/// The identity is *what the event is about*, not when it was seen. A
/// [`Confirmed`](ChainEvent::Confirmed) event includes its depth because a new
/// depth is genuinely a new event; a [`BlockApplied`](ChainEvent::BlockApplied)
/// re-delivered for the same block is a duplicate.
fn event_key<Ext>(event: &ChainEvent<Ext>) -> String {
    match event {
        ChainEvent::BlockApplied(block) => format!("BlockApplied:{}", block.id),
        ChainEvent::Reorg(reorg) => format!("Reorg:{}->{}", reorg.from.block, reorg.to.block),
        ChainEvent::Confirmed { outpoint, depth } => {
            format!("Confirmed:{outpoint}:{}", depth.0)
        }
    }
}

/// Maps a [`ChainEvent`] to a broker [`Message`] ready for the outbox/inbox
/// seam.
///
/// The payload is the JSON-serialized event. The message's `message_id` is a
/// **deterministic UUID v5** derived from the event's identity ([`event_key`]),
/// which is the crux of idempotency: the inbox keys on `message_id`
/// (see [`process_idempotent`](pharos_app::process_idempotent)), so the same
/// transaction observed twice — after a restart, from an overlapping poll —
/// produces the same id and the second delivery is skipped. `Message::new`
/// would otherwise assign a random v7 id and defeat that.
///
/// The routing `key` is set to the primary subject (block or transaction id)
/// so per-key ordering holds through the `OutboxDispatcher`, and operational
/// facts (`tx_id`, `block_height`, `confirmations`, `correlation_id`) are
/// attached as headers.
pub fn chain_event_to_message<Ext>(
    event: &ChainEvent<Ext>,
    topic: impl Into<String>,
) -> Result<Message, ChainError>
where
    Ext: serde::Serialize,
{
    let key = event_key(event);
    let payload = serde_json::to_vec(event)?;

    let (routing_key, headers) = match event {
        ChainEvent::BlockApplied(block) => (
            block.id.to_string(),
            vec![("block_height".to_string(), block.height.to_string())],
        ),
        ChainEvent::Reorg(reorg) => (
            reorg.to.block.to_string(),
            vec![("block_height".to_string(), reorg.to.height.to_string())],
        ),
        ChainEvent::Confirmed { outpoint, depth } => (
            outpoint.tx.to_string(),
            vec![
                ("tx_id".to_string(), outpoint.tx.to_string()),
                ("confirmations".to_string(), depth.0.to_string()),
            ],
        ),
    };

    let mut message = Message::new(topic, payload, "application/json").with_key(routing_key);
    // Overwrite the random v7 id with a deterministic v5 id so re-observation
    // deduplicates through the standard inbox.
    message.message_id = Uuid::new_v5(&CHAIN_EVENT_NAMESPACE, key.as_bytes());
    message = message.with_header("correlation_id", key);
    for (name, value) in headers {
        message = message.with_header(name, value);
    }
    Ok(message)
}

/// Tracks how close a transaction is to being final, for a confirmation saga.
///
/// # The confirmation saga pattern
///
/// A chain transaction is not a domain fact the moment it appears — it can be
/// rolled back by a reorg. The correct pattern is a **saga that defers the
/// domain fact until finality**, built on `pharos-saga`:
///
/// - **Start.** The first time the transaction is seen (a
///   [`Confirmed`](ChainEvent::Confirmed) at low depth), the saga starts with a
///   `deadline` equal to the maximum time you're willing to wait.
/// - **Advance.** Each further confirmation advances the saga's state
///   ([`depth`](Self::depth) rises) without emitting anything.
/// - **Complete.** Once [`is_final`](Self::is_final) holds, the saga completes
///   and *only then* emits the domain fact / command.
/// - **Reorg.** A [`ReorgEvent`](crate::ReorgEvent) that rolls the transaction
///   back **before** finality merely moves the saga back (advance/ignore) or
///   fails it — because the fact was never emitted, most reorgs never become a
///   domain fact at all. This deferral is the key correctness rule.
///
/// Compensation, when needed, is a forward transition (emit a compensating
/// command): `pharos-saga` has no automatic undo. Drive the deadline through
/// [`SagaRunner::run_due_timeouts`](https://docs.rs/pharos-saga) as usual.
///
/// # Outbound submission
///
/// The mirror image on the write side: enqueue a
/// [`RawTx`](crate::RawTx) as an outbox `Message` and let a chain-adapter
/// `MessagePublisher` call [`TxSubmitter::submit`](crate::TxSubmitter) in its
/// `publish`. Wrapped in `save_and_enqueue_in`, that makes "persist the intent
/// and submit the transaction" a single atomic step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfirmationState {
    /// The output being tracked toward finality.
    pub outpoint: OutPoint,
    /// The confirmation depth observed so far.
    pub depth: Confirmations,
    /// The finality policy that decides when the fact may be emitted.
    pub required: FinalityPolicy,
}

impl ConfirmationState {
    /// Starts tracking an output at zero confirmations under a policy.
    pub fn start(outpoint: OutPoint, required: FinalityPolicy) -> Self {
        Self {
            outpoint,
            depth: Confirmations(0),
            required,
        }
    }

    /// Records a new confirmation depth.
    pub fn advance(&mut self, depth: Confirmations) {
        self.depth = depth;
    }

    /// Returns whether the tracked output has reached finality — the point at
    /// which the domain fact may be emitted.
    pub fn is_final(&self) -> bool {
        self.required.is_final(self.depth)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::finality::Confirmations;
    use crate::outpoint::OutPoint;

    #[test]
    fn identical_events_produce_the_same_message_id() -> Result<(), ChainError> {
        let event: ChainEvent = ChainEvent::Confirmed {
            outpoint: OutPoint::new("tx-1", 0),
            depth: Confirmations(1),
        };
        let a = chain_event_to_message(&event, "chain.events")?;
        let b = chain_event_to_message(&event, "chain.events")?;
        // Deterministic id: the whole point of the idempotency bridge.
        assert_eq!(a.message_id, b.message_id);
        assert_eq!(a.message_id.get_version_num(), 5);
        assert_eq!(a.key.as_deref(), Some("tx-1"));
        assert_eq!(
            a.headers.get("confirmations").map(String::as_str),
            Some("1")
        );
        Ok(())
    }

    #[test]
    fn a_deeper_confirmation_is_a_different_message() -> Result<(), ChainError> {
        let shallow: ChainEvent = ChainEvent::Confirmed {
            outpoint: OutPoint::new("tx-1", 0),
            depth: Confirmations(1),
        };
        let deep: ChainEvent = ChainEvent::Confirmed {
            outpoint: OutPoint::new("tx-1", 0),
            depth: Confirmations(6),
        };
        assert_ne!(
            chain_event_to_message(&shallow, "t")?.message_id,
            chain_event_to_message(&deep, "t")?.message_id
        );
        Ok(())
    }

    #[test]
    fn confirmation_state_defers_until_final() {
        let mut state =
            ConfirmationState::start(OutPoint::new("tx-1", 0), FinalityPolicy::Depth(6));
        assert!(!state.is_final());
        state.advance(Confirmations(5));
        assert!(!state.is_final());
        state.advance(Confirmations(6));
        assert!(state.is_final());
    }
}
