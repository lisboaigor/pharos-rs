//! The complete idempotent-consumer flow in one call.
//!
//! Every consumer of a distributed system repeats the same dance:
//! `begin_processing` → run the business logic → `mark_completed`/`mark_failed`
//! → `ack`/`nack`. Writing it by hand invites the classic bug of forgetting
//! `mark_failed` (or the `nack`) on one error path. [`process_idempotent`]
//! owns the dance; the consumer supplies only the business logic.

use std::future::Future;

use thiserror::Error;
use tracing::{Instrument, info_span};

use crate::inbox::{IdempotencyDecision, InboxError, InboxStore};
use crate::messaging::{Delivery, MessageAcknowledger, MessagingError};

/// Outcome of one [`process_idempotent`] call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessOutcome {
    /// The message was processed and acknowledged.
    Processed,
    /// The message was a duplicate (already completed or currently being
    /// processed elsewhere) and was acknowledged without running the handler.
    SkippedDuplicate,
}

/// Error produced by the idempotent-consumer flow itself (not by the handler).
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ProcessError<E: std::error::Error> {
    /// The inbox store failed.
    #[error(transparent)]
    Inbox(#[from] InboxError),
    /// Ack/nack failed.
    #[error(transparent)]
    Messaging(#[from] MessagingError),
    /// The handler failed **and** the failure bookkeeping succeeded: the
    /// message was marked failed and nacked for redelivery.
    #[error("message processing failed (marked for redelivery): {0}")]
    Handler(#[source] E),
}

/// Runs `handle` for a delivery exactly like a disciplined consumer should.
///
/// The flow:
///
/// 1. [`InboxStore::begin_processing`] — duplicates (`AlreadyCompleted`,
///    `AlreadyProcessing`) are **acked and skipped** without running `handle`.
/// 2. `handle(&delivery)` — your business logic.
/// 3. Success → [`mark_completed`](InboxStore::mark_completed) + `ack`.
///    Failure → [`mark_failed`](InboxStore::mark_failed) + `nack(requeue)`,
///    and the handler error is returned as [`ProcessError::Handler`].
///
/// `consumer` names this consumer (or consumer group) for inbox scoping.
pub async fn process_idempotent<A, S, F, Fut, E>(
    inbox: &S,
    acknowledger: &A,
    consumer: &str,
    delivery: &Delivery,
    handle: F,
) -> Result<ProcessOutcome, ProcessError<E>>
where
    S: InboxStore,
    A: MessageAcknowledger,
    F: FnOnce(&Delivery) -> Fut,
    Fut: Future<Output = Result<(), E>>,
    E: std::error::Error + Send + Sync + 'static,
{
    let message_id = delivery.message.message_id;
    let span = info_span!(
        "consumer.process_idempotent",
        consumer,
        message_id = %message_id,
        topic = delivery.message.topic,
        attempt = delivery.attempt,
    );

    async move {
        match inbox.begin_processing(message_id, consumer).await? {
            IdempotencyDecision::AlreadyCompleted | IdempotencyDecision::AlreadyProcessing => {
                // Duplicate delivery: acknowledge so the broker stops
                // redelivering, and never run the handler again.
                acknowledger.ack(delivery).await?;
                metrics::counter!("pharos.consumer.duplicates", "consumer" => consumer.to_string())
                    .increment(1);
                Ok(ProcessOutcome::SkippedDuplicate)
            }
            IdempotencyDecision::StartProcessing | IdempotencyDecision::RetryPreviousFailure => {
                match handle(delivery).await {
                    Ok(()) => {
                        inbox.mark_completed(message_id, consumer).await?;
                        acknowledger.ack(delivery).await?;
                        metrics::counter!(
                            "pharos.consumer.processed",
                            "consumer" => consumer.to_string()
                        )
                        .increment(1);
                        Ok(ProcessOutcome::Processed)
                    }
                    Err(error) => {
                        inbox
                            .mark_failed(message_id, consumer, error.to_string())
                            .await?;
                        acknowledger.nack(delivery, true).await?;
                        metrics::counter!(
                            "pharos.consumer.failed",
                            "consumer" => consumer.to_string()
                        )
                        .increment(1);
                        Err(ProcessError::Handler(error))
                    }
                }
            }
        }
    }
    .instrument(span)
    .await
}
