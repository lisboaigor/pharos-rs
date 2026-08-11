//! The complete idempotent-consumer flow in one call.
//!
//! Every consumer of a distributed system repeats the same dance:
//! `begin_processing` → run the business logic → `mark_completed`/`mark_failed`
//! → `ack`/`nack`. Writing it by hand invites the classic bug of forgetting
//! `mark_failed` (or the `nack`) on one error path. [`process_idempotent`]
//! owns the dance; the consumer supplies only the business logic.
//!
//! [`process_idempotent_with_retry`] owns the same dance plus one more
//! decision a broker consumer cannot skip: when to stop retrying. Without a
//! bound, a message whose handler always fails — a malformed payload, a bug
//! triggered by one specific value — nacks with `requeue: true` forever.
//! Every adapter's `nack(_, true)` puts the message straight back at the head
//! of the same queue, so the very next `next()` call receives it again: a
//! self-sustaining loop against the consumer's own infrastructure, and it
//! only takes one bad message to start it. [`process_idempotent_with_retry`]
//! consults a [`RetryPolicy`] against [`Delivery::attempt`] and dead-letters
//! once the budget is spent, so the loop has a floor.

use std::future::Future;

use thiserror::Error;
use tracing::{Instrument, info_span};

use crate::dead_letter::{DeadLetterError, DeadLetterMessage, DeadLetterQueue};
use crate::inbox::{IdempotencyDecision, InboxError, InboxStore};
use crate::messaging::{Delivery, MessageAcknowledger, MessagingError, RetryDecision, RetryPolicy};

/// Outcome of one [`process_idempotent`] call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessOutcome {
    /// The message was processed and acknowledged.
    Processed,
    /// The message was a duplicate (already completed or currently being
    /// processed elsewhere) and was acknowledged without running the handler.
    SkippedDuplicate,
    /// The handler failed and the retry budget was spent: the message was
    /// dead-lettered and acknowledged so the broker stops redelivering it.
    /// Only returned by [`process_idempotent_with_retry`].
    DeadLettered,
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
    /// Moving the message to the dead-letter queue failed. The handler's
    /// original failure is preserved in `handler_error` — losing it in favor
    /// of the dead-letter error would erase the reason the message is being
    /// parked in the first place.
    #[error(
        "moving message to dead-letter queue failed: {source} (handler had failed with: {handler_error})"
    )]
    DeadLetter {
        /// The dead-letter queue's own failure.
        #[source]
        source: DeadLetterError,
        /// The handler error that triggered dead-lettering, stringified since
        /// `E` was already consumed to build the dead-letter record.
        handler_error: String,
    },
    /// The handler failed **and** the failure bookkeeping succeeded: the
    /// message was marked failed and nacked for redelivery (or dead-lettered,
    /// via [`process_idempotent_with_retry`]).
    #[error("message processing failed: {0}")]
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
///    Failure → [`mark_failed`](InboxStore::mark_failed) +
///    **`nack(delivery, true)` unconditionally** — every failure is requeued,
///    with no bound.
///
/// # This has no retry ceiling — prefer [`process_idempotent_with_retry`]
///
/// A message whose handler can never succeed (a payload the handler cannot
/// parse, a bug on one specific value) is nacked with `requeue: true` on
/// every attempt, forever: nothing here ever chooses to stop. Reach for this
/// function only when the caller has its own bound around it (a saga step
/// with its own timeout, a one-off tool); for a broker consumer loop, use
/// [`process_idempotent_with_retry`], which dead-letters once a
/// [`RetryPolicy`] says to.
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

/// Runs `handle` for a delivery, dead-lettering once `retry` says to stop.
///
/// Identical to [`process_idempotent`] through duplicate detection and the
/// happy path. On a handler failure, `retry.decide(delivery.attempt)` chooses
/// between the two outcomes a broker consumer actually has:
///
/// - [`RetryDecision::RetryAfter`] — nacks with `requeue: true`, same as
///   [`process_idempotent`]. The delay itself is *not* slept here: this
///   function runs inline in the consumer's poll loop, and a partition is
///   processed sequentially, so blocking on a timer would stall every other
///   message behind this one — the delay is informational, for a caller that
///   wants to log it or feed it into its own backoff between `next()` calls.
/// - [`RetryDecision::DeadLetter`] — the message is recorded on `dead_letter`
///   with the handler's error as the reason and `delivery.attempt` as the
///   attempt count, then nacked with `requeue: false` so the broker stops
///   redelivering it. The event is never silently dropped: it is durably
///   parked for offline inspection, the same contract `pharos_app`'s
///   `DeadLettering` decorator already gives in-process event-handler
///   retries.
///
/// This requires [`Delivery::attempt`] to reflect prior deliveries — an
/// adapter that always reports `1` (the default `Delivery::new` value) makes
/// `retry` see every attempt as the first one and never dead-letter. Kafka's
/// adapter reads the `pharos.retry.attempt` header it itself writes on nack
/// back into `Delivery::attempt` for exactly this reason.
pub async fn process_idempotent_with_retry<A, S, Q, F, Fut, E>(
    inbox: &S,
    acknowledger: &A,
    dead_letter: &Q,
    consumer: &str,
    delivery: &Delivery,
    retry: &RetryPolicy,
    handle: F,
) -> Result<ProcessOutcome, ProcessError<E>>
where
    S: InboxStore,
    A: MessageAcknowledger,
    Q: DeadLetterQueue,
    F: FnOnce(&Delivery) -> Fut,
    Fut: Future<Output = Result<(), E>>,
    E: std::error::Error + Send + Sync + 'static,
{
    let message_id = delivery.message.message_id;
    let span = info_span!(
        "consumer.process_idempotent_with_retry",
        consumer,
        message_id = %message_id,
        topic = delivery.message.topic,
        attempt = delivery.attempt,
    );

    async move {
        match inbox.begin_processing(message_id, consumer).await? {
            IdempotencyDecision::AlreadyCompleted | IdempotencyDecision::AlreadyProcessing => {
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

                        match retry.decide(delivery.attempt) {
                            RetryDecision::RetryAfter(_delay) => {
                                acknowledger.nack(delivery, true).await?;
                                metrics::counter!(
                                    "pharos.consumer.failed",
                                    "consumer" => consumer.to_string()
                                )
                                .increment(1);
                                Err(ProcessError::Handler(error))
                            }
                            RetryDecision::DeadLetter => {
                                let handler_error = error.to_string();
                                let dead = DeadLetterMessage::new(
                                    delivery.message.clone(),
                                    handler_error.clone(),
                                    delivery.attempt,
                                );
                                if let Err(source) = dead_letter.dead_letter(dead).await {
                                    return Err(ProcessError::DeadLetter {
                                        source,
                                        handler_error,
                                    });
                                }
                                acknowledger.nack(delivery, false).await?;
                                metrics::counter!(
                                    "pharos.consumer.dead_lettered",
                                    "consumer" => consumer.to_string()
                                )
                                .increment(1);
                                Err(ProcessError::Handler(error))
                            }
                        }
                    }
                }
            }
        }
    }
    .instrument(span)
    .await
}
