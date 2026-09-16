use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use tokio::sync::Notify;
use uuid::Uuid;

use crate::messaging::Message;

/// In-process wake-up signal between an outbox writer and its dispatcher.
///
/// A dispatcher normally polls the outbox table on a fixed interval — simple,
/// but it puts that interval on the critical path of "how long after commit
/// does an event's effects run". `OutboxSignal` closes that gap for same-
/// process writer/dispatcher pairs: the writer calls [`Self::notify`] right
/// after its transaction commits, and a dispatcher loop selects on
/// [`Self::notified`] alongside its regular tick, draining immediately
/// instead of waiting out the rest of the interval. The interval remains the
/// fallback — a missed or coalesced notification (multiple `notify` calls
/// before the dispatcher wakes collapse to one wake-up, same as
/// [`tokio::sync::Notify`]) is never fatal, only slower.
///
/// This is strictly a same-process optimization: it carries no information
/// across machines, so a multi-replica dispatcher still needs its interval
/// tuned to an acceptable worst-case latency.
#[derive(Clone, Default)]
pub struct OutboxSignal(Arc<Notify>);

impl OutboxSignal {
    /// Creates a new signal, initially with nothing pending.
    pub fn new() -> Self {
        Self::default()
    }

    /// Wakes one waiting [`Self::notified`] caller, if any; otherwise leaves
    /// a permit for the next call to consume immediately.
    pub fn notify(&self) {
        self.0.notify_one();
    }

    /// Resolves on the next [`Self::notify`] call (or immediately, if a
    /// permit from a previous call is still outstanding).
    pub async fn notified(&self) {
        self.0.notified().await
    }
}

/// Current lifecycle status of an outbox message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutboxStatus {
    /// Message is waiting to be published.
    Pending,
    /// Message was published successfully.
    Published,
    /// Message failed and is no longer immediately publishable.
    Failed,
    /// Message was moved to a dead-letter queue by a sweep.
    DeadLettered,
}

/// Durable message record used by the outbox pattern.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboxMessage {
    /// Outbox record id. Generated as UUID v7.
    pub id: Uuid,
    /// Broker message to publish.
    pub message: Message,
    /// Current status.
    pub status: OutboxStatus,
    /// Number of publish attempts.
    pub attempts: u32,
    /// Creation timestamp.
    pub created_at: DateTime<Utc>,
    /// Last update timestamp.
    pub updated_at: DateTime<Utc>,
    /// Earliest instant at which the message may be (re)claimed for publishing.
    ///
    /// Repositories use this both as the retry-backoff schedule (see
    /// [`OutboxRepository::schedule_retry`]) and as a claim lease:
    /// [`OutboxRepository::pending`] pushes it into the near future when it
    /// hands a message to a dispatcher, so concurrent dispatchers cannot claim
    /// the same row while one is publishing it.
    pub next_attempt_at: DateTime<Utc>,
    /// Last failure reason, when available.
    pub last_error: Option<String>,
}

impl OutboxMessage {
    /// Creates a pending outbox message.
    pub fn new(message: Message) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::now_v7(),
            message,
            status: OutboxStatus::Pending,
            attempts: 0,
            created_at: now,
            updated_at: now,
            next_attempt_at: now,
            last_error: None,
        }
    }

    /// Marks the record as published.
    pub fn mark_published(&mut self) {
        self.status = OutboxStatus::Published;
        self.updated_at = Utc::now();
        self.last_error = None;
    }

    /// Marks the record as failed and stores the reason.
    pub fn mark_failed(&mut self, error: impl Into<String>) {
        self.status = OutboxStatus::Failed;
        self.updated_at = Utc::now();
        self.last_error = Some(error.into());
    }

    /// Increments the publish attempt count.
    pub fn record_attempt(&mut self) {
        self.attempts += 1;
        self.updated_at = Utc::now();
    }
}

/// Errors produced by outbox repositories.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum OutboxError {
    /// The outbox record does not exist.
    #[error("outbox message not found: {0}")]
    NotFound(Uuid),
    /// Adapter-specific failure; the source carries the original error.
    #[error("outbox storage failed: {0}")]
    Storage(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),
}

impl OutboxError {
    /// Wraps any `Error + Send + Sync + 'static` as a storage failure.
    pub fn storage(e: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::Storage(Box::new(e))
    }
}

/// Stores and updates outbox messages.
#[trait_variant::make(Send)]
pub trait OutboxRepository: Sync + 'static {
    /// Inserts a pending outbox message.
    async fn insert(&self, message: OutboxMessage) -> Result<(), OutboxError>;
    /// Inserts several pending outbox messages.
    ///
    /// The default implementation calls [`Self::insert`] once per message,
    /// stopping at the first error — it exists so every implementation gets
    /// a working `insert_many` for free, not because looping over `insert`
    /// is fast. A store that can batch the underlying writes (a single
    /// multi-row `INSERT`, a pipelined command) should override this: the
    /// framework's own outbox path issues one `insert` per event with no
    /// batching anywhere, which is the dominant cost in the outbox
    /// throughput numbers in `docs/guide/benchmarks.md`.
    // Provided (default) methods keep the manual `-> impl Future<..> +
    // Send { async move { .. } }` shape rather than `async fn`:
    // `#[trait_variant::make(Send)]` only rewrites a bodyless `async fn`'s
    // signature — for an item that already has a body, it passes that body
    // through unchanged while stripping `asyncness` from the signature, so
    // an `async fn` body written with bare top-level `.await` stops being
    // inside an async context. Wrapping the body in `async move` here is
    // what keeps it valid post-expansion.
    fn insert_many(
        &self,
        messages: Vec<OutboxMessage>,
    ) -> impl Future<Output = Result<(), OutboxError>> + Send {
        async move {
            for message in messages {
                self.insert(message).await?;
            }
            Ok(())
        }
    }
    /// Claims up to `limit` due pending messages, ordered by creation time
    /// where supported.
    ///
    /// Implementations that support concurrent dispatchers must make the claim
    /// atomic (e.g. `FOR UPDATE SKIP LOCKED` inside a single statement) and
    /// lease the claimed rows by moving their `next_attempt_at` into the
    /// future, so two dispatchers never publish the same message while one of
    /// them holds the lease.
    async fn pending(&self, limit: usize) -> Result<Vec<OutboxMessage>, OutboxError>;
    /// Records one publication attempt.
    async fn record_attempt(&self, id: Uuid) -> Result<(), OutboxError>;
    /// Schedules the next retry of a still-pending message after `delay`.
    ///
    /// Called by the dispatcher when a publish fails but the retry policy still
    /// has budget, so the configured backoff is actually honored between polls.
    /// The default implementation is a no-op for stores without scheduling
    /// support: the message is simply retried on the next poll.
    fn schedule_retry(
        &self,
        id: Uuid,
        delay: Duration,
    ) -> impl Future<Output = Result<(), OutboxError>> + Send {
        let _ = (id, delay);
        async { Ok(()) }
    }
    /// Marks a message as published.
    async fn mark_published(&self, id: Uuid) -> Result<(), OutboxError>;
    /// Marks a message as failed.
    async fn mark_failed(&self, id: Uuid, error: String) -> Result<(), OutboxError>;
    /// Lists up to `limit` messages in the terminal `failed` state, oldest
    /// first where supported. Used by the dead-letter sweep.
    async fn failed(&self, limit: usize) -> Result<Vec<OutboxMessage>, OutboxError>;
    /// Marks a failed message as dead-lettered so a sweep never parks it twice.
    async fn mark_dead_lettered(&self, id: Uuid) -> Result<(), OutboxError>;
}

/// Moves terminally `failed` outbox messages onto a [`DeadLetterQueue`].
///
/// Run it periodically next to the dispatcher: each swept message becomes a
/// [`DeadLetterMessage`] carrying the outbox row's `last_error` and attempt
/// count, then the row is marked `dead_lettered` so it is swept exactly once.
/// Returns how many messages were parked.
///
/// [`DeadLetterQueue`]: crate::dead_letter::DeadLetterQueue
/// [`DeadLetterMessage`]: crate::dead_letter::DeadLetterMessage
pub async fn sweep_failed_to_dead_letter<R, Q>(
    outbox: &R,
    dlq: &Q,
    limit: usize,
) -> Result<usize, SweepError>
where
    R: OutboxRepository,
    Q: crate::dead_letter::DeadLetterQueue,
{
    let failed = outbox.failed(limit).await?;
    let mut swept = 0;
    for message in failed {
        let reason = message
            .last_error
            .clone()
            .unwrap_or_else(|| "publish failed".to_string());
        let dead = crate::dead_letter::DeadLetterMessage::new(
            message.message.clone(),
            reason,
            message.attempts,
        );
        dlq.dead_letter(dead).await?;
        outbox.mark_dead_lettered(message.id).await?;
        metrics::counter!("pharos.outbox.swept_to_dead_letter").increment(1);
        swept += 1;
    }
    Ok(swept)
}

/// Error produced by [`sweep_failed_to_dead_letter`].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SweepError {
    /// Outbox storage failed.
    #[error(transparent)]
    Outbox(#[from] OutboxError),
    /// The dead-letter queue failed.
    #[error(transparent)]
    DeadLetter(#[from] crate::dead_letter::DeadLetterError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn outbox_message_tracks_status_and_attempts() {
        let message = Message::new("orders", b"{}".to_vec(), "application/json");
        let mut outbox = OutboxMessage::new(message);

        assert_eq!(outbox.id.get_version_num(), 7);
        assert_eq!(outbox.status, OutboxStatus::Pending);
        assert_eq!(outbox.attempts, 0);

        outbox.record_attempt();
        assert_eq!(outbox.attempts, 1);

        outbox.mark_failed("broker unavailable");
        assert_eq!(outbox.status, OutboxStatus::Failed);
        assert_eq!(outbox.last_error.as_deref(), Some("broker unavailable"));

        outbox.mark_published();
        assert_eq!(outbox.status, OutboxStatus::Published);
        assert_eq!(outbox.last_error, None);
    }
}
