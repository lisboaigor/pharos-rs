use std::future::Future;
use std::time::Duration;

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::messaging::Message;

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
pub trait OutboxRepository: Send + Sync + 'static {
    /// Inserts a pending outbox message.
    fn insert(
        &self,
        message: OutboxMessage,
    ) -> impl Future<Output = Result<(), OutboxError>> + Send;
    /// Claims up to `limit` due pending messages, ordered by creation time
    /// where supported.
    ///
    /// Implementations that support concurrent dispatchers must make the claim
    /// atomic (e.g. `FOR UPDATE SKIP LOCKED` inside a single statement) and
    /// lease the claimed rows by moving their `next_attempt_at` into the
    /// future, so two dispatchers never publish the same message while one of
    /// them holds the lease.
    fn pending(
        &self,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<OutboxMessage>, OutboxError>> + Send;
    /// Records one publication attempt.
    fn record_attempt(&self, id: Uuid) -> impl Future<Output = Result<(), OutboxError>> + Send;
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
    fn mark_published(&self, id: Uuid) -> impl Future<Output = Result<(), OutboxError>> + Send;
    /// Marks a message as failed.
    fn mark_failed(
        &self,
        id: Uuid,
        error: String,
    ) -> impl Future<Output = Result<(), OutboxError>> + Send;
    /// Lists up to `limit` messages in the terminal `failed` state, oldest
    /// first where supported. Used by the dead-letter sweep.
    fn failed(
        &self,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<OutboxMessage>, OutboxError>> + Send;
    /// Marks a failed message as dead-lettered so a sweep never parks it twice.
    fn mark_dead_lettered(&self, id: Uuid) -> impl Future<Output = Result<(), OutboxError>> + Send;
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
