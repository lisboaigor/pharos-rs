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
