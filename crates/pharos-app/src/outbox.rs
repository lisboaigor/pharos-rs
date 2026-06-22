use std::future::Future;

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
#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum OutboxError {
    /// The outbox record does not exist.
    #[error("outbox message not found: {0}")]
    NotFound(Uuid),
    /// Adapter-specific failure.
    #[error("outbox storage failed: {0}")]
    Storage(String),
}

/// Stores and updates outbox messages.
pub trait OutboxRepository: Send + Sync + 'static {
    /// Inserts a pending outbox message.
    fn insert(
        &self,
        message: OutboxMessage,
    ) -> impl Future<Output = Result<(), OutboxError>> + Send;
    /// Fetches pending messages, ordered by creation time where supported.
    fn pending(
        &self,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<OutboxMessage>, OutboxError>> + Send;
    /// Records one publication attempt.
    fn record_attempt(&self, id: Uuid) -> impl Future<Output = Result<(), OutboxError>> + Send;
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
