use chrono::{DateTime, Utc};
use thiserror::Error;
use uuid::Uuid;

use crate::messaging::Message;

/// Message moved to a dead-letter path after processing or publishing failures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeadLetterMessage {
    /// Dead-letter record id.
    pub id: Uuid,
    /// Original message.
    pub message: Message,
    /// Failure reason.
    pub reason: String,
    /// Number of attempts observed before dead-lettering.
    pub attempts: u32,
    /// Timestamp when the message was dead-lettered.
    pub dead_lettered_at: DateTime<Utc>,
}

impl DeadLetterMessage {
    /// Creates a new dead-letter record.
    pub fn new(message: Message, reason: impl Into<String>, attempts: u32) -> Self {
        Self {
            id: Uuid::now_v7(),
            message,
            reason: reason.into(),
            attempts,
            dead_lettered_at: Utc::now(),
        }
    }
}

/// Errors produced by dead-letter queue implementations.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum DeadLetterError {
    /// Storage or adapter failure.
    #[error("dead-letter queue failed: {0}")]
    Storage(String),
}

/// Stores messages that can no longer be processed successfully.
#[trait_variant::make(Send)]
pub trait DeadLetterQueue: Sync + 'static {
    /// Sends a message to the dead-letter queue.
    async fn dead_letter(&self, message: DeadLetterMessage) -> Result<(), DeadLetterError>;
    /// Lists dead-letter messages, where supported.
    async fn list(&self, limit: usize) -> Result<Vec<DeadLetterMessage>, DeadLetterError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_dead_letter_message_with_uuid_v7() {
        let message = Message::new("orders", b"payload".to_vec(), "text/plain");
        let dead_letter = DeadLetterMessage::new(message, "failed", 3);

        assert_eq!(dead_letter.id.get_version_num(), 7);
        assert_eq!(dead_letter.reason, "failed");
        assert_eq!(dead_letter.attempts, 3);
    }
}
