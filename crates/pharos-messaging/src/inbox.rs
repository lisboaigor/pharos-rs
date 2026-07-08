use std::future::Future;

use chrono::{DateTime, Utc};
use uuid::Uuid;

/// Processing status for an inbox/idempotency record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InboxStatus {
    /// Processing has started but not completed yet.
    Processing,
    /// Processing completed successfully.
    Completed,
    /// Processing failed.
    Failed,
}

/// Inbox record used to implement idempotent consumers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboxMessage {
    /// Message id received from the broker.
    pub message_id: Uuid,
    /// Consumer name or consumer group.
    pub consumer: String,
    /// Processing status.
    pub status: InboxStatus,
    /// First-seen timestamp.
    pub received_at: DateTime<Utc>,
    /// Last update timestamp.
    pub updated_at: DateTime<Utc>,
    /// Last processing error, when available.
    pub last_error: Option<String>,
}

impl InboxMessage {
    /// Creates a processing inbox record.
    pub fn processing(message_id: Uuid, consumer: impl Into<String>) -> Self {
        let now = Utc::now();
        Self {
            message_id,
            consumer: consumer.into(),
            status: InboxStatus::Processing,
            received_at: now,
            updated_at: now,
            last_error: None,
        }
    }

    /// Marks processing as completed.
    pub fn mark_completed(&mut self) {
        self.status = InboxStatus::Completed;
        self.updated_at = Utc::now();
        self.last_error = None;
    }

    /// Marks processing as failed.
    pub fn mark_failed(&mut self, error: impl Into<String>) {
        self.status = InboxStatus::Failed;
        self.updated_at = Utc::now();
        self.last_error = Some(error.into());
    }
}

/// Result of trying to start idempotent processing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdempotencyDecision {
    /// Message was not seen before and processing may start.
    StartProcessing,
    /// Message is already being processed by this consumer.
    AlreadyProcessing,
    /// Message has already been processed successfully.
    AlreadyCompleted,
    /// Message failed previously and may be retried.
    RetryPreviousFailure,
}

/// Errors produced by inbox stores.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum InboxError {
    /// Inbox record does not exist.
    #[error("inbox message not found: message_id={message_id}, consumer={consumer}")]
    NotFound {
        /// Message id.
        message_id: Uuid,
        /// Consumer name.
        consumer: String,
    },
    /// Adapter-specific failure; the source carries the original error.
    #[error("inbox storage failed: {0}")]
    Storage(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),
}

impl InboxError {
    /// Wraps any `Error + Send + Sync + 'static` as a storage failure.
    pub fn storage(e: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::Storage(Box::new(e))
    }
}

/// Stores inbox records and implements consumer idempotency.
pub trait InboxStore: Send + Sync + 'static {
    /// Starts processing or returns the current idempotency decision.
    fn begin_processing(
        &self,
        message_id: Uuid,
        consumer: &str,
    ) -> impl Future<Output = Result<IdempotencyDecision, InboxError>> + Send;

    /// Marks a message as successfully processed.
    fn mark_completed(
        &self,
        message_id: Uuid,
        consumer: &str,
    ) -> impl Future<Output = Result<(), InboxError>> + Send;

    /// Marks a message as failed.
    fn mark_failed(
        &self,
        message_id: Uuid,
        consumer: &str,
        error: String,
    ) -> impl Future<Output = Result<(), InboxError>> + Send;

    /// Returns the inbox record when it exists.
    fn get(
        &self,
        message_id: Uuid,
        consumer: &str,
    ) -> impl Future<Output = Result<Option<InboxMessage>, InboxError>> + Send;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inbox_message_tracks_processing_status() {
        let message_id = Uuid::now_v7();
        let mut inbox = InboxMessage::processing(message_id, "billing");

        assert_eq!(inbox.message_id, message_id);
        assert_eq!(inbox.consumer, "billing");
        assert_eq!(inbox.status, InboxStatus::Processing);

        inbox.mark_failed("temporary failure");
        assert_eq!(inbox.status, InboxStatus::Failed);
        assert_eq!(inbox.last_error.as_deref(), Some("temporary failure"));

        inbox.mark_completed();
        assert_eq!(inbox.status, InboxStatus::Completed);
        assert_eq!(inbox.last_error, None);
    }
}
