use dashmap::DashMap;
use pharos_app::{IdempotencyDecision, InboxError, InboxMessage, InboxStatus, InboxStore};
use tracing::{Instrument, info_span};
use uuid::Uuid;

/// In-memory inbox/idempotency store for tests and local usage.
#[derive(Debug, Default)]
pub struct InMemoryInboxStore {
    store: DashMap<(Uuid, String), InboxMessage>,
}

impl InMemoryInboxStore {
    /// Creates an empty in-memory inbox store.
    pub fn new() -> Self {
        Self::default()
    }

    fn key(message_id: Uuid, consumer: &str) -> (Uuid, String) {
        (message_id, consumer.to_string())
    }
}

impl InboxStore for InMemoryInboxStore {
    async fn begin_processing(
        &self,
        message_id: Uuid,
        consumer: &str,
    ) -> Result<IdempotencyDecision, InboxError> {
        async move {
            let key = Self::key(message_id, consumer);
            if let Some(existing) = self.store.get(&key) {
                return Ok(match existing.status {
                    InboxStatus::Processing => IdempotencyDecision::AlreadyProcessing,
                    InboxStatus::Completed => IdempotencyDecision::AlreadyCompleted,
                    InboxStatus::Failed => IdempotencyDecision::RetryPreviousFailure,
                });
            }

            self.store
                .insert(key, InboxMessage::processing(message_id, consumer));
            Ok(IdempotencyDecision::StartProcessing)
        }
        .instrument(info_span!("inbox.begin_processing", %message_id, consumer))
        .await
    }

    async fn mark_completed(&self, message_id: Uuid, consumer: &str) -> Result<(), InboxError> {
        async move {
            let key = Self::key(message_id, consumer);
            let mut message = self
                .store
                .get_mut(&key)
                .ok_or_else(|| InboxError::NotFound {
                    message_id,
                    consumer: consumer.to_string(),
                })?;
            message.mark_completed();
            Ok(())
        }
        .instrument(info_span!("inbox.mark_completed", %message_id, consumer))
        .await
    }

    async fn mark_failed(
        &self,
        message_id: Uuid,
        consumer: &str,
        error: String,
    ) -> Result<(), InboxError> {
        async move {
            let key = Self::key(message_id, consumer);
            let mut message = self
                .store
                .get_mut(&key)
                .ok_or_else(|| InboxError::NotFound {
                    message_id,
                    consumer: consumer.to_string(),
                })?;
            message.mark_failed(error);
            Ok(())
        }
        .instrument(info_span!("inbox.mark_failed", %message_id, consumer))
        .await
    }

    async fn get(
        &self,
        message_id: Uuid,
        consumer: &str,
    ) -> Result<Option<InboxMessage>, InboxError> {
        async move {
            Ok(self
                .store
                .get(&Self::key(message_id, consumer))
                .map(|e| e.value().clone()))
        }
        .instrument(info_span!("inbox.get", %message_id, consumer))
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn enforces_idempotency_decisions() {
        let store = InMemoryInboxStore::new();
        let message_id = Uuid::now_v7();

        assert_eq!(
            store.begin_processing(message_id, "billing").await.unwrap(),
            IdempotencyDecision::StartProcessing
        );
        assert_eq!(
            store.begin_processing(message_id, "billing").await.unwrap(),
            IdempotencyDecision::AlreadyProcessing
        );

        store
            .mark_failed(message_id, "billing", "temporary".to_string())
            .await
            .unwrap();
        assert_eq!(
            store.begin_processing(message_id, "billing").await.unwrap(),
            IdempotencyDecision::RetryPreviousFailure
        );

        store.mark_completed(message_id, "billing").await.unwrap();
        assert_eq!(
            store.begin_processing(message_id, "billing").await.unwrap(),
            IdempotencyDecision::AlreadyCompleted
        );
    }
}
