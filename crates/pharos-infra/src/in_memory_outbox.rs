use dashmap::DashMap;
use pharos_app::{OutboxError, OutboxMessage, OutboxRepository, OutboxStatus};
use tracing::{Instrument, info_span};
use uuid::Uuid;

/// In-memory [`OutboxRepository`] implementation for tests and local usage.
#[derive(Debug, Default)]
pub struct InMemoryOutboxRepository {
    store: DashMap<Uuid, OutboxMessage>,
}

impl InMemoryOutboxRepository {
    /// Creates an empty in-memory outbox repository.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the number of stored outbox records.
    pub fn len(&self) -> usize {
        self.store.len()
    }

    /// Returns `true` when no records are stored.
    pub fn is_empty(&self) -> bool {
        self.store.is_empty()
    }

    /// Returns a stored message by id.
    pub fn get(&self, id: Uuid) -> Option<OutboxMessage> {
        self.store.get(&id).map(|entry| entry.value().clone())
    }
}

impl OutboxRepository for InMemoryOutboxRepository {
    async fn insert(&self, message: OutboxMessage) -> Result<(), OutboxError> {
        async move {
            self.store.insert(message.id, message);
            Ok(())
        }
        .instrument(info_span!("outbox.insert"))
        .await
    }

    async fn pending(&self, limit: usize) -> Result<Vec<OutboxMessage>, OutboxError> {
        async move {
            let mut messages: Vec<_> = self
                .store
                .iter()
                .filter(|entry| entry.status == OutboxStatus::Pending)
                .map(|entry| entry.value().clone())
                .collect();
            messages.sort_by_key(|message| message.created_at);
            messages.truncate(limit);
            Ok(messages)
        }
        .instrument(info_span!("outbox.pending", limit))
        .await
    }

    async fn record_attempt(&self, id: Uuid) -> Result<(), OutboxError> {
        async move {
            let mut message = self.store.get_mut(&id).ok_or(OutboxError::NotFound(id))?;
            message.record_attempt();
            Ok(())
        }
        .instrument(info_span!("outbox.record_attempt", %id))
        .await
    }

    async fn mark_published(&self, id: Uuid) -> Result<(), OutboxError> {
        async move {
            let mut message = self.store.get_mut(&id).ok_or(OutboxError::NotFound(id))?;
            message.mark_published();
            Ok(())
        }
        .instrument(info_span!("outbox.mark_published", %id))
        .await
    }

    async fn mark_failed(&self, id: Uuid, error: String) -> Result<(), OutboxError> {
        async move {
            let mut message = self.store.get_mut(&id).ok_or(OutboxError::NotFound(id))?;
            message.mark_failed(error);
            Ok(())
        }
        .instrument(info_span!("outbox.mark_failed", %id))
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pharos_app::{Message, OutboxStatus};

    #[tokio::test]
    async fn stores_and_updates_outbox_messages() {
        let repo = InMemoryOutboxRepository::new();
        let outbox = OutboxMessage::new(Message::new("orders", b"{}".to_vec(), "application/json"));
        let id = outbox.id;

        repo.insert(outbox).await.unwrap();
        assert_eq!(repo.len(), 1);
        assert_eq!(repo.pending(10).await.unwrap().len(), 1);

        repo.record_attempt(id).await.unwrap();
        repo.mark_published(id).await.unwrap();

        let stored = repo.get(id).unwrap();
        assert_eq!(stored.attempts, 1);
        assert_eq!(stored.status, OutboxStatus::Published);
        assert!(repo.pending(10).await.unwrap().is_empty());
    }
}
