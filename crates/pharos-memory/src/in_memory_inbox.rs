use std::time::Duration;

use chrono::Utc;
use dashmap::DashMap;
use pharos_app::{IdempotencyDecision, InboxError, InboxMessage, InboxStatus, InboxStore};
use tracing::{Instrument, info_span};
use uuid::Uuid;

/// Default lease after which a `processing` inbox record may be reclaimed.
const DEFAULT_STALE_AFTER: Duration = Duration::from_secs(300);

/// In-memory inbox/idempotency store for tests and local usage.
///
/// Mirrors the PostgreSQL adapter's stale-processing lease: a `processing`
/// record older than the lease is treated as abandoned and taken over by the
/// next `begin_processing` call.
#[derive(Debug)]
pub struct InMemoryInboxStore {
    store: DashMap<(Uuid, String), InboxMessage>,
    stale_after: Duration,
}

impl Default for InMemoryInboxStore {
    fn default() -> Self {
        Self {
            store: DashMap::new(),
            stale_after: DEFAULT_STALE_AFTER,
        }
    }
}

impl InMemoryInboxStore {
    /// Creates an empty in-memory inbox store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Overrides the stale-processing lease.
    pub fn with_stale_after(mut self, stale_after: Duration) -> Self {
        self.stale_after = stale_after;
        self
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
            // The entry API holds the shard lock, making the check-and-set
            // atomic across concurrent consumers.
            match self.store.entry(key) {
                dashmap::mapref::entry::Entry::Vacant(vacant) => {
                    vacant.insert(InboxMessage::processing(message_id, consumer));
                    Ok(IdempotencyDecision::StartProcessing)
                }
                dashmap::mapref::entry::Entry::Occupied(mut occupied) => {
                    let entry = occupied.get_mut();
                    Ok(match entry.status {
                        InboxStatus::Processing => {
                            let stale_after = chrono::Duration::from_std(self.stale_after)
                                .map_err(InboxError::storage)?;
                            if entry.updated_at > Utc::now() - stale_after {
                                IdempotencyDecision::AlreadyProcessing
                            } else {
                                // Abandoned by a crashed consumer: take it over.
                                entry.updated_at = Utc::now();
                                entry.last_error = None;
                                IdempotencyDecision::StartProcessing
                            }
                        }
                        InboxStatus::Completed => IdempotencyDecision::AlreadyCompleted,
                        InboxStatus::Failed => IdempotencyDecision::RetryPreviousFailure,
                    })
                }
            }
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
    async fn enforces_idempotency_decisions() -> Result<(), Box<dyn std::error::Error>> {
        let store = InMemoryInboxStore::new();
        let message_id = Uuid::now_v7();

        assert_eq!(
            store.begin_processing(message_id, "billing").await?,
            IdempotencyDecision::StartProcessing
        );
        assert_eq!(
            store.begin_processing(message_id, "billing").await?,
            IdempotencyDecision::AlreadyProcessing
        );

        store
            .mark_failed(message_id, "billing", "temporary".to_string())
            .await?;
        assert_eq!(
            store.begin_processing(message_id, "billing").await?,
            IdempotencyDecision::RetryPreviousFailure
        );

        store.mark_completed(message_id, "billing").await?;
        assert_eq!(
            store.begin_processing(message_id, "billing").await?,
            IdempotencyDecision::AlreadyCompleted
        );
        Ok(())
    }

    #[tokio::test]
    async fn stale_processing_records_are_taken_over() -> Result<(), Box<dyn std::error::Error>> {
        let store = InMemoryInboxStore::new().with_stale_after(Duration::ZERO);
        let message_id = Uuid::now_v7();

        assert_eq!(
            store.begin_processing(message_id, "billing").await?,
            IdempotencyDecision::StartProcessing
        );
        // With a zero lease the first record is immediately stale, simulating
        // a consumer that crashed without marking the message.
        assert_eq!(
            store.begin_processing(message_id, "billing").await?,
            IdempotencyDecision::StartProcessing
        );
        Ok(())
    }
}
