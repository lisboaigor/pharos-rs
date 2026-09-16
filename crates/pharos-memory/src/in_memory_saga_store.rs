use std::hash::Hash;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use pharos_saga::{SagaInstance, SagaSaveError, SagaStatus, SagaStore, SagaTimeoutStore};
use thiserror::Error;
use tokio::sync::Mutex;
use tracing::{Instrument, info_span};

/// Error type for [`InMemorySagaStore`].
///
/// The in-memory store never fails on its own; this variant exists only to
/// satisfy the `Error` bound on [`SagaStore::Error`].
#[derive(Debug, Error)]
pub enum InMemorySagaStoreError {
    /// Placeholder — the in-memory backend has no failure modes of its own.
    #[error("infallible")]
    Never,
}

/// In-memory [`SagaStore`] + [`SagaTimeoutStore`] for one saga type, for
/// tests, examples, and local development.
///
/// `claim_due` serializes against `save` through a single lock covering the
/// whole store: a real backend arbitrates the two through its own
/// compare-and-swap (`SagaStore::save`'s optimistic-concurrency check races
/// `claim_due`'s own version bump, and whichever loses gets
/// [`SagaSaveError::ConcurrencyConflict`]) — see `pharos-postgres`'s
/// `PgSagaStore` for the row-level version of the same guarantee. A single
/// lock is the in-memory equivalent: coarser, but every claim and every save
/// still observes a consistent, serialized view of the store.
pub struct InMemorySagaStore<I, S> {
    store: Arc<DashMap<I, SagaInstance<I, S>>>,
    lock: Arc<Mutex<()>>,
}

impl<I: Eq + std::hash::Hash, S> Default for InMemorySagaStore<I, S> {
    fn default() -> Self {
        Self {
            store: Arc::new(DashMap::new()),
            lock: Arc::new(Mutex::new(())),
        }
    }
}

impl<I, S> Clone for InMemorySagaStore<I, S> {
    fn clone(&self) -> Self {
        Self {
            store: Arc::clone(&self.store),
            lock: Arc::clone(&self.lock),
        }
    }
}

impl<I: Eq + Hash, S> InMemorySagaStore<I, S> {
    /// Creates an empty in-memory saga store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the number of stored saga instances.
    pub fn len(&self) -> usize {
        self.store.len()
    }

    /// Returns `true` when no instance is stored.
    pub fn is_empty(&self) -> bool {
        self.store.is_empty()
    }
}

impl<I, S> SagaStore<I, S> for InMemorySagaStore<I, S>
where
    I: Eq + Hash + Clone + Send + Sync + 'static,
    S: Clone + Send + Sync + 'static,
{
    type Error = InMemorySagaStoreError;

    async fn load(&self, id: &I) -> Result<Option<SagaInstance<I, S>>, Self::Error> {
        async move { Ok(self.store.get(id).map(|entry| entry.value().clone())) }
            .instrument(info_span!("saga_store.load"))
            .await
    }

    async fn save(&self, instance: SagaInstance<I, S>) -> Result<(), SagaSaveError<Self::Error>> {
        async move {
            let expected = instance.version;
            let new_version = expected + 1;

            // Held across the whole check-and-write so a concurrent
            // `claim_due` (which also bumps `version`) can never interleave
            // between the version check and the write.
            let _guard = self.lock.lock().await;

            match self.store.entry(instance.id.clone()) {
                Entry::Vacant(vacant) => {
                    if expected != 0 {
                        return Err(SagaSaveError::ConcurrencyConflict {
                            expected,
                            actual: None,
                        });
                    }
                    vacant.insert(SagaInstance {
                        version: new_version,
                        ..instance
                    });
                }
                Entry::Occupied(mut occupied) => {
                    let actual = occupied.get().version;
                    if actual != expected {
                        return Err(SagaSaveError::ConcurrencyConflict {
                            expected,
                            actual: Some(actual),
                        });
                    }
                    occupied.insert(SagaInstance {
                        version: new_version,
                        ..instance
                    });
                }
            }
            Ok(())
        }
        .instrument(info_span!("saga_store.save"))
        .await
    }
}

impl<I, S> SagaTimeoutStore<I, S> for InMemorySagaStore<I, S>
where
    I: Eq + Hash + Clone + Send + Sync + 'static,
    S: Clone + Send + Sync + 'static,
{
    async fn claim_due(
        &self,
        now: DateTime<Utc>,
        lease: chrono::Duration,
        limit: usize,
    ) -> Result<Vec<SagaInstance<I, S>>, Self::Error> {
        async move {
            let _guard = self.lock.lock().await;

            let mut due: Vec<I> = self
                .store
                .iter()
                .filter(|entry| {
                    entry.status == SagaStatus::Running
                        && entry.deadline.is_some_and(|deadline| deadline <= now)
                })
                .map(|entry| entry.key().clone())
                .collect();
            due.sort_by_key(|id| {
                self.store
                    .get(id)
                    .and_then(|entry| entry.deadline)
                    .unwrap_or(now)
            });
            due.truncate(limit);

            let mut claimed = Vec::with_capacity(due.len());
            for id in due {
                if let Some(mut entry) = self.store.get_mut(&id) {
                    // The caller gets the original, elapsed deadline; the
                    // stored row moves to the lease so no other claimer
                    // picks this instance up before it either resolves or
                    // the lease itself expires. The version bump is what
                    // makes a racing `save` for the same saga lose with
                    // `ConcurrencyConflict` instead of silently clobbering
                    // this claim.
                    let original_deadline = entry.deadline;
                    entry.deadline = Some(now + lease);
                    entry.version += 1;
                    claimed.push(SagaInstance {
                        deadline: original_deadline,
                        ..entry.clone()
                    });
                }
            }
            Ok(claimed)
        }
        .instrument(info_span!("saga_store.claim_due"))
        .await
    }
}
