use std::hash::Hash;
use std::sync::Arc;

use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use pharos_core::RepositoryError;
use pharos_es::{EventStore, Snapshot, SnapshotStore, StoredEvent};
use thiserror::Error;
use tracing::{Instrument, info_span};

/// Error type for [`InMemoryEventStore`] and [`InMemorySnapshotStore`].
///
/// The in-memory store never fails on its own; this variant exists only to
/// satisfy the `Error` bound both traits require.
#[derive(Debug, Error)]
pub enum InMemoryEventStoreError {
    /// Placeholder — the in-memory backend has no failure modes of its own.
    #[error("infallible")]
    Never,
}

/// In-memory [`EventStore`] for tests, examples, and local development.
///
/// Enforces the same optimistic-concurrency contract a durable store does:
/// `append`'s `expected_version` must match the stream's current length
/// (the sequence of its last stored event), checked and updated atomically
/// per stream.
pub struct InMemoryEventStore<I, E> {
    streams: Arc<DashMap<I, Vec<StoredEvent<E>>>>,
}

impl<I: Eq + std::hash::Hash, E> Default for InMemoryEventStore<I, E> {
    fn default() -> Self {
        Self {
            streams: Arc::new(DashMap::new()),
        }
    }
}

impl<I, E> Clone for InMemoryEventStore<I, E> {
    fn clone(&self) -> Self {
        Self {
            streams: Arc::clone(&self.streams),
        }
    }
}

impl<I: Eq + Hash, E> InMemoryEventStore<I, E> {
    /// Creates an empty in-memory event store.
    pub fn new() -> Self {
        Self::default()
    }
}

impl<I, E> EventStore<I, E> for InMemoryEventStore<I, E>
where
    I: Eq + Hash + Clone + Sync + Send + 'static,
    E: Clone + Send + Sync + 'static,
{
    type Error = InMemoryEventStoreError;

    async fn load(&self, id: &I) -> Result<Vec<StoredEvent<E>>, Self::Error> {
        async move { Ok(self.streams.get(id).map(|s| s.clone()).unwrap_or_default()) }
            .instrument(info_span!("event_store.load"))
            .await
    }

    async fn append(
        &self,
        id: &I,
        expected_version: u64,
        events: Vec<E>,
    ) -> Result<(), RepositoryError<Self::Error>> {
        async move {
            // The entry lock is held across the read-check-write, so the
            // version check and the push are atomic with respect to other
            // appenders on the same stream.
            match self.streams.entry(id.clone()) {
                Entry::Occupied(mut occupied) => {
                    let stream = occupied.get_mut();
                    let actual = stream.len() as u64;
                    if actual != expected_version {
                        return Err(RepositoryError::ConcurrencyConflict {
                            expected: expected_version,
                            actual: Some(actual),
                        });
                    }
                    let mut sequence = actual;
                    for event in events {
                        sequence += 1;
                        stream.push(StoredEvent::new(sequence, event));
                    }
                }
                Entry::Vacant(vacant) => {
                    if expected_version != 0 {
                        return Err(RepositoryError::ConcurrencyConflict {
                            expected: expected_version,
                            actual: None,
                        });
                    }
                    let stream = events
                        .into_iter()
                        .enumerate()
                        .map(|(i, event)| StoredEvent::new(i as u64 + 1, event))
                        .collect();
                    vacant.insert(stream);
                }
            }
            Ok(())
        }
        .instrument(info_span!("event_store.append"))
        .await
    }

    async fn delete_stream(&self, id: &I) -> Result<(), Self::Error> {
        async move {
            self.streams.remove(id);
            Ok(())
        }
        .instrument(info_span!("event_store.delete_stream"))
        .await
    }
}

/// In-memory [`SnapshotStore`] for tests, examples, and local development.
pub struct InMemorySnapshotStore<I, S> {
    snapshots: Arc<DashMap<I, Snapshot<S>>>,
}

impl<I: Eq + std::hash::Hash, S> Default for InMemorySnapshotStore<I, S> {
    fn default() -> Self {
        Self {
            snapshots: Arc::new(DashMap::new()),
        }
    }
}

impl<I, S> Clone for InMemorySnapshotStore<I, S> {
    fn clone(&self) -> Self {
        Self {
            snapshots: Arc::clone(&self.snapshots),
        }
    }
}

impl<I: Eq + Hash, S> InMemorySnapshotStore<I, S> {
    /// Creates an empty in-memory snapshot store.
    pub fn new() -> Self {
        Self::default()
    }
}

impl<I, S> SnapshotStore<I, S> for InMemorySnapshotStore<I, S>
where
    I: Eq + Hash + Clone + Send + Sync + 'static,
    S: Clone + Send + Sync + 'static,
{
    type Error = InMemoryEventStoreError;

    async fn load(&self, id: &I) -> Result<Option<Snapshot<S>>, Self::Error> {
        async move { Ok(self.snapshots.get(id).map(|s| s.clone())) }
            .instrument(info_span!("snapshot_store.load"))
            .await
    }

    async fn save(&self, id: &I, snapshot: Snapshot<S>) -> Result<(), Self::Error> {
        async move {
            self.snapshots.insert(id.clone(), snapshot);
            Ok(())
        }
        .instrument(info_span!("snapshot_store.save"))
        .await
    }
}
