use std::{any::type_name, sync::Arc};

use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use pharos_core::{AggregateRoot, Entity, Repository, RepositoryError};
use thiserror::Error;
use tracing::{Instrument, info_span};

/// Error type for the in-memory repository implementation.
#[derive(Debug, Error)]
pub enum InMemoryRepoError {
    /// The in-memory store never fails on its own; this variant exists only to
    /// satisfy the `Repository::Error` bound.
    #[error("infallible")]
    Never,
}

/// Repository implementation backed by an in-memory concurrent map.
///
/// Enforces optimistic concurrency: each stored aggregate keeps its version and
/// a save only succeeds when the in-memory aggregate's expected version matches
/// the stored one.
pub struct InMemoryRepository<A: AggregateRoot + Clone> {
    store: Arc<DashMap<<A as Entity>::Id, A>>,
}

impl<A: AggregateRoot + Clone> Default for InMemoryRepository<A> {
    fn default() -> Self {
        Self {
            store: Arc::new(DashMap::new()),
        }
    }
}

impl<A: AggregateRoot + Clone> InMemoryRepository<A> {
    /// Creates an empty in-memory repository.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the number of stored aggregates.
    pub fn len(&self) -> usize {
        self.store.len()
    }

    /// Returns `true` when the repository has no stored aggregates.
    pub fn is_empty(&self) -> bool {
        self.store.is_empty()
    }
}

impl<A: AggregateRoot + Clone> Repository<A> for InMemoryRepository<A> {
    type Error = InMemoryRepoError;

    async fn find_by_id(&self, id: &<A as Entity>::Id) -> Result<Option<A>, Self::Error> {
        async move { Ok(self.store.get(id).map(|e| e.value().clone())) }
            .instrument(info_span!(
                "repository.find_by_id",
                repository = type_name::<Self>(),
                aggregate = type_name::<A>(),
            ))
            .await
    }

    async fn save(&self, aggregate: &mut A) -> Result<(), RepositoryError<Self::Error>> {
        async move {
            let expected = aggregate.version();
            let new_version = expected + 1;

            // The entry lock is held across the read-check-write, so the version
            // check and store are atomic with respect to other writers.
            match self.store.entry(aggregate.id().clone()) {
                Entry::Occupied(mut occupied) => {
                    let actual = occupied.get().version();
                    if actual != expected {
                        return Err(RepositoryError::ConcurrencyConflict {
                            expected,
                            actual: Some(actual),
                        });
                    }
                    aggregate.set_version(new_version);
                    occupied.insert(snapshot_without_pending_events(aggregate));
                }
                Entry::Vacant(vacant) => {
                    if expected != 0 {
                        return Err(RepositoryError::ConcurrencyConflict {
                            expected,
                            actual: None,
                        });
                    }
                    aggregate.set_version(new_version);
                    vacant.insert(snapshot_without_pending_events(aggregate));
                }
            }
            Ok(())
        }
        .instrument(info_span!(
            "repository.save",
            repository = type_name::<Self>(),
            aggregate = type_name::<A>(),
        ))
        .await
    }

    async fn delete(&self, id: &<A as Entity>::Id) -> Result<(), Self::Error> {
        async move {
            self.store.remove(id);
            Ok(())
        }
        .instrument(info_span!(
            "repository.delete",
            repository = type_name::<Self>(),
            aggregate = type_name::<A>(),
        ))
        .await
    }
}

/// Clones the aggregate for storage with its pending events dropped.
///
/// Persisted state must never round-trip pending events: they belong to the
/// current unit of work, and are published/enqueued (then drained) by the
/// caller after the save. Storing them would make a later `find_by_id` hand
/// back events that were already dispatched, duplicating them on the next
/// save. This mirrors the JSON adapters, where the events field is excluded
/// from the serialized payload via `#[serde(skip)]`.
fn snapshot_without_pending_events<A: AggregateRoot + Clone>(aggregate: &A) -> A {
    let mut stored = aggregate.clone();
    stored.drain_events();
    stored
}
