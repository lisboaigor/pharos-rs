use std::any::type_name;
use std::sync::Arc;

use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use pharos_app::{OutboxMessage, OutboxRepository, TransactionalRepository, TransactionalStore};
use pharos_core::{AggregateRoot, Entity, Repository, RepositoryError};
use thiserror::Error;
use tokio::sync::{Mutex, OwnedMutexGuard};
use tracing::{Instrument, info_span};

use crate::in_memory_outbox::InMemoryOutboxRepository;

/// Error type for [`InMemoryUnitOfWork`].
///
/// The in-memory store never fails on its own; this variant exists only to
/// satisfy the `Error` bounds on [`Repository`], [`TransactionalStore`], and
/// [`TransactionalRepository`].
#[derive(Debug, Error)]
pub enum InMemoryUnitOfWorkError {
    /// Placeholder — the in-memory backend has no failure modes of its own.
    #[error("infallible")]
    Never,
}

/// Staged writes for one [`InMemoryUnitOfWork`] transaction.
///
/// Nothing here is visible to [`Repository::find_by_id`] or
/// [`OutboxRepository::pending`] until [`TransactionalStore::commit`]
/// applies it — dropping the transaction without committing (the caller's
/// error path) leaves the aggregate map and the outbox untouched, exactly
/// like a rolled-back database transaction.
pub struct InMemoryTx<A> {
    // Held for the transaction's lifetime: begin() -> commit()/drop() is the
    // single-writer critical section, so the version check in `save_in_tx`
    // and the write in `commit` are atomic with respect to any other
    // transaction on this unit of work — the same guarantee a real
    // backend's row/transaction lock gives `save_and_enqueue_in`.
    _guard: OwnedMutexGuard<()>,
    staged_aggregate: Option<A>,
    staged_outbox: Vec<OutboxMessage>,
}

/// In-memory [`TransactionalStore`] + [`TransactionalRepository`] pair for
/// one aggregate type, backing [`pharos_app::DefaultAggregateStore`] with the
/// same atomic save-and-enqueue guarantee a real backend (e.g. a SeaORM
/// adapter) provides — for tests, examples, and local development.
///
/// Also implements [`Repository`], so the same value works for
/// [`DefaultAggregateStore::new`](pharos_app::DefaultAggregateStore::new)'s
/// in-process delivery mode, with no separate repository needed.
///
/// Share the outbox with an [`pharos_app::OutboxDispatcher`] by cloning the
/// `Arc<InMemoryOutboxRepository>` passed to [`Self::new`] — both then see
/// the same pending messages.
pub struct InMemoryUnitOfWork<A: AggregateRoot + Clone> {
    aggregates: Arc<DashMap<<A as Entity>::Id, A>>,
    outbox: Arc<InMemoryOutboxRepository>,
    lock: Arc<Mutex<()>>,
}

impl<A: AggregateRoot + Clone> Clone for InMemoryUnitOfWork<A> {
    fn clone(&self) -> Self {
        Self {
            aggregates: Arc::clone(&self.aggregates),
            outbox: Arc::clone(&self.outbox),
            lock: Arc::clone(&self.lock),
        }
    }
}

impl<A: AggregateRoot + Clone> InMemoryUnitOfWork<A> {
    /// Creates a unit of work with a fresh aggregate map, backed by
    /// `outbox`. Pass the same `Arc<InMemoryOutboxRepository>` to an
    /// [`pharos_app::OutboxDispatcher`] to have it drain what this unit of
    /// work enqueues.
    pub fn new(outbox: Arc<InMemoryOutboxRepository>) -> Self {
        Self {
            aggregates: Arc::new(DashMap::new()),
            outbox,
            lock: Arc::new(Mutex::new(())),
        }
    }

    /// The underlying outbox, for inspection in tests or for handing to a
    /// dispatcher.
    pub fn outbox(&self) -> &Arc<InMemoryOutboxRepository> {
        &self.outbox
    }

    /// Returns the number of stored aggregates.
    pub fn len(&self) -> usize {
        self.aggregates.len()
    }

    /// Returns `true` when the unit of work has no stored aggregates.
    pub fn is_empty(&self) -> bool {
        self.aggregates.is_empty()
    }
}

impl<A: AggregateRoot + Clone> Repository<A> for InMemoryUnitOfWork<A> {
    type Error = InMemoryUnitOfWorkError;

    async fn find_by_id(&self, id: &<A as Entity>::Id) -> Result<Option<A>, Self::Error> {
        async move { Ok(self.aggregates.get(id).map(|e| e.value().clone())) }
            .instrument(info_span!(
                "unit_of_work.find_by_id",
                repository = type_name::<Self>(),
                aggregate = type_name::<A>(),
            ))
            .await
    }

    async fn save(&self, aggregate: &mut A) -> Result<(), RepositoryError<Self::Error>> {
        async move {
            let expected = aggregate.version();
            let new_version = expected + 1;

            match self.aggregates.entry(aggregate.id().clone()) {
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
            "unit_of_work.save",
            repository = type_name::<Self>(),
            aggregate = type_name::<A>(),
        ))
        .await
    }

    async fn delete(&self, id: &<A as Entity>::Id) -> Result<(), Self::Error> {
        async move {
            self.aggregates.remove(id);
            Ok(())
        }
        .instrument(info_span!(
            "unit_of_work.delete",
            repository = type_name::<Self>(),
            aggregate = type_name::<A>(),
        ))
        .await
    }
}

impl<A: AggregateRoot + Clone + Send + Sync + 'static> TransactionalStore
    for InMemoryUnitOfWork<A>
{
    type Tx = InMemoryTx<A>;
    type Error = InMemoryUnitOfWorkError;

    async fn begin(&self) -> Result<Self::Tx, Self::Error> {
        let guard = Arc::clone(&self.lock).lock_owned().await;
        Ok(InMemoryTx {
            _guard: guard,
            staged_aggregate: None,
            staged_outbox: Vec::new(),
        })
    }

    async fn commit(&self, tx: Self::Tx) -> Result<(), Self::Error> {
        if let Some(aggregate) = tx.staged_aggregate {
            self.aggregates.insert(aggregate.id().clone(), aggregate);
        }
        for message in tx.staged_outbox {
            // `InMemoryOutboxRepository::insert` is infallible in practice
            // (see its `Error` type); a real error here would mean the
            // in-memory backend itself is broken, not the caller's data.
            self.outbox
                .insert(message)
                .await
                .map_err(|_| InMemoryUnitOfWorkError::Never)?;
        }
        Ok(())
    }

    async fn insert_outbox_in_tx<'a>(
        &'a self,
        tx: &'a mut Self::Tx,
        message: &'a OutboxMessage,
    ) -> Result<(), Self::Error> {
        tx.staged_outbox.push(message.clone());
        Ok(())
    }
}

impl<A: AggregateRoot + Clone + Send + Sync + 'static> TransactionalRepository<A, Self>
    for InMemoryUnitOfWork<A>
{
    type Error = InMemoryUnitOfWorkError;

    async fn save_in_tx<'c>(
        &'c self,
        tx: &'c mut InMemoryTx<A>,
        aggregate: &'c mut A,
    ) -> Result<(), RepositoryError<Self::Error>> {
        let expected = aggregate.version();
        let actual = self.aggregates.get(aggregate.id()).map(|e| e.version());

        match actual {
            Some(actual) if actual != expected => {
                return Err(RepositoryError::ConcurrencyConflict {
                    expected,
                    actual: Some(actual),
                });
            }
            None if expected != 0 => {
                return Err(RepositoryError::ConcurrencyConflict {
                    expected,
                    actual: None,
                });
            }
            _ => {}
        }

        aggregate.set_version(expected + 1);
        tx.staged_aggregate = Some(snapshot_without_pending_events(aggregate));
        Ok(())
    }
}

/// Clones the aggregate for storage with its pending events dropped — see
/// `in_memory_repository`'s function of the same name for why.
fn snapshot_without_pending_events<A: AggregateRoot + Clone>(aggregate: &A) -> A {
    let mut stored = aggregate.clone();
    stored.drain_events();
    stored
}
