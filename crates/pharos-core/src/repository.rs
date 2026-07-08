use std::error::Error;

use crate::aggregate::AggregateRoot;

/// Error returned by [`Repository::save`].
///
/// Saving an aggregate can fail either because of an optimistic-concurrency
/// conflict (another writer persisted a newer version of the same aggregate) or
/// because of an adapter-specific storage failure.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum RepositoryError<E: Error> {
    /// The aggregate could not be saved because its expected version no longer
    /// matches the stored version. The caller should reload and retry.
    #[error("optimistic concurrency conflict: expected version {expected}, found {actual:?}")]
    ConcurrencyConflict {
        /// Version the in-memory aggregate was loaded at.
        expected: u64,
        /// Version currently stored, when known.
        actual: Option<u64>,
    },
    /// Adapter-specific storage failure.
    #[error(transparent)]
    Storage(E),
}

/// Persists and retrieves aggregate roots.
///
/// `save` takes `&mut` because a successful write advances the aggregate's
/// optimistic-concurrency version, which the repository writes back onto the
/// in-memory instance.
pub trait Repository<A: AggregateRoot>: Send + Sync + 'static {
    /// The repository-specific storage error type.
    type Error: Error + Send + Sync + 'static;

    /// Finds an aggregate by its identifier.
    fn find_by_id(&self, id: &A::Id)
    -> impl Future<Output = Result<Option<A>, Self::Error>> + Send;

    /// Saves the current aggregate state, enforcing optimistic concurrency.
    ///
    /// On success the aggregate's [`version`](AggregateRoot::version) is advanced
    /// to the newly persisted value. On a version mismatch this returns
    /// [`RepositoryError::ConcurrencyConflict`] without mutating storage.
    fn save(
        &self,
        aggregate: &mut A,
    ) -> impl Future<Output = Result<(), RepositoryError<Self::Error>>> + Send;

    /// Deletes an aggregate by identifier.
    fn delete(&self, id: &A::Id) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
