use std::error::Error;

use crate::aggregate::AggregateRoot;
use crate::classify::{ClassifiedError, ErrorKind};

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

impl<E: Error + Send + Sync + 'static> ClassifiedError for RepositoryError<E> {
    fn kind(&self) -> ErrorKind {
        match self {
            RepositoryError::ConcurrencyConflict { .. } => ErrorKind::Conflict,
            RepositoryError::Storage(_) => ErrorKind::Internal,
        }
    }

    fn public_message(&self) -> String {
        match self {
            RepositoryError::ConcurrencyConflict { .. } => self.to_string(),
            // `E` is deliberately not required to implement `ClassifiedError`
            // here: an adapter's storage error (a `sqlx::Error`, a driver
            // timeout) is exactly the kind of detail this trait exists to
            // stop at the boundary. The fixed string is the whole point —
            // see `ClassifiedError::public_message`'s contract on `Internal`.
            RepositoryError::Storage(_) => "internal storage error".to_string(),
        }
    }
}

/// Persists and retrieves aggregate roots.
///
/// `save` takes `&mut` because a successful write advances the aggregate's
/// optimistic-concurrency version, which the repository writes back onto the
/// in-memory instance.
///
/// `#[trait_variant::make(Send)]` rewrites the `async fn`s below to add
/// `+ Send` to their returned futures — the trait's public shape is
/// unchanged (same name, same methods), this only spares implementors (and
/// this file) the hand-written `-> impl Future<Output = T> + Send` form.
/// `Send` is required because `Repository<A>` is consumed by code generic
/// over the trait itself (`DefaultAggregateStore`, `save_and_enqueue_in`,
/// `tower::Service`-backed handlers) — see `pharos_app::AggregateStore`'s
/// doc comment for the one trait in this workspace that deliberately opts
/// out of this and why.
#[trait_variant::make(Send)]
pub trait Repository<A: AggregateRoot>: Sync + 'static {
    /// The repository-specific storage error type.
    type Error: Error + Send + Sync + 'static;

    /// Finds an aggregate by its identifier.
    async fn find_by_id(&self, id: &A::Id) -> Result<Option<A>, Self::Error>;

    /// Saves the current aggregate state, enforcing optimistic concurrency.
    ///
    /// On success the aggregate's [`version`](AggregateRoot::version) is advanced
    /// to the newly persisted value. On a version mismatch this returns
    /// [`RepositoryError::ConcurrencyConflict`] without mutating storage.
    async fn save(&self, aggregate: &mut A) -> Result<(), RepositoryError<Self::Error>>;

    /// Deletes an aggregate by identifier.
    async fn delete(&self, id: &A::Id) -> Result<(), Self::Error>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, thiserror::Error)]
    #[error("connection to postgres://prod-db.internal:5432 refused")]
    struct SensitiveAdapterError;

    #[test]
    fn concurrency_conflict_classifies_as_conflict_with_its_own_message() {
        let error: RepositoryError<SensitiveAdapterError> = RepositoryError::ConcurrencyConflict {
            expected: 3,
            actual: Some(4),
        };
        assert_eq!(error.kind(), ErrorKind::Conflict);
        assert!(error.public_message().contains("expected version 3"));
    }

    #[test]
    fn storage_failure_never_leaks_the_adapter_error_into_the_public_message() {
        let error = RepositoryError::Storage(SensitiveAdapterError);
        assert_eq!(error.kind(), ErrorKind::Internal);
        // The whole point of the boundary: whatever the adapter's error says
        // (here, a connection string) must never reach `public_message`,
        // even though it's right there in `Display`/`source()` for logging.
        assert!(!error.public_message().contains("postgres://"));
        assert!(!error.public_message().contains("5432"));
        assert_eq!(error.public_message(), "internal storage error");
    }
}
