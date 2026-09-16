use std::error::Error;

use pharos_core::{AggregateRoot, ClassifiedError, ErrorKind, RepositoryError};

/// Error returned by an [`AggregateStore`].
///
/// This is the store-level error a `CommandHandler` sees: it collapses
/// whatever a concrete store implementation does internally (a plain
/// repository save, or a transactional save-and-enqueue against an outbox)
/// into one small, uniform shape. Application code that maps errors to a
/// transport (e.g. HTTP 404/409/500) matches on this instead of on a
/// backend-specific error type.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum StoreError {
    /// No aggregate exists for the given id.
    #[error("aggregate not found")]
    NotFound,
    /// A concurrent write raced this one; the caller should reload and retry.
    #[error("optimistic concurrency conflict: expected version {expected}, found {actual:?}")]
    ConcurrencyConflict { expected: u64, actual: Option<u64> },
    /// Any other failure: a database error, a serialization error, an
    /// outbox-insert failure. The original error is preserved as a typed
    /// `source`.
    #[error("aggregate store failed: {0}")]
    Storage(#[source] Box<dyn Error + Send + Sync + 'static>),
}

impl StoreError {
    /// Wraps any `Error + Send + Sync + 'static` as a storage failure.
    pub fn storage(e: impl Error + Send + Sync + 'static) -> Self {
        Self::Storage(Box::new(e))
    }
}

impl ClassifiedError for StoreError {
    fn kind(&self) -> ErrorKind {
        match self {
            StoreError::NotFound => ErrorKind::NotFound,
            StoreError::ConcurrencyConflict { .. } => ErrorKind::Conflict,
            StoreError::Storage(_) => ErrorKind::Internal,
        }
    }

    fn public_message(&self) -> String {
        match self {
            StoreError::NotFound | StoreError::ConcurrencyConflict { .. } => self.to_string(),
            // `Storage` boxes whatever the concrete backend produced — a
            // `sqlx::Error`, a serialization failure, an outbox-insert
            // failure. None of that is safe to hand to a caller; it stays
            // reachable only through `source()` for logging.
            _ => "internal storage error".to_string(),
        }
    }
}

impl From<crate::ApplicationError> for StoreError {
    fn from(error: crate::ApplicationError) -> Self {
        match error {
            crate::ApplicationError::ConcurrencyConflict { expected, actual } => {
                StoreError::ConcurrencyConflict { expected, actual }
            }
            other => StoreError::Storage(other.to_string().into()),
        }
    }
}

impl<E> From<RepositoryError<E>> for StoreError
where
    E: Error + Send + Sync + 'static,
{
    fn from(error: RepositoryError<E>) -> Self {
        match error {
            RepositoryError::ConcurrencyConflict { expected, actual } => {
                StoreError::ConcurrencyConflict { expected, actual }
            }
            RepositoryError::Storage(e) => StoreError::storage(e),
            // `RepositoryError` is non_exhaustive; treat unknown future
            // variants as storage failures so the caller still gets the full
            // error text instead of a match that silently fails to compile
            // and forces every downstream crate to add its own wildcard.
            other => StoreError::Storage(other.to_string().into()),
        }
    }
}

/// Loads and persists a single aggregate, hiding how — and whether — its
/// domain events get delivered.
///
/// This is the seam a `CommandHandler` depends on instead of composing
/// `Repository::find_by_id` + a save-and-publish/save-and-enqueue helper by
/// hand: a command handler only ever needs "load this aggregate, mutate it,
/// save it back", and a concrete `AggregateStore` implementation decides
/// whether that save publishes in-process ([`crate::save_and_publish`]) or
/// enqueues to a durable outbox ([`crate::save_and_enqueue_in`]), and how the
/// outgoing messages are enriched (tenant, trace headers). Swapping delivery
/// mode is then a change to how the store is constructed, not to every
/// handler that uses it.
///
/// Uses native `async fn` rather than the `-> impl Future<..> + Send` form
/// most other traits in this workspace use (see [`crate::CommandHandler`],
/// [`crate::EventHandler`], [`crate::unit_of_work::TransactionalStore`]):
/// those are invoked through code that is itself generic over the trait and
/// then boxed as `Send` (a `tower::Service::call` returning `Pin<Box<dyn
/// Future + Send>>`, `EventBus`'s type-erased dispatch) — a call chain a
/// bare `async fn`, whose associated future carries no `Send` bound, cannot
/// satisfy for an arbitrary implementor. `AggregateStore` is never used that
/// way: every call site holds a concrete implementor (typically resolved
/// through a default type parameter, e.g. `XHandlers<R: XRepository =
/// PostgresXRepository>`), so `Send` is proven trivially at the concrete
/// type rather than needing to hold generically.
#[allow(async_fn_in_trait)]
pub trait AggregateStore<A: AggregateRoot>: Send + Sync {
    /// Loads the aggregate by id, or `None` if it does not exist.
    async fn find(&self, id: &A::Id) -> Result<Option<A>, StoreError>;

    /// Loads the aggregate by id, or [`StoreError::NotFound`] if it does not
    /// exist.
    async fn load(&self, id: &A::Id) -> Result<A, StoreError> {
        self.find(id).await?.ok_or(StoreError::NotFound)
    }

    /// Persists the aggregate and delivers its pending domain events.
    ///
    /// On success the aggregate's pending events are drained and its version
    /// reflects the newly persisted state; on failure both are left intact so
    /// a retry starts clean (see the contract on [`crate::save_and_publish`]
    /// and [`crate::save_and_enqueue_in`], which concrete stores build on).
    async fn save(&self, aggregate: &mut A) -> Result<(), StoreError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, thiserror::Error)]
    #[error("connection to postgres://prod-db.internal:5432 refused")]
    struct SensitiveAdapterError;

    #[test]
    fn not_found_and_conflict_classify_with_their_own_message() {
        assert_eq!(StoreError::NotFound.kind(), ErrorKind::NotFound);
        assert_eq!(StoreError::NotFound.public_message(), "aggregate not found");

        let conflict = StoreError::ConcurrencyConflict {
            expected: 2,
            actual: Some(3),
        };
        assert_eq!(conflict.kind(), ErrorKind::Conflict);
        assert!(conflict.public_message().contains("expected version 2"));
    }

    #[test]
    fn storage_failure_never_leaks_the_wrapped_adapter_error() {
        let error = StoreError::storage(SensitiveAdapterError);
        assert_eq!(error.kind(), ErrorKind::Internal);
        assert!(!error.public_message().contains("postgres://"));
        assert!(!error.public_message().contains("5432"));
        assert_eq!(error.public_message(), "internal storage error");
    }
}
