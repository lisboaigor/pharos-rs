use thiserror::Error;

/// Error produced by unit-of-work implementations.
///
/// Every variant keeps the originating error as a typed `source`, so callers
/// can walk the chain down to the adapter failure (e.g. a `sqlx::Error`)
/// instead of matching on strings.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum UnitOfWorkError {
    /// Transaction begin failed.
    #[error("begin transaction failed: {0}")]
    Begin(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),
    /// Transaction commit failed.
    #[error("commit transaction failed: {0}")]
    Commit(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),
    /// Transaction rollback failed.
    #[error("rollback transaction failed: {0}")]
    Rollback(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),
    /// The transactional operation failed.
    #[error("transactional operation failed: {0}")]
    Operation(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),
}

impl UnitOfWorkError {
    /// Wraps an error as a begin failure.
    pub fn begin(e: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::Begin(Box::new(e))
    }
    /// Wraps an error as a commit failure.
    pub fn commit(e: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::Commit(Box::new(e))
    }
    /// Wraps an error as a rollback failure.
    pub fn rollback(e: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::Rollback(Box::new(e))
    }
    /// Wraps an error as an operation failure.
    pub fn operation(e: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::Operation(Box::new(e))
    }
}

// There is deliberately no `UnitOfWork` trait here. A generic trait whose
// closure cannot borrow the live transaction handle would prevent repositories
// from participating in the transaction — an abstraction that looks like a
// unit of work but cannot compose one. Transactional composition is inherently
// adapter-specific, so each infrastructure crate exposes its own concrete
// seam (e.g. `PostgresUnitOfWork::transaction`, which hands the closure a
// `&mut PgConnection` that every query can share). Only the error vocabulary
// is shared, so application code can report transaction failures uniformly.
