use std::future::Future;

use thiserror::Error;

/// Error produced by unit-of-work implementations.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum UnitOfWorkError {
    /// Transaction begin failed.
    #[error("begin transaction failed: {0}")]
    Begin(String),
    /// Transaction commit failed.
    #[error("commit transaction failed: {0}")]
    Commit(String),
    /// Transaction rollback failed.
    #[error("rollback transaction failed: {0}")]
    Rollback(String),
    /// The transactional operation failed.
    #[error("transactional operation failed: {0}")]
    Operation(String),
}

/// Runs an application operation inside a transactional boundary.
///
/// This trait intentionally models the unit-of-work seam without dictating a
/// concrete database transaction type. Infrastructure crates can implement it
/// with SQL transactions, document sessions, message transactions, or no-op
/// behavior for tests.
pub trait UnitOfWork: Send + Sync + 'static {
    /// Runs `operation` inside a unit of work.
    fn run<T, F, Fut>(
        &self,
        operation: F,
    ) -> impl Future<Output = Result<T, UnitOfWorkError>> + Send
    where
        T: Send + 'static,
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, UnitOfWorkError>> + Send + 'static;
}

/// No-op unit of work useful for tests, in-memory adapters, and applications
/// that manage transactions elsewhere.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopUnitOfWork;

impl UnitOfWork for NoopUnitOfWork {
    async fn run<T, F, Fut>(&self, operation: F) -> Result<T, UnitOfWorkError>
    where
        T: Send + 'static,
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, UnitOfWorkError>> + Send + 'static,
    {
        operation().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn noop_unit_of_work_runs_operation() {
        let uow = NoopUnitOfWork;
        let result = uow.run(|| async { Ok(42) }).await.unwrap();
        assert_eq!(result, 42);
    }
}
