use std::error::Error;
use std::future::Future;

/// Marker trait for application queries.
pub trait Query: Send + Sync + 'static {
    /// Result type returned by the query.
    type Result: Send + Sync + 'static;
}

/// Handles a query and returns the requested read model.
pub trait QueryHandler<Q: Query>: Send + Sync + 'static {
    /// Concrete error type returned by the handler.
    type Error: Error + Send + Sync + 'static;

    /// Executes the query.
    fn handle(&self, query: Q) -> impl Future<Output = Result<Q::Result, Self::Error>> + Send;
}
