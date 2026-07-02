use std::error::Error;
use std::future::Future;

use tracing::{Instrument, Span, info_span};

/// Marker trait for application queries.
pub trait Query: Send + Sync + 'static {
    /// Result type returned by the query.
    type Result: Send + Sync + 'static;

    /// Stable label for this query in traces and metrics.
    ///
    /// Independent of any wire or serialization name, so spans stay meaningful
    /// even if the type is renamed. [`dispatch`] uses it to populate the
    /// `query.handle` span.
    const NAME: &'static str;

    /// Builds the tracing span under which this query is handled.
    ///
    /// The default span carries only `query = Self::NAME`. Override it to record
    /// query-specific fields (ids, filters, …): they describe the query's own
    /// data, so they live on the DTO and keep handlers free of any tracing
    /// concern.
    ///
    /// ```ignore
    /// fn trace_span(&self) -> tracing::Span {
    ///     tracing::info_span!(
    ///         "query.handle",
    ///         query = Self::NAME,
    ///         order_id = %self.order_id,
    ///     )
    /// }
    /// ```
    fn trace_span(&self) -> Span {
        info_span!("query.handle", query = Self::NAME)
    }
}

/// Handles a query and returns the requested read model.
pub trait QueryHandler<Q: Query>: Send + Sync + 'static {
    /// Concrete error type returned by the handler.
    type Error: Error + Send + Sync + 'static;

    /// Executes the query.
    ///
    /// Implementations should contain read logic only; tracing is applied by
    /// [`dispatch`]. Prefer dispatching over calling this directly.
    fn handle(&self, query: Q) -> impl Future<Output = Result<Q::Result, Self::Error>> + Send;
}

/// Dispatches a query to its handler inside the query's tracing span.
///
/// This is the read-side counterpart to [`crate::command::dispatch`]: handlers
/// stay pure read logic while the `query.handle` span — and its fields, via
/// [`Query::trace_span`] — is applied here, consistently and impossible to
/// forget. Prefer this over calling [`QueryHandler::handle`] directly.
pub async fn dispatch<Q, H>(handler: &H, query: Q) -> Result<Q::Result, H::Error>
where
    Q: Query,
    H: QueryHandler<Q>,
{
    let span = query.trace_span();
    handler.handle(query).instrument(span).await
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Double(u32);
    impl Query for Double {
        type Result = u32;
        const NAME: &'static str = "Double";
    }

    struct DoubleHandler;
    impl QueryHandler<Double> for DoubleHandler {
        type Error = std::convert::Infallible;

        async fn handle(&self, q: Double) -> Result<u32, Self::Error> {
            Ok(q.0 * 2)
        }
    }

    #[tokio::test]
    async fn dispatch_runs_handler_and_returns_result() {
        let out = dispatch(&DoubleHandler, Double(21)).await.unwrap();
        assert_eq!(out, 42);
    }

    #[test]
    fn name_is_exposed_as_a_stable_label() {
        assert_eq!(Double::NAME, "Double");
    }

    // The span name ("query.handle") and its field contents are asserted
    // end-to-end in the `order` example's instrumentation test, where a real
    // capturing subscriber is installed.
}
