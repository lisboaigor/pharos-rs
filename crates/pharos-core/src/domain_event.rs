use chrono::{DateTime, Utc};

/// Represents an immutable fact that happened in the domain.
///
/// `DomainEvent` is a pure domain trait. It deliberately knows nothing about
/// dynamic dispatch, `Any`, or `TypeId`: the in-process event bus performs typed
/// dispatch internally without leaking infrastructure concerns into the domain.
pub trait DomainEvent: Send + Sync + 'static {
    /// Returns the logical event name used for routing and observability.
    fn event_type(&self) -> &'static str;
    /// Returns the timestamp when the event occurred.
    fn occurred_at(&self) -> DateTime<Utc>;
    /// Returns the aggregate identifier used for correlation.
    ///
    /// This borrows the identifier from the event instead of allocating a fresh
    /// `String` on every call, which matters on the event-publishing hot path.
    /// Implementers store the aggregate id as owned event state.
    fn aggregate_id(&self) -> &str;
}
