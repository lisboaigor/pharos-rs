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

    /// Version of this event type's payload shape.
    ///
    /// Defaults to `0` — a type whose shape never changes can ignore this
    /// entirely. A type that *does* evolve should bump it whenever a field
    /// is added, renamed, or reinterpreted, and pair it with a store-side
    /// upcaster (e.g. `pharos-postgres`'s `EventUpcasterRegistry`) keyed on
    /// this value together with [`Self::event_type`]. Without it, a durable
    /// event store has no way to tell which shape an already-written
    /// payload was serialized under except by inferring it from the JSON's
    /// own structure — fragile, and exactly what this exists to replace.
    fn schema_version(&self) -> u32 {
        0
    }
}
