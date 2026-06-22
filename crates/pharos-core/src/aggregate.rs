use crate::domain_event::DomainEvent;
use crate::entity::Entity;

/// Stores the pending domain events raised by an aggregate.
#[derive(Debug, Clone)]
pub struct AggregateEvents<E: DomainEvent> {
    pending: Vec<E>,
}

impl<E: DomainEvent> Default for AggregateEvents<E> {
    fn default() -> Self {
        Self {
            pending: Vec::new(),
        }
    }
}

impl<E: DomainEvent> AggregateEvents<E> {
    /// Adds a new event to the pending event list.
    pub fn raise(&mut self, event: E) {
        self.pending.push(event);
    }

    /// Returns all currently pending events without removing them.
    pub fn pending(&self) -> &[E] {
        &self.pending
    }

    /// Removes and returns all pending events.
    pub fn drain(&mut self) -> Vec<E> {
        std::mem::take(&mut self.pending)
    }
}

/// Represents an aggregate root that can raise domain events.
///
/// Aggregate roots are the transactional consistency boundary of a domain
/// model. They carry an optimistic-concurrency [`version`](AggregateRoot::version)
/// so repositories can detect and reject lost updates.
pub trait AggregateRoot: Entity {
    /// The concrete domain event type emitted by this aggregate.
    type Event: DomainEvent;

    /// Returns the events currently waiting to be published.
    fn pending_events(&self) -> &[Self::Event];
    /// Removes and returns all pending events.
    fn drain_events(&mut self) -> Vec<Self::Event>;

    /// Returns the optimistic-concurrency version of the aggregate.
    ///
    /// A freshly created aggregate starts at version `0`. A repository
    /// increments the version on every successful save and rejects writes whose
    /// expected version no longer matches the stored state.
    fn version(&self) -> u64;

    /// Sets the optimistic-concurrency version.
    ///
    /// Repositories call this after a successful save to reflect the newly
    /// persisted version on the in-memory aggregate.
    fn set_version(&mut self, version: u64);
}
