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

    /// Puts previously drained events back at the front of the buffer.
    ///
    /// Used by `pharos_app::save_and_publish` when publishing fails partway
    /// through a batch: the events that were not published yet go back, ahead
    /// of anything raised since, so a retry republishes them in their original
    /// order and never republishes one that already reached its handlers.
    pub fn restore(&mut self, events: Vec<E>) {
        if events.is_empty() {
            return;
        }
        let raised_since = std::mem::replace(&mut self.pending, events);
        self.pending.extend(raised_since);
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

    /// Puts previously drained events back at the front of the pending buffer.
    ///
    /// This is the counterpart of [`drain_events`](Self::drain_events) that
    /// makes a failed publish recoverable: `pharos_app::save_and_publish`
    /// drains the batch it persisted, and if a handler rejects one of them it
    /// restores the ones that never got published so
    /// `pharos_app::republish_pending` can retry exactly those.
    ///
    /// Implementations must preserve order and put `events` *ahead* of
    /// anything raised since the drain.
    fn restore_events(&mut self, events: Vec<Self::Event>);

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
