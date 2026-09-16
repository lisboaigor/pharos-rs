//! Canonical fixture types the repository- and event-sourcing-shaped
//! contract suites run against.
//!
//! Most adapters (a `Repository<A>` or `EventStore<I, E>` implementation)
//! are generic over the domain type they store, so instantiating them
//! against [`ContractAggregate`]/[`ContractEvent`] instead of a real
//! application's own types is usually free — the suites in
//! [`crate::contract`] never need to know anything about your domain.
use chrono::{DateTime, Utc};
use pharos_core::{AggregateEvents, AggregateRoot, DomainEvent, Entity};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// The one event [`ContractAggregate`] ever raises.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContractEvent {
    /// The aggregate that raised this event, as a string (see
    /// [`DomainEvent::aggregate_id`]).
    pub aggregate_id: String,
    /// When the event occurred.
    pub occurred_at: DateTime<Utc>,
    /// The label [`ContractAggregate`] carried when this event was raised.
    pub label: String,
}

impl DomainEvent for ContractEvent {
    fn event_type(&self) -> &'static str {
        "ContractEvent"
    }

    fn occurred_at(&self) -> DateTime<Utc> {
        self.occurred_at
    }

    fn aggregate_id(&self) -> &str {
        &self.aggregate_id
    }
}

/// A minimal, fully-featured aggregate: an id, a version, one mutable
/// field, and the event it raises when that field changes.
///
/// Implement `Repository<ContractAggregate>` (and, for the transactional
/// suites, `TransactionalRepository<ContractAggregate, YourStore>`) against
/// your adapter to run [`crate::contract::repository`] and
/// [`crate::contract::transactional_store`] against it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContractAggregate {
    id: Uuid,
    version: u64,
    label: String,
    #[serde(skip)]
    events: AggregateEvents<ContractEvent>,
}

impl ContractAggregate {
    /// Creates a new, unsaved aggregate (version `0`) with one pending
    /// event — exactly what a real aggregate looks like right after its
    /// constructor runs, before the first [`Repository::save`].
    ///
    /// [`Repository::save`]: pharos_core::Repository::save
    pub fn new(id: Uuid, label: impl Into<String>) -> Self {
        let label = label.into();
        let mut events = AggregateEvents::default();
        events.raise(ContractEvent {
            aggregate_id: id.to_string(),
            occurred_at: Utc::now(),
            label: label.clone(),
        });
        Self {
            id,
            version: 0,
            label,
            events,
        }
    }

    /// Changes the label and raises a second [`ContractEvent`] — the
    /// "load, mutate, save again" step contract suites use to exercise a
    /// second round trip.
    pub fn relabel(&mut self, label: impl Into<String>) {
        self.label = label.into();
        self.events.raise(ContractEvent {
            aggregate_id: self.id.to_string(),
            occurred_at: Utc::now(),
            label: self.label.clone(),
        });
    }

    /// The current label.
    pub fn label(&self) -> &str {
        &self.label
    }
}

impl Entity for ContractAggregate {
    type Id = Uuid;

    fn id(&self) -> &Uuid {
        &self.id
    }
}

impl AggregateRoot for ContractAggregate {
    type Event = ContractEvent;

    fn pending_events(&self) -> &[ContractEvent] {
        self.events.pending()
    }

    fn drain_events(&mut self) -> Vec<ContractEvent> {
        self.events.drain()
    }

    fn restore_events(&mut self, events: Vec<ContractEvent>) {
        self.events.restore(events);
    }

    fn version(&self) -> u64 {
        self.version
    }

    fn set_version(&mut self, version: u64) {
        self.version = version;
    }
}
