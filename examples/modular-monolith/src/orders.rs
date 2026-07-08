//! The `orders` bounded context.
//!
//! It owns the [`Order`] aggregate and knows nothing about billing. Other
//! contexts react to its [`OrderPlaced`] event through the in-process event bus.

use chrono::{DateTime, Utc};
use pharos_core::{AggregateEvents, AggregateRoot, DomainEvent, Entity};
use serde::{Deserialize, Serialize};

/// An order placed by a customer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Order {
    id: String,
    customer: String,
    total_cents: i64,
    #[serde(default)]
    version: u64,
    #[serde(skip)]
    events: AggregateEvents<OrderPlaced>,
}

impl Order {
    /// Places a new order, raising an [`OrderPlaced`] event.
    pub fn place(id: impl Into<String>, customer: impl Into<String>, total_cents: i64) -> Self {
        let id = id.into();
        let customer = customer.into();
        let mut events = AggregateEvents::default();
        events.raise(OrderPlaced {
            order_id: id.clone(),
            customer: customer.clone(),
            total_cents,
            occurred_at: Utc::now(),
        });
        Self {
            id,
            customer,
            total_cents,
            version: 0,
            events,
        }
    }

    /// Returns the customer name.
    pub fn customer(&self) -> &str {
        &self.customer
    }

    /// Returns the order total in cents.
    pub fn total_cents(&self) -> i64 {
        self.total_cents
    }
}

impl Entity for Order {
    type Id = String;

    fn id(&self) -> &Self::Id {
        &self.id
    }
}

impl AggregateRoot for Order {
    type Event = OrderPlaced;

    fn pending_events(&self) -> &[Self::Event] {
        self.events.pending()
    }

    fn drain_events(&mut self) -> Vec<Self::Event> {
        self.events.drain()
    }

    fn version(&self) -> u64 {
        self.version
    }

    fn set_version(&mut self, version: u64) {
        self.version = version;
    }
}

/// Raised when an order is placed.
#[derive(Debug, Clone)]
pub struct OrderPlaced {
    /// Order identifier.
    pub order_id: String,
    /// Customer who placed the order.
    pub customer: String,
    /// Order total in cents.
    pub total_cents: i64,
    /// When the order was placed.
    pub occurred_at: DateTime<Utc>,
}

impl DomainEvent for OrderPlaced {
    fn event_type(&self) -> &'static str {
        "OrderPlaced"
    }

    fn occurred_at(&self) -> DateTime<Utc> {
        self.occurred_at
    }

    fn aggregate_id(&self) -> &str {
        &self.order_id
    }
}
