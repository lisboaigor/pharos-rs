use chrono::{DateTime, Utc};
use pharos::prelude::*;
use serde::{Deserialize, Serialize};

id_type!(OrderId);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderPlaced {
    order_id: String,
    occurred_at: DateTime<Utc>,
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

#[derive(Debug, Clone, Entity, AggregateRoot, Serialize, Deserialize)]
pub struct Order {
    #[id]
    id: OrderId,
    #[version]
    version: u64,
    #[events]
    #[serde(skip, default)]
    events: AggregateEvents<OrderPlaced>,
    customer_name: String,
}

impl Order {
    pub fn place(customer_name: impl Into<String>) -> Self {
        let id = OrderId::new();
        let mut events = AggregateEvents::default();
        events.raise(OrderPlaced {
            order_id: id.to_string(),
            occurred_at: Utc::now(),
        });
        Self {
            id,
            version: 0,
            events,
            customer_name: customer_name.into(),
        }
    }
}
