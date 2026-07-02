use order::domain::events::OrderEvent;
use order::domain::order::Order;
use order::domain::value_objects::{CustomerId, Money, Quantity};
use pharos_app::{
    EventSerializer, IntegrationEvent, JsonEventSerializer, Message, OutboxRepository,
    OutboxStatus, save_and_enqueue,
};
use pharos_core::{AggregateRoot, DomainEvent, Entity, Repository};
use pharos_infra::{InMemoryOutboxRepository, InMemoryRepository};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind")]
enum OrderIntegrationPayload {
    OrderCreated {
        order_id: String,
        customer_id: Uuid,
    },
    ItemAdded {
        order_id: String,
        item_id: Uuid,
        description: String,
        quantity: u32,
        unit_price_cents: u64,
    },
    OrderConfirmed {
        order_id: String,
        total_cents: u64,
    },
    OrderCancelled {
        order_id: String,
        reason: String,
    },
}

fn payload_from(event: &OrderEvent) -> OrderIntegrationPayload {
    match event {
        OrderEvent::OrderCreated {
            order_id,
            customer_id,
            ..
        } => OrderIntegrationPayload::OrderCreated {
            order_id: order_id.clone(),
            customer_id: *customer_id,
        },
        OrderEvent::ItemAdded {
            order_id,
            item_id,
            description,
            quantity,
            unit_price_cents,
            ..
        } => OrderIntegrationPayload::ItemAdded {
            order_id: order_id.clone(),
            item_id: *item_id,
            description: description.clone(),
            quantity: *quantity,
            unit_price_cents: *unit_price_cents,
        },
        OrderEvent::OrderConfirmed {
            order_id,
            total_cents,
            ..
        } => OrderIntegrationPayload::OrderConfirmed {
            order_id: order_id.clone(),
            total_cents: *total_cents,
        },
        OrderEvent::OrderCancelled {
            order_id, reason, ..
        } => OrderIntegrationPayload::OrderCancelled {
            order_id: order_id.clone(),
            reason: reason.clone(),
        },
    }
}

fn order_event_to_message(event: &OrderEvent) -> Message {
    let integration_event =
        IntegrationEvent::from_domain_event(event, 1, "order-example", payload_from(event))
            .with_correlation_id(event.aggregate_id())
            .with_metadata("bounded_context", "ordering");

    let Ok(serialized) = JsonEventSerializer.serialize(&integration_event) else {
        panic!("order integration events should serialize to JSON");
    };

    Message::new("order-events", serialized.payload, serialized.content_type)
        .with_key(event.aggregate_id())
        .with_header("event_type", event.event_type())
        .with_header(
            "schema_version",
            integration_event.schema_version.to_string(),
        )
        .with_header(
            "correlation_id",
            integration_event.correlation_id.unwrap_or_default(),
        )
}

#[tokio::test]
async fn save_and_enqueue_persists_order_and_writes_pending_events_to_outbox()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let repo = InMemoryRepository::<Order>::new();
    let outbox = InMemoryOutboxRepository::new();
    let mut order = Order::create(CustomerId::new())?;
    let order_id = *order.id();

    order.add_item(
        "Rust book".to_string(),
        Quantity::new(1)?,
        Money::from_cents(4_500),
    )?;
    order.confirm()?;

    save_and_enqueue(&repo, &outbox, &mut order, order_event_to_message).await?;

    assert!(order.pending_events().is_empty());
    assert!(repo.find_by_id(&order_id).await?.is_some());

    let pending = outbox.pending(10).await?;
    assert_eq!(pending.len(), 3);
    assert!(
        pending
            .iter()
            .all(|message| message.status == OutboxStatus::Pending)
    );
    assert!(pending.iter().any(|message| {
        message
            .message
            .headers
            .get("event_type")
            .map(String::as_str)
            == Some("OrderConfirmed")
    }));
    assert!(
        pending
            .iter()
            .all(|message| message.message.topic == "order-events"
                && message.message.key.as_deref() == Some(&order_id.as_uuid().to_string()))
    );

    Ok(())
}
