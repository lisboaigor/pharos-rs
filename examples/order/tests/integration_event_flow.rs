use order::domain::events::OrderEvent;
use order::domain::order::Order;
use order::domain::value_objects::{CustomerId, Money, Quantity};
use pharos_app::{EventSerializer, IntegrationEvent, JsonEventSerializer};
use pharos_core::{AggregateRoot, DomainEvent};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct OrderConfirmedPayload {
    order_id: String,
    total_cents: u64,
}

fn confirmed_integration_event(
    event: &OrderEvent,
) -> Option<IntegrationEvent<OrderConfirmedPayload>> {
    match event {
        OrderEvent::OrderConfirmed {
            order_id,
            total_cents,
            ..
        } => Some(
            IntegrationEvent::from_domain_event(
                event,
                1,
                "order-example",
                OrderConfirmedPayload {
                    order_id: order_id.clone(),
                    total_cents: *total_cents,
                },
            )
            .with_correlation_id(format!("order-{order_id}"))
            .with_causation_id("confirm-order-command")
            .with_trace_id("trace-order-confirmed")
            .with_tenant_id("tenant-demo")
            .with_metadata("topic", "order-events"),
        ),
        _ => None,
    }
}

#[test]
fn maps_order_confirmed_domain_event_to_versioned_integration_event()
-> Result<(), Box<dyn std::error::Error>> {
    let mut order = Order::create(CustomerId::new())?;
    order.add_item(
        "Rust book".to_string(),
        Quantity::new(2)?,
        Money::from_cents(4_500),
    )?;
    order.confirm()?;

    let events = order.drain_events();
    let domain_event = events
        .iter()
        .find(|event| matches!(event, OrderEvent::OrderConfirmed { .. }))
        .ok_or("expected OrderConfirmed event")?;

    let integration_event = confirmed_integration_event(domain_event)
        .ok_or("expected integration event for OrderConfirmed")?;

    let aggregate_id = domain_event.aggregate_id();

    assert_eq!(integration_event.event_id.get_version_num(), 7);
    assert_eq!(integration_event.event_type, "OrderConfirmed");
    assert_eq!(integration_event.schema_version, 1);
    assert_eq!(integration_event.source, "order-example");
    assert_eq!(
        integration_event.aggregate_id.as_deref(),
        Some(aggregate_id)
    );
    assert_eq!(
        integration_event.causation_id.as_deref(),
        Some("confirm-order-command")
    );
    assert_eq!(integration_event.tenant_id.as_deref(), Some("tenant-demo"));
    assert_eq!(
        integration_event.metadata.get("topic").map(String::as_str),
        Some("order-events")
    );

    let serializer = JsonEventSerializer;
    let serialized = serializer.serialize(&integration_event)?;
    let roundtrip: IntegrationEvent<OrderConfirmedPayload> = serializer.deserialize(&serialized)?;

    assert_eq!(serialized.content_type, "application/json");
    assert_eq!(roundtrip, integration_event);

    Ok(())
}
