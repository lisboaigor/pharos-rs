use order::domain::events::OrderEvent;
use order::domain::order::Order;
use order::domain::value_objects::{CustomerId, Money, Quantity};
use pharos_app::serialization::SerializedEvent;
use pharos_app::{EventSerializer, IntegrationEvent, JsonEventSerializer, MessageCodec};
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

#[test]
fn protobuf_serializer_roundtrips_integration_event_envelope()
-> Result<(), Box<dyn std::error::Error>> {
    use pharos_proto::{APPLICATION_PROTOBUF, ProtobufEventSerializer};

    // Protobuf payload — mirrors OrderConfirmedPayload but derives prost::Message.
    // prost auto-derives Default and Debug; do not add them manually.
    #[derive(Clone, prost::Message)]
    struct OrderConfirmedProto {
        #[prost(string, tag = "1")]
        order_id: String,
        #[prost(uint64, tag = "2")]
        total_cents: u64,
    }

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
        .find(|e| matches!(e, OrderEvent::OrderConfirmed { .. }))
        .ok_or("expected OrderConfirmed event")?;

    let (order_id_str, total_cents) = match domain_event {
        OrderEvent::OrderConfirmed {
            order_id,
            total_cents,
            ..
        } => (order_id.clone(), *total_cents),
        _ => unreachable!(),
    };

    let proto_payload = OrderConfirmedProto {
        order_id: order_id_str.clone(),
        total_cents,
    };

    let integration_event =
        IntegrationEvent::from_domain_event(domain_event, 1, "order-example", proto_payload)
            .with_correlation_id(format!("order-{order_id_str}"))
            .with_causation_id("confirm-order-command")
            .with_metadata("topic", "order-events");

    let serializer = ProtobufEventSerializer;
    let wire = serializer.serialize(&integration_event)?;

    assert_eq!(wire.content_type, APPLICATION_PROTOBUF);

    let recovered: IntegrationEvent<OrderConfirmedProto> = serializer.deserialize(&wire)?;

    assert_eq!(recovered.event_type, "OrderConfirmed");
    assert_eq!(recovered.schema_version, 1);
    assert_eq!(recovered.source, "order-example");
    assert_eq!(
        recovered.aggregate_id.as_deref(),
        Some(domain_event.aggregate_id())
    );
    assert_eq!(
        recovered.causation_id.as_deref(),
        Some("confirm-order-command")
    );
    assert_eq!(
        recovered.metadata.get("topic").map(String::as_str),
        Some("order-events")
    );
    assert_eq!(recovered.payload.order_id, order_id_str);
    assert_eq!(recovered.payload.total_cents, total_cents);

    // Protobuf should be smaller than JSON for the same logical data.
    let json_payload = OrderConfirmedPayload {
        order_id: order_id_str.clone(),
        total_cents,
    };
    let json_event =
        IntegrationEvent::from_domain_event(domain_event, 1, "order-example", json_payload);
    let json_wire = JsonEventSerializer.serialize(&json_event)?;

    assert!(
        wire.payload.len() < json_wire.payload.len(),
        "proto ({} B) should be smaller than json ({} B)",
        wire.payload.len(),
        json_wire.payload.len(),
    );

    Ok(())
}

/// Infrastructure code that is agnostic about wire format: it accepts any
/// `MessageCodec<P>` and returns the encoded bytes ready for a broker.
fn encode_for_dispatch<P, C: MessageCodec<P>>(
    codec: &C,
    event: &IntegrationEvent<P>,
) -> Result<SerializedEvent, C::Error> {
    codec.encode(event)
}

#[test]
fn message_codec_is_format_agnostic() -> Result<(), Box<dyn std::error::Error>> {
    use pharos_proto::{APPLICATION_PROTOBUF, ProtobufEventSerializer};

    // JSON payload — standard serde path.
    #[derive(Clone, Serialize, Deserialize)]
    struct OrderConfirmedJson {
        order_id: String,
        total_cents: u64,
    }

    // Protobuf payload — prost path. prost derives Default and Debug automatically.
    #[derive(Clone, prost::Message)]
    struct OrderConfirmedProto {
        #[prost(string, tag = "1")]
        pub order_id: String,
        #[prost(uint64, tag = "2")]
        pub total_cents: u64,
    }

    let json_event = IntegrationEvent::new(
        "OrderConfirmed",
        1,
        "orders",
        OrderConfirmedJson {
            order_id: "ord-1".into(),
            total_cents: 9_900,
        },
    )
    .with_correlation_id("corr-1");

    let proto_event = IntegrationEvent::new(
        "OrderConfirmed",
        1,
        "orders",
        OrderConfirmedProto {
            order_id: "ord-1".into(),
            total_cents: 9_900,
        },
    )
    .with_correlation_id("corr-1");

    // The same generic helper works for both codecs.
    let json_wire = encode_for_dispatch(&JsonEventSerializer, &json_event)?;
    let proto_wire = encode_for_dispatch(&ProtobufEventSerializer, &proto_event)?;

    assert_eq!(json_wire.content_type, "application/json");
    assert_eq!(proto_wire.content_type, APPLICATION_PROTOBUF);

    // Both decode back to equivalent envelope metadata.
    let json_decoded: IntegrationEvent<OrderConfirmedJson> =
        JsonEventSerializer.decode(&json_wire)?;
    let proto_decoded: IntegrationEvent<OrderConfirmedProto> =
        ProtobufEventSerializer.decode(&proto_wire)?;

    assert_eq!(json_decoded.event_type, "OrderConfirmed");
    assert_eq!(proto_decoded.event_type, "OrderConfirmed");
    assert_eq!(
        json_decoded.payload.order_id,
        proto_decoded.payload.order_id
    );
    assert_eq!(
        json_decoded.payload.total_cents,
        proto_decoded.payload.total_cents
    );
    assert_eq!(json_decoded.correlation_id, proto_decoded.correlation_id);

    Ok(())
}
