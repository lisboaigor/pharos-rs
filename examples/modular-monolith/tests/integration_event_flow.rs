//! Shows how the OrderPlaced domain event is mapped to an IntegrationEvent and
//! serialized for cross-process delivery using either wire format.
//!
//! In a modular monolith the EventBus handles in-process fan-out. When a second
//! deployable process needs to consume the same events, they are encoded into a
//! SerializedEvent via MessageCodec and written to the outbox. This test
//! demonstrates that the encoding step is format-agnostic.

use chrono::Utc;
use modular_monolith::orders::OrderPlaced;
use pharos_app::serialization::SerializedEvent;
use pharos_app::{IntegrationEvent, JsonEventSerializer, MessageCodec};
use pharos_proto::{APPLICATION_PROTOBUF, ProtobufEventSerializer};
use serde::{Deserialize, Serialize};

/// Simulates an outbox helper generic over wire format.
fn encode_for_outbox<P, C: MessageCodec<P>>(
    codec: &C,
    event: &IntegrationEvent<P>,
) -> Result<SerializedEvent, C::Error> {
    codec.encode(event)
}

fn sample_domain_event() -> OrderPlaced {
    OrderPlaced {
        order_id: "ord-42".into(),
        customer: "Ada Lovelace".into(),
        total_cents: 12_000,
        occurred_at: Utc::now(),
    }
}

// JSON integration event payload.
#[derive(Clone, Serialize, Deserialize)]
struct OrderPlacedJson {
    order_id: String,
    customer: String,
    total_cents: i64,
}

// Protobuf integration event payload — prost derives Default and Debug.
#[derive(Clone, prost::Message)]
struct OrderPlacedProto {
    #[prost(string, tag = "1")]
    pub order_id: String,
    #[prost(string, tag = "2")]
    pub customer: String,
    #[prost(int64, tag = "3")]
    pub total_cents: i64,
}

#[test]
fn order_placed_encodes_as_json_integration_event() -> Result<(), Box<dyn std::error::Error>> {
    let ev = sample_domain_event();
    let payload = OrderPlacedJson {
        order_id: ev.order_id.clone(),
        customer: ev.customer.clone(),
        total_cents: ev.total_cents,
    };
    let integration_event = IntegrationEvent::from_domain_event(&ev, 1, "orders", payload)
        .with_correlation_id("corr-42");

    let wire = encode_for_outbox(&JsonEventSerializer, &integration_event)?;
    assert_eq!(wire.content_type, "application/json");

    let decoded: IntegrationEvent<OrderPlacedJson> = JsonEventSerializer.decode(&wire)?;
    assert_eq!(decoded.event_type, "OrderPlaced");
    assert_eq!(decoded.source, "orders");
    assert_eq!(decoded.schema_version, 1);
    assert_eq!(decoded.payload.order_id, "ord-42");
    assert_eq!(decoded.payload.total_cents, 12_000);
    assert_eq!(decoded.correlation_id.as_deref(), Some("corr-42"));
    Ok(())
}

#[test]
fn order_placed_encodes_as_protobuf_integration_event() -> Result<(), Box<dyn std::error::Error>> {
    let ev = sample_domain_event();
    let payload = OrderPlacedProto {
        order_id: ev.order_id.clone(),
        customer: ev.customer.clone(),
        total_cents: ev.total_cents,
    };
    let integration_event = IntegrationEvent::from_domain_event(&ev, 1, "orders", payload)
        .with_correlation_id("corr-42");

    let wire = encode_for_outbox(&ProtobufEventSerializer, &integration_event)?;
    assert_eq!(wire.content_type, APPLICATION_PROTOBUF);

    let decoded: IntegrationEvent<OrderPlacedProto> = ProtobufEventSerializer.decode(&wire)?;
    assert_eq!(decoded.event_type, "OrderPlaced");
    assert_eq!(decoded.source, "orders");
    assert_eq!(decoded.schema_version, 1);
    assert_eq!(decoded.payload.order_id, "ord-42");
    assert_eq!(decoded.payload.total_cents, 12_000);
    assert_eq!(decoded.correlation_id.as_deref(), Some("corr-42"));
    Ok(())
}

#[test]
fn both_codecs_produce_equivalent_envelope_metadata() -> Result<(), Box<dyn std::error::Error>> {
    let ev = sample_domain_event();

    let json_event = IntegrationEvent::from_domain_event(
        &ev,
        1,
        "orders",
        OrderPlacedJson {
            order_id: ev.order_id.clone(),
            customer: ev.customer.clone(),
            total_cents: ev.total_cents,
        },
    )
    .with_correlation_id("corr-42")
    .with_tenant_id("tenant-acme");

    let proto_event = IntegrationEvent::from_domain_event(
        &ev,
        1,
        "orders",
        OrderPlacedProto {
            order_id: ev.order_id.clone(),
            customer: ev.customer.clone(),
            total_cents: ev.total_cents,
        },
    )
    .with_correlation_id("corr-42")
    .with_tenant_id("tenant-acme");

    let json_wire = encode_for_outbox(&JsonEventSerializer, &json_event)?;
    let proto_wire = encode_for_outbox(&ProtobufEventSerializer, &proto_event)?;

    let json_decoded: IntegrationEvent<OrderPlacedJson> = JsonEventSerializer.decode(&json_wire)?;
    let proto_decoded: IntegrationEvent<OrderPlacedProto> =
        ProtobufEventSerializer.decode(&proto_wire)?;

    // Envelope fields are identical regardless of wire format.
    assert_eq!(json_decoded.event_type, proto_decoded.event_type);
    assert_eq!(json_decoded.schema_version, proto_decoded.schema_version);
    assert_eq!(json_decoded.source, proto_decoded.source);
    assert_eq!(json_decoded.correlation_id, proto_decoded.correlation_id);
    assert_eq!(json_decoded.tenant_id, proto_decoded.tenant_id);

    // Payload round-trips correctly in both formats.
    assert_eq!(
        json_decoded.payload.order_id,
        proto_decoded.payload.order_id
    );
    assert_eq!(
        json_decoded.payload.total_cents,
        proto_decoded.payload.total_cents
    );

    // Protobuf is smaller for the same logical data.
    assert!(
        proto_wire.payload.len() < json_wire.payload.len(),
        "proto ({} B) should be smaller than json ({} B)",
        proto_wire.payload.len(),
        json_wire.payload.len(),
    );
    Ok(())
}
