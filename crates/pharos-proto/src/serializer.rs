use pharos_app::IntegrationEvent;
use pharos_app::serialization::{MessageCodec, SerializedEvent};
use prost::Message;

use crate::envelope::IntegrationEventEnvelope;
use crate::error::ProtobufSerializationError;

/// MIME content type used for Protobuf-encoded messages.
pub const APPLICATION_PROTOBUF: &str = "application/x-protobuf";

/// Serializes and deserializes [`IntegrationEvent`] envelopes using the
/// Protobuf binary format.
///
/// This is the binary-protocol counterpart of [`JsonEventSerializer`] in
/// `pharos-app`. The two serializers share the same conceptual API —
/// `serialize` / `deserialize` operating on [`SerializedEvent`] — but
/// `ProtobufEventSerializer` requires the payload type `P` to implement
/// [`prost::Message`] rather than serde's `Serialize` / `DeserializeOwned`.
///
/// The encoded bytes represent an [`IntegrationEventEnvelope`] message that
/// wraps all envelope metadata together with the prost-encoded `P` payload.
/// The `content_type` of the returned [`SerializedEvent`] is always
/// [`APPLICATION_PROTOBUF`], which consumers can inspect to select the correct
/// deserializer without additional out-of-band signalling.
///
/// # Example
///
/// ```rust,ignore
/// use pharos_app::IntegrationEvent;
/// use pharos_proto::ProtobufEventSerializer;
///
/// // The payload must derive prost::Message and Default.
/// #[derive(Clone, Default, prost::Message)]
/// struct OrderPlaced {
///     #[prost(string, tag = "1")]
///     pub order_id: String,
///     #[prost(uint64, tag = "2")]
///     pub amount_cents: u64,
/// }
///
/// let serializer = ProtobufEventSerializer;
///
/// let event = IntegrationEvent::new("OrderPlaced", 1, "orders", OrderPlaced {
///     order_id: "ord-1".into(),
///     amount_cents: 5000,
/// });
///
/// let serialized = serializer.serialize(&event).unwrap();
/// assert_eq!(serialized.content_type, "application/x-protobuf");
///
/// let roundtrip: IntegrationEvent<OrderPlaced> = serializer.deserialize(&serialized).unwrap();
/// assert_eq!(roundtrip.event_type, "OrderPlaced");
/// assert_eq!(roundtrip.payload.order_id, "ord-1");
/// ```
///
/// [`JsonEventSerializer`]: pharos_app::serialization::JsonEventSerializer
#[derive(Debug, Default, Clone, Copy)]
pub struct ProtobufEventSerializer;

impl<P: Message + Default + 'static> MessageCodec<P> for ProtobufEventSerializer {
    type Error = ProtobufSerializationError;

    fn encode(&self, event: &IntegrationEvent<P>) -> Result<SerializedEvent, Self::Error> {
        self.serialize(event)
    }

    fn decode(&self, wire: &SerializedEvent) -> Result<IntegrationEvent<P>, Self::Error> {
        self.deserialize(wire)
    }
}

impl ProtobufEventSerializer {
    /// Encodes an [`IntegrationEvent<P>`] envelope into Protobuf bytes.
    ///
    /// The payload `P` is encoded first with [`prost::Message::encode_to_vec`],
    /// then embedded inside the wire-format [`IntegrationEventEnvelope`]. The
    /// serialization of a `Vec`-backed buffer is infallible in prost, so this
    /// method only propagates errors from field-level conversions.
    pub fn serialize<P: Message>(
        &self,
        event: &IntegrationEvent<P>,
    ) -> Result<SerializedEvent, ProtobufSerializationError> {
        let envelope = IntegrationEventEnvelope::from_integration_event(event);
        let bytes = envelope.encode_to_bytes();
        Ok(SerializedEvent::new(APPLICATION_PROTOBUF, bytes))
    }

    /// Decodes a [`SerializedEvent`] produced by [`serialize`] back into an
    /// [`IntegrationEvent<P>`].
    ///
    /// The method first decodes the outer [`IntegrationEventEnvelope`], then
    /// uses prost to decode the embedded payload bytes into `P`. Both steps can
    /// fail with [`ProtobufSerializationError`].
    ///
    /// [`serialize`]: Self::serialize
    pub fn deserialize<P: Message + Default>(
        &self,
        event: &SerializedEvent,
    ) -> Result<IntegrationEvent<P>, ProtobufSerializationError> {
        let envelope = IntegrationEventEnvelope::decode_from_bytes(&event.payload)?;
        envelope.into_integration_event()
    }
}

#[cfg(test)]
mod tests {
    use pharos_app::MessageCodec;

    use super::*;

    #[derive(Clone, PartialEq, prost::Message)]
    struct TestPayload {
        #[prost(string, tag = "1")]
        pub id: String,
        #[prost(uint64, tag = "2")]
        pub value: u64,
    }

    fn make_event() -> IntegrationEvent<TestPayload> {
        IntegrationEvent::new(
            "TestEvent",
            1,
            "test-service",
            TestPayload {
                id: "abc-123".into(),
                value: 42,
            },
        )
        .with_aggregate_id("agg-1")
        .with_correlation_id("corr-1")
        .with_causation_id("cause-1")
        .with_tenant_id("tenant-1")
        .with_trace_id("trace-1")
        .with_metadata("region", "us-east-1")
    }

    #[test]
    fn roundtrip_preserves_all_envelope_fields() -> Result<(), Box<dyn std::error::Error>> {
        let serializer = ProtobufEventSerializer;
        let event = make_event();

        let serialized = serializer.serialize(&event)?;
        let recovered: IntegrationEvent<TestPayload> = serializer.deserialize(&serialized)?;

        assert_eq!(serialized.content_type, APPLICATION_PROTOBUF);
        assert_eq!(recovered.event_id, event.event_id);
        assert_eq!(recovered.event_type, "TestEvent");
        assert_eq!(recovered.schema_version, 1);
        // ms precision: sub-millisecond part is intentionally dropped by the wire format.
        assert_eq!(
            recovered.occurred_at.timestamp_millis(),
            event.occurred_at.timestamp_millis()
        );
        assert_eq!(recovered.aggregate_id.as_deref(), Some("agg-1"));
        assert_eq!(recovered.correlation_id.as_deref(), Some("corr-1"));
        assert_eq!(recovered.causation_id.as_deref(), Some("cause-1"));
        assert_eq!(recovered.tenant_id.as_deref(), Some("tenant-1"));
        assert_eq!(recovered.trace_id.as_deref(), Some("trace-1"));
        assert_eq!(recovered.source, "test-service");
        assert_eq!(
            recovered.metadata.get("region").map(String::as_str),
            Some("us-east-1")
        );
        Ok(())
    }

    #[test]
    fn roundtrip_preserves_payload_fields() -> Result<(), Box<dyn std::error::Error>> {
        let serializer = ProtobufEventSerializer;
        let event = make_event();

        let serialized = serializer.serialize(&event)?;
        let recovered: IntegrationEvent<TestPayload> = serializer.deserialize(&serialized)?;

        assert_eq!(recovered.payload.id, "abc-123");
        assert_eq!(recovered.payload.value, 42);
        Ok(())
    }

    #[test]
    fn serialized_bytes_are_smaller_than_json_for_same_payload()
    -> Result<(), Box<dyn std::error::Error>> {
        use pharos_app::serialization::{EventSerializer as _, JsonEventSerializer};
        use serde::{Deserialize, Serialize};

        #[derive(Clone, Serialize, Deserialize)]
        struct JsonPayload {
            id: String,
            value: u64,
        }

        let json_event = IntegrationEvent::new(
            "TestEvent",
            1,
            "test-service",
            JsonPayload {
                id: "abc-123".into(),
                value: 42,
            },
        )
        .with_aggregate_id("agg-1")
        .with_correlation_id("corr-1");

        let proto_event = make_event();

        let json_bytes = JsonEventSerializer.serialize(&json_event)?.payload.len();
        let proto_bytes = ProtobufEventSerializer
            .serialize(&proto_event)?
            .payload
            .len();

        assert!(
            proto_bytes < json_bytes,
            "expected proto ({proto_bytes}B) < json ({json_bytes}B)"
        );
        Ok(())
    }

    #[test]
    fn protobuf_serializer_implements_message_codec() -> Result<(), Box<dyn std::error::Error>> {
        fn roundtrip_via_codec<P, C>(
            codec: C,
            event: IntegrationEvent<P>,
        ) -> Result<IntegrationEvent<P>, C::Error>
        where
            C: MessageCodec<P>,
        {
            let wire = codec.encode(&event)?;
            codec.decode(&wire)
        }

        let event = make_event();
        let recovered = roundtrip_via_codec(ProtobufEventSerializer, event.clone())?;

        assert_eq!(recovered.event_id, event.event_id);
        assert_eq!(recovered.event_type, event.event_type);
        assert_eq!(recovered.payload, event.payload);
        Ok(())
    }

    #[test]
    fn deserialize_rejects_corrupted_bytes() {
        let serializer = ProtobufEventSerializer;
        let bad = SerializedEvent::new(APPLICATION_PROTOBUF, vec![0xFF, 0xFE, 0x00]);

        let result: Result<IntegrationEvent<TestPayload>, _> = serializer.deserialize(&bad);
        assert!(result.is_err());
    }
}
