use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error;

use crate::integration_event::IntegrationEvent;

/// Serialized representation of an integration event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SerializedEvent {
    /// MIME content type of the payload.
    pub content_type: String,
    /// Serialized event bytes.
    pub payload: Vec<u8>,
}

impl SerializedEvent {
    /// Creates a serialized event from content type and bytes.
    pub fn new(content_type: impl Into<String>, payload: Vec<u8>) -> Self {
        Self {
            content_type: content_type.into(),
            payload,
        }
    }
}

/// Errors produced while serializing or deserializing events.
#[derive(Debug, Error)]
pub enum EventSerializationError {
    /// JSON serialization/deserialization failed.
    #[error("json serialization failed: {0}")]
    Json(#[from] serde_json::Error),
}

/// Unified codec for encoding and decoding [`IntegrationEvent<P>`] envelopes.
///
/// This is the preferred abstraction when writing code that must remain
/// agnostic about the wire format. Both [`JsonEventSerializer`] and
/// `pharos_proto::ProtobufEventSerializer` implement this trait, so a single
/// generic function can accept either serializer:
///
/// ```rust,ignore
/// use pharos_app::{IntegrationEvent, MessageCodec, serialization::SerializedEvent};
///
/// fn enqueue<P, C>(codec: &C, event: &IntegrationEvent<P>) -> SerializedEvent
/// where
///     C: MessageCodec<P>,
/// {
///     codec.encode(event).expect("encoding should not fail")
/// }
///
/// // Works with JSON:
/// enqueue(&JsonEventSerializer, &json_event);
/// // Works with Protobuf:
/// enqueue(&ProtobufEventSerializer, &proto_event);
/// ```
pub trait MessageCodec<P>: Send + Sync + 'static {
    /// Error type returned by [`encode`](Self::encode) and [`decode`](Self::decode).
    type Error: std::error::Error + Send + Sync + 'static;

    /// Encodes an [`IntegrationEvent<P>`] into a [`SerializedEvent`].
    fn encode(&self, event: &IntegrationEvent<P>) -> Result<SerializedEvent, Self::Error>;

    /// Decodes a [`SerializedEvent`] back into an [`IntegrationEvent<P>`].
    fn decode(&self, wire: &SerializedEvent) -> Result<IntegrationEvent<P>, Self::Error>;
}

/// Converts integration event envelopes to and from bytes.
pub trait EventSerializer: Send + Sync + 'static {
    /// Serializes an integration event envelope.
    fn serialize<P: Serialize>(
        &self,
        event: &IntegrationEvent<P>,
    ) -> Result<SerializedEvent, EventSerializationError>;

    /// Deserializes an integration event envelope.
    fn deserialize<P: DeserializeOwned>(
        &self,
        event: &SerializedEvent,
    ) -> Result<IntegrationEvent<P>, EventSerializationError>;
}

/// JSON implementation of [`EventSerializer`] and [`MessageCodec`].
#[derive(Debug, Default, Clone, Copy)]
pub struct JsonEventSerializer;

impl<P: Serialize + DeserializeOwned + 'static> MessageCodec<P> for JsonEventSerializer {
    type Error = EventSerializationError;

    fn encode(&self, event: &IntegrationEvent<P>) -> Result<SerializedEvent, Self::Error> {
        self.serialize(event)
    }

    fn decode(&self, wire: &SerializedEvent) -> Result<IntegrationEvent<P>, Self::Error> {
        self.deserialize(wire)
    }
}

impl EventSerializer for JsonEventSerializer {
    fn serialize<P: Serialize>(
        &self,
        event: &IntegrationEvent<P>,
    ) -> Result<SerializedEvent, EventSerializationError> {
        let payload = serde_json::to_vec(event)?;
        Ok(SerializedEvent::new("application/json", payload))
    }

    fn deserialize<P: DeserializeOwned>(
        &self,
        event: &SerializedEvent,
    ) -> Result<IntegrationEvent<P>, EventSerializationError> {
        Ok(serde_json::from_slice(&event.payload)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct Payload {
        order_id: String,
    }

    fn roundtrip_via_codec<P, C>(
        codec: C,
        event: IntegrationEvent<P>,
    ) -> Result<IntegrationEvent<P>, C::Error>
    where
        P: PartialEq + std::fmt::Debug,
        C: MessageCodec<P>,
    {
        let wire = codec.encode(&event)?;
        codec.decode(&wire)
    }

    #[test]
    fn json_serializer_implements_message_codec() -> Result<(), Box<dyn std::error::Error>> {
        let event = IntegrationEvent::new(
            "OrderPlaced",
            1,
            "orders",
            Payload {
                order_id: "order-1".to_string(),
            },
        )
        .with_correlation_id("corr-1");

        let recovered = roundtrip_via_codec(JsonEventSerializer, event.clone())?;

        assert_eq!(recovered.event_id, event.event_id);
        assert_eq!(recovered.event_type, "OrderPlaced");
        assert_eq!(recovered.payload, event.payload);
        assert_eq!(recovered.correlation_id.as_deref(), Some("corr-1"));
        Ok(())
    }

    #[test]
    fn serializes_and_deserializes_json_envelope() -> Result<(), Box<dyn std::error::Error>> {
        let serializer = JsonEventSerializer;
        let event = IntegrationEvent::new(
            "OrderPlaced",
            1,
            "orders",
            Payload {
                order_id: "order-1".to_string(),
            },
        )
        .with_correlation_id("corr-1");

        let serialized = serializer.serialize(&event)?;
        let deserialized: IntegrationEvent<Payload> = serializer.deserialize(&serialized)?;

        assert_eq!(serialized.content_type, "application/json");
        assert_eq!(deserialized.event_type, "OrderPlaced");
        assert_eq!(deserialized.schema_version, 1);
        assert_eq!(deserialized.correlation_id.as_deref(), Some("corr-1"));
        assert_eq!(deserialized.payload.order_id, "order-1");
        Ok(())
    }
}
