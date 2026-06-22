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

/// JSON implementation of [`EventSerializer`].
#[derive(Debug, Default, Clone, Copy)]
pub struct JsonEventSerializer;

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

    #[test]
    fn serializes_and_deserializes_json_envelope() {
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

        let serialized = serializer.serialize(&event).unwrap();
        let deserialized: IntegrationEvent<Payload> = serializer.deserialize(&serialized).unwrap();

        assert_eq!(serialized.content_type, "application/json");
        assert_eq!(deserialized.event_type, "OrderPlaced");
        assert_eq!(deserialized.schema_version, 1);
        assert_eq!(deserialized.correlation_id.as_deref(), Some("corr-1"));
        assert_eq!(deserialized.payload.order_id, "order-1");
    }
}
