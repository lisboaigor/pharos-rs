use std::collections::BTreeMap;

use chrono::DateTime;
use prost::Message;
use uuid::Uuid;

use pharos_app::IntegrationEvent;

use crate::error::ProtobufSerializationError;

/// Wire-format envelope for [`IntegrationEvent`] encoded as Protobuf.
///
/// All fields mirror those of [`IntegrationEvent`]; optional string fields are
/// represented as `Option<String>` using proto3 optional semantics. The generic
/// payload `P` is encoded separately as raw bytes and stored in `payload`.
///
/// Field tag assignments are **stable** — never reuse a tag even after removing
/// a field, as existing encoded messages would be misinterpreted.
#[derive(Clone, PartialEq, prost::Message)]
pub struct IntegrationEventEnvelope {
    /// UUID v7 string representation of the event identifier.
    #[prost(string, tag = "1")]
    pub event_id: String,

    /// Logical event type used for routing and schema lookup.
    #[prost(string, tag = "2")]
    pub event_type: String,

    /// Event schema version.
    #[prost(uint32, tag = "3")]
    pub schema_version: u32,

    /// Unix timestamp in milliseconds (UTC) when the envelope was created.
    #[prost(int64, tag = "4")]
    pub occurred_at_ms: i64,

    /// Optional aggregate identifier.
    #[prost(string, optional, tag = "5")]
    pub aggregate_id: Option<String>,

    /// Optional correlation identifier.
    #[prost(string, optional, tag = "6")]
    pub correlation_id: Option<String>,

    /// Optional causation identifier.
    #[prost(string, optional, tag = "7")]
    pub causation_id: Option<String>,

    /// Component or service that emitted the event.
    #[prost(string, tag = "8")]
    pub source: String,

    /// Optional tenant identifier for multi-tenant systems.
    #[prost(string, optional, tag = "9")]
    pub tenant_id: Option<String>,

    /// Optional distributed trace identifier.
    #[prost(string, optional, tag = "10")]
    pub trace_id: Option<String>,

    /// Protobuf-encoded payload bytes (the domain-specific message body).
    #[prost(bytes = "vec", tag = "11")]
    pub payload: Vec<u8>,

    /// Arbitrary string metadata forwarded by adapters and consumers.
    ///
    /// `BTreeMap`, not `HashMap`: prost emits a map field's entries in
    /// iteration order, which `HashMap` does not guarantee to be stable
    /// across processes or even across two runs of the same binary. Nothing
    /// in this crate depends on that today, but [`IntegrationEvent`]'s own
    /// `metadata` is already a `BTreeMap` for exactly this reason — a
    /// checksum, HMAC, or content-addressed id computed over the encoded
    /// bytes would otherwise vary for what is logically the same envelope.
    /// Matching the JSON side's ordering here means that guarantee, whenever
    /// it is added, does not have to special-case one wire format.
    #[prost(btree_map = "string, string", tag = "12")]
    pub metadata: BTreeMap<String, String>,
}

impl IntegrationEventEnvelope {
    /// Builds an envelope from an [`IntegrationEvent`], encoding the payload
    /// with prost. The caller must ensure `P` implements [`prost::Message`].
    pub fn from_integration_event<P: Message>(event: &IntegrationEvent<P>) -> Self {
        Self {
            event_id: event.event_id.to_string(),
            event_type: event.event_type.clone(),
            schema_version: event.schema_version,
            occurred_at_ms: event.occurred_at.timestamp_millis(),
            aggregate_id: event.aggregate_id.clone(),
            correlation_id: event.correlation_id.as_ref().map(ToString::to_string),
            causation_id: event.causation_id.as_ref().map(ToString::to_string),
            source: event.source.clone(),
            tenant_id: event.tenant_id.clone(),
            trace_id: event.trace_id.clone(),
            payload: event.payload.encode_to_vec(),
            metadata: event
                .metadata
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        }
    }

    /// Reconstructs an [`IntegrationEvent<P>`] by decoding the stored payload
    /// bytes with prost. Returns an error if the envelope header or the payload
    /// cannot be parsed.
    pub fn into_integration_event<P: Message + Default>(
        self,
    ) -> Result<IntegrationEvent<P>, ProtobufSerializationError> {
        let event_id = Uuid::parse_str(&self.event_id)
            .map_err(|e| ProtobufSerializationError::InvalidEnvelope(e.to_string()))?;

        let occurred_at = DateTime::from_timestamp_millis(self.occurred_at_ms).ok_or(
            ProtobufSerializationError::InvalidTimestamp(self.occurred_at_ms),
        )?;

        let payload = P::decode(self.payload.as_slice())?;

        Ok(IntegrationEvent {
            event_id,
            event_type: self.event_type,
            schema_version: self.schema_version,
            occurred_at,
            aggregate_id: self.aggregate_id,
            correlation_id: self.correlation_id.map(pharos_app::CorrelationId::from),
            causation_id: self.causation_id.map(pharos_app::CausationId::from),
            source: self.source,
            tenant_id: self.tenant_id,
            trace_id: self.trace_id,
            payload,
            metadata: self.metadata.into_iter().collect(),
        })
    }

    /// Encodes the envelope to a byte vector using Protobuf wire format.
    pub fn encode_to_bytes(&self) -> Vec<u8> {
        self.encode_to_vec()
    }

    /// Decodes an envelope from a Protobuf-encoded byte slice.
    pub fn decode_from_bytes(bytes: &[u8]) -> Result<Self, ProtobufSerializationError> {
        Ok(Self::decode(bytes)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn envelope_with_metadata(pairs: &[(&str, &str)]) -> IntegrationEventEnvelope {
        IntegrationEventEnvelope {
            // Fixed rather than freshly generated: only `metadata`'s
            // ordering is under test, so every other field must be
            // identical between the two envelopes being compared.
            event_id: "018ffc00-e489-7b82-8512-54d0383152b8".to_string(),
            event_type: "TestEvent".to_string(),
            schema_version: 1,
            occurred_at_ms: 0,
            aggregate_id: None,
            correlation_id: None,
            causation_id: None,
            source: "test".to_string(),
            tenant_id: None,
            trace_id: None,
            payload: Vec::new(),
            metadata: pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }

    /// `metadata` must serialize to the exact same bytes regardless of
    /// insertion order — a `HashMap` field would not guarantee this, and
    /// anything computed over the encoded bytes (a checksum, an HMAC, a
    /// content-addressed id) would then vary for what is logically the same
    /// envelope.
    #[test]
    fn metadata_encodes_deterministically_regardless_of_insertion_order() {
        let forward = envelope_with_metadata(&[("a", "1"), ("m", "2"), ("z", "3")]);
        let reverse = envelope_with_metadata(&[("z", "3"), ("m", "2"), ("a", "1")]);

        assert_eq!(forward.encode_to_bytes(), reverse.encode_to_bytes());
    }

    #[test]
    fn metadata_round_trips_through_encode_and_decode() -> Result<(), Box<dyn std::error::Error>> {
        let original = envelope_with_metadata(&[("region", "us-east-1"), ("shard", "7")]);

        let decoded = IntegrationEventEnvelope::decode_from_bytes(&original.encode_to_bytes())?;

        assert_eq!(
            decoded.metadata.get("region").map(String::as_str),
            Some("us-east-1")
        );
        assert_eq!(decoded.metadata.get("shard").map(String::as_str), Some("7"));
        Ok(())
    }
}
