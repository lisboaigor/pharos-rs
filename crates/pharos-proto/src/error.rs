use thiserror::Error;

/// Errors produced while encoding or decoding a Protobuf integration event.
#[derive(Debug, Error)]
pub enum ProtobufSerializationError {
    /// Protobuf wire-format decoding failed.
    #[error("protobuf decode failed: {0}")]
    Decode(#[from] prost::DecodeError),

    /// The envelope header contained an invalid or malformed field.
    #[error("invalid event envelope: {0}")]
    InvalidEnvelope(String),

    /// The `occurred_at_ms` field could not be converted to a valid UTC timestamp.
    #[error("invalid timestamp (ms since epoch): {0}")]
    InvalidTimestamp(i64),
}
