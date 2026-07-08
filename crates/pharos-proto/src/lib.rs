//! Protobuf serialization support for Pharos RS integration events.
//!
//! This crate provides [`ProtobufEventSerializer`], the binary-protocol
//! counterpart of `pharos-app`'s [`JsonEventSerializer`]. Payload types must
//! implement [`prost::Message`] and [`Default`]; the envelope metadata
//! (event id, type, schema version, timestamps, correlations, …) is handled
//! automatically by [`IntegrationEventEnvelope`].
//!
//! # Choosing a serializer
//!
//! | Scenario | Serializer |
//! |---|---|
//! | HTTP APIs, human-readable logs, early development | [`JsonEventSerializer`] |
//! | Kafka pipelines, low-latency dispatch, strict schema contracts | [`ProtobufEventSerializer`] |
//!
//! # Quick start
//!
//! 1. Add `pharos-proto` and `prost` to your `Cargo.toml`:
//!
//! ```toml
//! pharos-proto = { git = "ssh://git@github.com/lisboaigor/pharos-rs", branch = "main" }
//! prost        = "0.13"
//! ```
//!
//! 2. Derive [`prost::Message`] on your payload type:
//!
//! ```rust,ignore
//! #[derive(Clone, Default, prost::Message)]
//! pub struct OrderPlaced {
//!     #[prost(string, tag = "1")]
//!     pub order_id: String,
//!     #[prost(uint64, tag = "2")]
//!     pub amount_cents: u64,
//! }
//! ```
//!
//! 3. Serialize and deserialize with [`ProtobufEventSerializer`]:
//!
//! ```rust,ignore
//! use pharos_app::IntegrationEvent;
//! use pharos_proto::ProtobufEventSerializer;
//!
//! let serializer = ProtobufEventSerializer;
//! let event      = IntegrationEvent::new("OrderPlaced", 1, "orders", OrderPlaced { .. });
//!
//! let wire = serializer.serialize(&event)?;
//! // wire.content_type == "application/x-protobuf"
//!
//! let recovered: IntegrationEvent<OrderPlaced> = serializer.deserialize(&wire)?;
//! ```
//!
//! [`JsonEventSerializer`]: pharos_app::serialization::JsonEventSerializer

pub mod envelope;
pub mod error;
pub mod serializer;

pub use envelope::IntegrationEventEnvelope;
pub use error::ProtobufSerializationError;
pub use serializer::{APPLICATION_PROTOBUF, ProtobufEventSerializer};

/// Re-export of [`MessageCodec`] so callers writing codec-generic code do not
/// need to add `pharos-app` as a direct dependency.
pub use pharos_app::MessageCodec;

/// Re-export of [`prost`] so downstream crates do not need to add it as a
/// direct dependency when deriving [`prost::Message`] on payload types.
pub use prost;
