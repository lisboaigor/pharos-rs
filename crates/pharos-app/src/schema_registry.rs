use std::future::Future;

use chrono::{DateTime, Utc};
use thiserror::Error;

/// Schema descriptor for an integration event contract.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventSchema {
    /// Logical event type.
    pub event_type: String,
    /// Schema version.
    pub version: u32,
    /// Schema format, e.g. `json-schema`, `avro`, or `protobuf`.
    pub format: String,
    /// Raw schema document.
    pub schema: String,
    /// Registration timestamp.
    pub registered_at: DateTime<Utc>,
}

impl EventSchema {
    /// Creates a new schema descriptor.
    pub fn new(
        event_type: impl Into<String>,
        version: u32,
        format: impl Into<String>,
        schema: impl Into<String>,
    ) -> Self {
        Self {
            event_type: event_type.into(),
            version,
            format: format.into(),
            schema: schema.into(),
            registered_at: Utc::now(),
        }
    }
}

/// Errors produced by schema registries.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum SchemaRegistryError {
    /// Schema does not exist.
    #[error("schema not found: event_type={event_type}, version={version}")]
    NotFound {
        /// Event type.
        event_type: String,
        /// Schema version.
        version: u32,
    },
    /// Registry storage or API failure.
    #[error("schema registry failed: {0}")]
    Storage(String),
}

/// Registry for versioned integration event schemas.
pub trait SchemaRegistry: Send + Sync + 'static {
    /// Registers or replaces a schema.
    fn register(
        &self,
        schema: EventSchema,
    ) -> impl Future<Output = Result<(), SchemaRegistryError>> + Send;
    /// Finds a schema by event type and version.
    fn get(
        &self,
        event_type: &str,
        version: u32,
    ) -> impl Future<Output = Result<Option<EventSchema>, SchemaRegistryError>> + Send;
    /// Returns the latest schema for an event type, when supported.
    fn latest(
        &self,
        event_type: &str,
    ) -> impl Future<Output = Result<Option<EventSchema>, SchemaRegistryError>> + Send;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_event_schema_descriptor() {
        let schema = EventSchema::new("OrderConfirmed", 1, "json-schema", "{}");

        assert_eq!(schema.event_type, "OrderConfirmed");
        assert_eq!(schema.version, 1);
        assert_eq!(schema.format, "json-schema");
    }
}
