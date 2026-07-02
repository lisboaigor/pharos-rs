use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use pharos_core::DomainEvent;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Metadata envelope used to publish events outside the current process.
///
/// Domain events model facts inside a bounded context. Integration events are
/// the serialized contract sent to external brokers, services, or pipelines.
/// The envelope carries operational metadata needed by distributed systems:
/// event identity, schema version, correlation, causation, source, tenant, and
/// trace metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IntegrationEvent<P> {
    /// Unique event identifier. Generated as UUID v7 for temporal ordering.
    pub event_id: Uuid,
    /// Logical event type used for routing and schema lookup.
    pub event_type: String,
    /// Event schema version.
    pub schema_version: u32,
    /// Time when the integration event envelope was created.
    pub occurred_at: DateTime<Utc>,
    /// Optional aggregate identifier associated with the event.
    pub aggregate_id: Option<String>,
    /// Correlation identifier that ties a business flow together.
    pub correlation_id: Option<String>,
    /// Identifier of the event/command/message that caused this event.
    pub causation_id: Option<String>,
    /// Component or service that emitted the event.
    pub source: String,
    /// Optional tenant identifier for multi-tenant systems.
    pub tenant_id: Option<String>,
    /// Optional tracing identifier propagated from the current request/trace.
    pub trace_id: Option<String>,
    /// Event-specific payload.
    pub payload: P,
    /// Additional string metadata for adapters and consumers.
    pub metadata: BTreeMap<String, String>,
}

impl<P> IntegrationEvent<P> {
    /// Creates a new integration event envelope with a UUID v7 event id.
    pub fn new(
        event_type: impl Into<String>,
        schema_version: u32,
        source: impl Into<String>,
        payload: P,
    ) -> Self {
        Self {
            event_id: Uuid::now_v7(),
            event_type: event_type.into(),
            schema_version,
            occurred_at: Utc::now(),
            aggregate_id: None,
            correlation_id: None,
            causation_id: None,
            source: source.into(),
            tenant_id: None,
            trace_id: None,
            payload,
            metadata: BTreeMap::new(),
        }
    }

    /// Creates an integration envelope using metadata from a domain event.
    pub fn from_domain_event(
        event: &dyn DomainEvent,
        schema_version: u32,
        source: impl Into<String>,
        payload: P,
    ) -> Self {
        Self::new(event.event_type(), schema_version, source, payload)
            .with_occurred_at(event.occurred_at())
            .with_aggregate_id(event.aggregate_id())
    }

    /// Sets the event timestamp.
    pub fn with_occurred_at(mut self, occurred_at: DateTime<Utc>) -> Self {
        self.occurred_at = occurred_at;
        self
    }

    /// Sets the aggregate id.
    pub fn with_aggregate_id(mut self, aggregate_id: impl Into<String>) -> Self {
        self.aggregate_id = Some(aggregate_id.into());
        self
    }

    /// Sets the correlation id.
    pub fn with_correlation_id(mut self, correlation_id: impl Into<String>) -> Self {
        self.correlation_id = Some(correlation_id.into());
        self
    }

    /// Sets the causation id.
    pub fn with_causation_id(mut self, causation_id: impl Into<String>) -> Self {
        self.causation_id = Some(causation_id.into());
        self
    }

    /// Sets the tenant id.
    pub fn with_tenant_id(mut self, tenant_id: impl Into<String>) -> Self {
        self.tenant_id = Some(tenant_id.into());
        self
    }

    /// Sets the trace id.
    pub fn with_trace_id(mut self, trace_id: impl Into<String>) -> Self {
        self.trace_id = Some(trace_id.into());
        self
    }

    /// Adds one metadata entry.
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};

    #[derive(Debug)]
    struct OrderPlaced {
        occurred_at: DateTime<Utc>,
        order_id: String,
    }

    impl DomainEvent for OrderPlaced {
        fn event_type(&self) -> &'static str {
            "OrderPlaced"
        }

        fn occurred_at(&self) -> DateTime<Utc> {
            self.occurred_at
        }

        fn aggregate_id(&self) -> &str {
            self.order_id.as_str()
        }
    }

    #[test]
    fn creates_envelope_with_operational_metadata() {
        let event = IntegrationEvent::new("OrderPlaced", 1, "orders", "payload")
            .with_aggregate_id("order-1")
            .with_correlation_id("corr-1")
            .with_causation_id("cmd-1")
            .with_tenant_id("tenant-1")
            .with_trace_id("trace-1")
            .with_metadata("partition", "orders");

        assert_eq!(event.event_id.get_version_num(), 7);
        assert_eq!(event.event_type, "OrderPlaced");
        assert_eq!(event.schema_version, 1);
        assert_eq!(event.aggregate_id.as_deref(), Some("order-1"));
        assert_eq!(event.correlation_id.as_deref(), Some("corr-1"));
        assert_eq!(event.causation_id.as_deref(), Some("cmd-1"));
        assert_eq!(event.tenant_id.as_deref(), Some("tenant-1"));
        assert_eq!(event.trace_id.as_deref(), Some("trace-1"));
        assert_eq!(
            event.metadata.get("partition").map(String::as_str),
            Some("orders")
        );
    }

    #[test]
    fn creates_envelope_from_domain_event() -> Result<(), Box<dyn std::error::Error>> {
        let occurred_at = Utc
            .with_ymd_and_hms(2026, 1, 2, 3, 4, 5)
            .single()
            .ok_or("2026-01-02T03:04:05 is a valid unambiguous UTC date")?;
        let domain_event = OrderPlaced {
            occurred_at,
            order_id: "order-123".to_string(),
        };

        let integration_event =
            IntegrationEvent::from_domain_event(&domain_event, 2, "orders", "payload");

        assert_eq!(integration_event.event_type, "OrderPlaced");
        assert_eq!(integration_event.schema_version, 2);
        assert_eq!(integration_event.occurred_at, occurred_at);
        assert_eq!(integration_event.aggregate_id.as_deref(), Some("order-123"));
        assert_eq!(integration_event.source, "orders");

        Ok(())
    }
}
