use serde::{Deserialize, Serialize};

use crate::integration_event::IntegrationEvent;

/// Identifies the tenant that owns the data and events in the current operation.
///
/// In a multi-tenant system every request runs on behalf of exactly one tenant.
/// Threading a `TenantContext` through application services and into the
/// infrastructure layer keeps that identity explicit, so adapters can scope
/// storage and messaging to a single tenant and never leak rows across the
/// boundary. The tenant-scoped PostgreSQL adapters use it to enforce row-level
/// isolation.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TenantContext {
    tenant_id: String,
}

impl TenantContext {
    /// Creates a tenant context for the given tenant identifier.
    pub fn new(tenant_id: impl Into<String>) -> Self {
        Self {
            tenant_id: tenant_id.into(),
        }
    }

    /// Returns the tenant identifier.
    pub fn tenant_id(&self) -> &str {
        &self.tenant_id
    }

    /// Stamps an integration event with this tenant identifier.
    pub fn stamp<P>(&self, event: IntegrationEvent<P>) -> IntegrationEvent<P> {
        event.with_tenant_id(self.tenant_id.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stamps_integration_event_with_tenant() {
        let tenant = TenantContext::new("tenant-7");
        assert_eq!(tenant.tenant_id(), "tenant-7");

        let event = tenant.stamp(IntegrationEvent::new("OrderPlaced", 1, "orders", "payload"));
        assert_eq!(event.tenant_id.as_deref(), Some("tenant-7"));
    }
}
