use std::fmt::{Display, Formatter};
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::integration_event::IntegrationEvent;

/// Error returned when a string is not a valid tenant identifier.
#[derive(Debug, thiserror::Error)]
#[error("invalid tenant id: {0}")]
pub struct InvalidTenantId(#[from] uuid::Error);

/// Strongly typed tenant identifier.
///
/// A `TenantId` is always a valid UUID: the fallible conversion from text
/// happens exactly once, at the edge that receives the raw value (an HTTP
/// header, a JWT claim, a CLI flag). Everything downstream — application
/// services, repositories, adapters — carries the already-validated type, so
/// no adapter ever needs to re-parse a tenant id or invent a fallback for a
/// malformed one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TenantId(Uuid);

impl TenantId {
    /// Wraps an existing UUID.
    pub fn new(value: Uuid) -> Self {
        Self(value)
    }

    /// Parses a tenant id from its string representation.
    pub fn parse(value: &str) -> Result<Self, InvalidTenantId> {
        Ok(Self(Uuid::parse_str(value)?))
    }

    /// Returns the underlying UUID.
    pub fn as_uuid(&self) -> Uuid {
        self.0
    }
}

impl From<Uuid> for TenantId {
    fn from(value: Uuid) -> Self {
        Self(value)
    }
}

impl Display for TenantId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for TenantId {
    type Err = InvalidTenantId;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

/// Identifies the tenant that owns the data and events in the current operation.
///
/// In a multi-tenant system every request runs on behalf of exactly one tenant.
/// Threading a `TenantContext` through application services and into the
/// infrastructure layer keeps that identity explicit, so adapters can scope
/// storage and messaging to a single tenant and never leak rows across the
/// boundary. The tenant-scoped PostgreSQL adapters use it to enforce row-level
/// isolation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TenantContext {
    tenant_id: TenantId,
}

impl TenantContext {
    /// Creates a tenant context for an already-validated tenant identifier.
    pub fn new(tenant_id: impl Into<TenantId>) -> Self {
        Self {
            tenant_id: tenant_id.into(),
        }
    }

    /// Parses a tenant context from a raw string (e.g. a header or claim).
    ///
    /// This is the single fallible step of tenant identification: call it at
    /// the request edge and pass the resulting context downstream.
    pub fn parse(tenant_id: &str) -> Result<Self, InvalidTenantId> {
        Ok(Self {
            tenant_id: TenantId::parse(tenant_id)?,
        })
    }

    /// Returns the tenant identifier.
    pub fn tenant_id(&self) -> TenantId {
        self.tenant_id
    }

    /// Stamps an integration event with this tenant identifier.
    pub fn stamp<P>(&self, event: IntegrationEvent<P>) -> IntegrationEvent<P> {
        event.with_tenant_id(self.tenant_id.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_valid_tenant_ids_and_rejects_garbage() -> Result<(), InvalidTenantId> {
        let uuid = Uuid::now_v7();
        let parsed = TenantId::parse(&uuid.to_string())?;
        assert_eq!(parsed.as_uuid(), uuid);

        assert!(TenantId::parse("acme").is_err());
        assert!(TenantId::parse("").is_err());
        Ok(())
    }

    #[test]
    fn stamps_integration_event_with_tenant() {
        let uuid = Uuid::now_v7();
        let tenant = TenantContext::new(uuid);
        assert_eq!(tenant.tenant_id().as_uuid(), uuid);

        let event = tenant.stamp(IntegrationEvent::new("OrderPlaced", 1, "orders", "payload"));
        assert_eq!(event.tenant_id.as_deref(), Some(uuid.to_string().as_str()));
    }
}
