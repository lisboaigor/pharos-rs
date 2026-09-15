#[cfg(feature = "messaging")]
use pharos_messaging::Message;

/// Adds transport-agnostic metadata to an outgoing [`Message`] before it is
/// enqueued.
///
/// A store that delivers through a durable outbox (see
/// `pharos_app::AggregateStore`, and `pharos-postgres`'s
/// `PostgresAggregateStore`) applies every registered enricher to each
/// message it builds. This is how cross-cutting concerns — which tenant an
/// event belongs to, which trace it is part of — get attached without every
/// call site re-deriving them by hand.
///
/// Object-safe by design (no RPITIT): a store holds a
/// `Vec<Arc<dyn MessageEnricher>>` so callers can mix enrichers from
/// different crates (this crate's [`TenantHeader`], `pharos-observability`'s
/// trace-context enricher, an application's own) without the store needing a
/// type parameter per enricher.
#[cfg(feature = "messaging")]
pub trait MessageEnricher: Send + Sync {
    /// Mutates `message` in place — typically by adding a header.
    fn enrich(&self, message: &mut Message);
}

/// Stamps the outgoing message with the current tenant, read from
/// [`crate::CURRENT_TENANT`].
///
/// Requires the `tenant-task-local` feature. Writes the `tenant_id` header
/// only when a tenant is in scope; outside any tenant scope (a background
/// job, a health check) the message goes out unchanged — the same
/// deny-by-default posture `CURRENT_TENANT` already documents for storage
/// adapters.
#[cfg(all(feature = "messaging", feature = "tenant-task-local"))]
pub struct TenantHeader;

#[cfg(all(feature = "messaging", feature = "tenant-task-local"))]
impl MessageEnricher for TenantHeader {
    fn enrich(&self, message: &mut Message) {
        if let Some(tenant) = crate::tenant_local::CURRENT_TENANT
            .try_with(|t| *t)
            .ok()
            .flatten()
        {
            message
                .headers
                .insert("tenant_id".to_string(), tenant.tenant_id().to_string());
        }
    }
}
