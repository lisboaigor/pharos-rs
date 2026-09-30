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
                .insert(TENANT_HEADER.to_string(), tenant.tenant_id().to_string());
        }
    }
}

/// The header [`TenantHeader`] writes and [`with_message_scope`] reads.
#[cfg(all(feature = "messaging", feature = "tenant-task-local"))]
pub const TENANT_HEADER: &str = "tenant_id";

/// The tenant stamped on `message`, if any.
///
/// `Ok(None)` is a message with no tenant header: it was produced outside any
/// tenant scope (a background job, a system event). `Err` is a header that is
/// present but not a valid tenant id, which is a corrupt stamp and must not be
/// mistaken for "no tenant".
#[cfg(all(feature = "messaging", feature = "tenant-task-local"))]
pub fn tenant_of(
    message: &Message,
) -> Result<Option<crate::tenant::TenantContext>, crate::tenant::InvalidTenantId> {
    message
        .headers
        .get(TENANT_HEADER)
        .map(|raw| crate::tenant::TenantContext::parse(raw))
        .transpose()
}

/// Runs `work` for a consumed `message`, inside the tenant scope stamped on it.
///
/// The consumer-side pair of [`TenantHeader`]: a producer stamps the tenant it
/// was running for, and whatever handles the message later, on another task and
/// maybe another node, reopens that scope here, so repositories and
/// row-level-security policies see the same tenant the event came from.
///
/// A message with no tenant header runs with no scope, as it was produced.
/// A malformed header returns `Err` **without running `work`**: running it
/// unscoped would look like a message from no tenant, and a corrupt stamp is
/// better refused (and retried or dead-lettered by the caller) than guessed at.
#[cfg(all(feature = "messaging", feature = "tenant-task-local"))]
pub async fn with_message_scope<F: std::future::Future>(
    message: &Message,
    work: F,
) -> Result<F::Output, crate::tenant::InvalidTenantId> {
    let scope = tenant_of(message)?;
    let work = crate::elevation::without_elevation(work);
    Ok(crate::tenant_local::CURRENT_TENANT.scope(scope, work).await)
}

#[cfg(all(test, feature = "messaging", feature = "tenant-task-local"))]
mod tests {
    use uuid::Uuid;

    use super::*;
    use crate::tenant::{TenantContext, TenantId};
    use crate::tenant_local::CURRENT_TENANT;

    fn current() -> Option<TenantId> {
        CURRENT_TENANT
            .try_with(|t| t.map(|c| c.tenant_id()))
            .ok()
            .flatten()
    }

    fn message() -> Message {
        Message::new("orders", Vec::new(), "application/json")
    }

    #[tokio::test]
    async fn a_stamped_message_reopens_the_producers_tenant_for_the_consumer() {
        let tenant = TenantId::new(Uuid::now_v7());

        // The producer side: stamp from the scope it runs in.
        let mut stamped = message();
        CURRENT_TENANT
            .scope(Some(TenantContext::new(tenant)), async {
                TenantHeader.enrich(&mut stamped);
            })
            .await;

        // The consumer side: no scope of its own.
        assert_eq!(current(), None);
        let seen = with_message_scope(&stamped, async { current() }).await;
        assert_eq!(seen.ok().flatten(), Some(tenant));
        assert_eq!(current(), None, "the scope does not outlive the work");
    }

    #[tokio::test]
    async fn an_unstamped_message_runs_with_no_scope() {
        let seen = with_message_scope(&message(), async { current() }).await;
        assert_eq!(seen.ok(), Some(None));
    }

    #[tokio::test]
    async fn a_malformed_stamp_is_refused_and_the_work_never_runs() {
        let bad = message().with_header(TENANT_HEADER, "acme");
        let ran = std::sync::atomic::AtomicBool::new(false);

        let outcome = with_message_scope(&bad, async {
            ran.store(true, std::sync::atomic::Ordering::SeqCst);
        })
        .await;

        assert!(outcome.is_err());
        assert!(!ran.load(std::sync::atomic::Ordering::SeqCst));
    }
}
