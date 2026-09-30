use crate::tenant::TenantContext;

tokio::task_local! {
    /// Tenant context for the current asynchronous task.
    ///
    /// **The canonical way to carry tenant identity is threading a
    /// [`TenantContext`] explicitly** through application services and into
    /// adapters — it is runtime-agnostic and visible in every signature. This
    /// task-local is the opt-in alternative (feature `tenant-task-local`,
    /// which pulls in Tokio) for stacks where middleware sets the tenant once
    /// at the edge and threading it explicitly is impractical.
    ///
    /// The task-local is `Option<TenantContext>` so code running outside a
    /// scoped tenant context can still call `CURRENT_TENANT.with(...)` safely.
    pub static CURRENT_TENANT: Option<TenantContext>;
}

/// Spawns `future` on the Tokio runtime **inside the caller's tenant scope**.
///
/// `tokio::spawn` starts a task with empty task-locals, so work handed to it
/// from a request silently loses [`CURRENT_TENANT`]: an adapter that reads the
/// scope then sees no tenant, which under a deny-by-default store means zero
/// rows or a refused write, with no error pointing at the spawn. This reads
/// the scope where it is called and reopens it around `future`.
///
/// With no tenant in scope the task runs with none, exactly as `tokio::spawn`
/// would. The scope is a snapshot taken at the call: later changes on the
/// caller's task do not reach the spawned one.
///
/// Only the tenant is carried over. An [`Elevation`](crate::elevation::Elevation)
/// is **not**: a task started from an elevated request must ask for its own, so
/// the privilege cannot pass to whatever gets spawned by accident.
///
/// Requires a running Tokio runtime, like [`tokio::spawn`].
pub fn spawn_scoped<F>(future: F) -> tokio::task::JoinHandle<F::Output>
where
    F: std::future::Future + Send + 'static,
    F::Output: Send + 'static,
{
    let scope = CURRENT_TENANT.try_with(|tenant| *tenant).ok().flatten();
    tokio::spawn(CURRENT_TENANT.scope(scope, future))
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;

    fn current() -> Option<TenantContext> {
        CURRENT_TENANT.try_with(|tenant| *tenant).ok().flatten()
    }

    #[tokio::test]
    async fn a_plain_spawn_loses_the_tenant_and_spawn_scoped_keeps_it() {
        let tenant = TenantContext::new(Uuid::now_v7());

        let (plain, scoped) = CURRENT_TENANT
            .scope(Some(tenant), async {
                let plain = tokio::spawn(async { current() }).await;
                let scoped = spawn_scoped(async { current() }).await;
                (plain, scoped)
            })
            .await;

        assert_eq!(plain.ok().flatten(), None, "tokio::spawn drops the scope");
        assert_eq!(scoped.ok().flatten(), Some(tenant));
    }

    #[tokio::test]
    async fn with_no_tenant_in_scope_the_task_has_none() {
        let scoped = spawn_scoped(async { current() }).await;
        assert_eq!(scoped.ok().flatten(), None);
    }

    #[tokio::test]
    async fn the_spawned_task_keeps_the_tenant_it_was_spawned_under() {
        let (a, b) = (
            TenantContext::new(Uuid::now_v7()),
            TenantContext::new(Uuid::now_v7()),
        );

        let from_a = CURRENT_TENANT.sync_scope(Some(a), || spawn_scoped(async { current() }));
        let from_b = CURRENT_TENANT.sync_scope(Some(b), || spawn_scoped(async { current() }));

        assert_eq!(from_a.await.ok().flatten(), Some(a));
        assert_eq!(from_b.await.ok().flatten(), Some(b));
    }
}
