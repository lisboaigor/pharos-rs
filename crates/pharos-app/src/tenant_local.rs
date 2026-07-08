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
