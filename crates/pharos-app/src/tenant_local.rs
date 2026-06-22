use crate::tenant::TenantContext;

tokio::task_local! {
    /// Tenant context for the current asynchronous task.
    ///
    /// Storing the `TenantContext` in a task-local variable lets middleware or
    /// request handlers set it once at the edge and read it anywhere downstream
    /// without threading it through every function signature.
    ///
    /// The task-local is `Option<TenantContext>` so code running outside a
    /// scoped tenant context can still call `CURRENT_TENANT.with(...)` safely.
    pub static CURRENT_TENANT: Option<TenantContext>;
}
