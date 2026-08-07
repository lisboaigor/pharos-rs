use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use std::str::FromStr;
use std::sync::Arc;

/// Session setting the row-level security policies read to scope a query.
///
/// Exposed so policies and migrations can name it from one place:
///
/// ```sql
/// CREATE POLICY tenant_isolation ON orders
///     USING      (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid)
///     WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
/// ```
///
/// The `NULLIF` matters: with no tenant in scope the setting is empty, the cast
/// yields NULL, and the comparison matches nothing — denying by default rather
/// than exposing every tenant's rows.
pub const TENANT_SETTING: &str = "app.tenant_id";

/// A pooled set of PostgreSQL connections shared by the adapters in this crate.
///
/// `Pool` is a `sqlx::PgPool`, which is cheap to clone (reference-counted)
/// and can be shared across every repository and store in an application.
pub type Pool = sqlx::PgPool;

/// Errors produced while building or using a connection [`Pool`].
#[derive(Debug, thiserror::Error)]
pub enum PgPoolError {
    /// The connection string could not be parsed or the pool could not be built.
    #[error("failed to configure connection pool: {0}")]
    Config(sqlx::Error),
    /// A database operation failed.
    #[error("database error: {0}")]
    Db(sqlx::Error),
}

impl From<sqlx::Error> for PgPoolError {
    fn from(e: sqlx::Error) -> Self {
        PgPoolError::Db(e)
    }
}

/// Builds a connection [`Pool`] from a libpq-style or URL connection string.
///
/// The pool is constructed lazily: connections are opened on first use.
/// `max_connections` bounds the maximum number of concurrent connections.
///
/// Accepts both URL-style (`postgres://user:pass@host/db`) and key-value style
/// (`host=localhost user=postgres dbname=app`).
///
/// This helper uses the default TLS configuration from `sqlx`. For custom TLS
/// setup, build a `PgPool` directly via `PgPoolOptions` and pass it to the
/// adapter constructors.
pub fn connect_pool(connection_string: &str, max_connections: u32) -> Result<Pool, PgPoolError> {
    let options = PgConnectOptions::from_str(connection_string).map_err(PgPoolError::Config)?;
    Ok(PgPoolOptions::new()
        .max_connections(max_connections)
        .connect_lazy_with(options))
}

/// Builds a [`Pool`] that stamps the current tenant onto every connection it
/// hands out, so row-level security policies reading [`TENANT_SETTING`] scope
/// each query on their own.
///
/// `tenant_of` is called once per connection checkout and returns the tenant in
/// scope, or `None` outside any tenant (a login endpoint, a health probe, an
/// admin plane). Taking a closure rather than reading a task-local keeps this
/// crate agnostic about where the application stores that.
///
/// # Why three hooks
///
/// This is the part that is easy to get wrong, because getting it wrong fails
/// *silently*:
///
/// - `after_connect` — a freshly opened connection returns straight from
///   `connect()` to whoever asked for it. It is the **only** hook it passes
///   through.
/// - `before_acquire` — sqlx calls this exclusively for connections reused from
///   the idle pool (`check_idle_conn`). A new connection never reaches it.
/// - `after_release` — clears the setting, so a tenant cannot leak to the next
///   request that reuses the same physical connection.
///
/// Wiring only `before_acquire` looks correct and works under light load: the
/// pool is lazy, so the first requests after every restart — and any growth
/// under concurrency — get connections with no tenant set. The policies then
/// match nothing: reads come back empty (a phantom "not found" with no error
/// anywhere) and writes fail with a row-level security violation.
///
/// ```no_run
/// # use pharos_postgres::{tenant_pool, PgPoolError};
/// # fn tenant_in_scope() -> Option<String> { None }
/// let pool = tenant_pool("postgres://localhost/app", 16, tenant_in_scope)?;
/// # Ok::<(), PgPoolError>(())
/// ```
pub fn tenant_pool<F>(
    connection_string: &str,
    max_connections: u32,
    tenant_of: F,
) -> Result<Pool, PgPoolError>
where
    F: Fn() -> Option<String> + Send + Sync + 'static,
{
    let options = PgConnectOptions::from_str(connection_string).map_err(PgPoolError::Config)?;
    let on_connect = Arc::new(tenant_of);
    let on_acquire = Arc::clone(&on_connect);

    Ok(PgPoolOptions::new()
        .max_connections(max_connections)
        .after_connect(move |conn, _meta| {
            let tenant = on_connect();
            Box::pin(async move { apply_tenant(conn, tenant.as_deref()).await })
        })
        .before_acquire(move |conn, _meta| {
            let tenant = on_acquire();
            Box::pin(async move {
                apply_tenant(conn, tenant.as_deref()).await?;
                Ok(true)
            })
        })
        .after_release(|conn, _meta| {
            Box::pin(async move {
                apply_tenant(conn, None).await?;
                Ok(true)
            })
        })
        .connect_lazy_with(options))
}

/// Writes the tenant onto the connection's session, or clears it when `None`.
///
/// An empty string rather than `RESET`: the policies use
/// `NULLIF(current_setting(...), '')`, so empty means "no tenant" and denies by
/// default. `RESET` would leave the setting undefined, and `current_setting`
/// with `missing_ok` would return NULL — the same outcome, but only by accident
/// of the policy's own null handling.
async fn apply_tenant(
    conn: &mut sqlx::PgConnection,
    tenant: Option<&str>,
) -> Result<(), sqlx::Error> {
    sqlx::query("SELECT set_config($1, $2, false)")
        .bind(TENANT_SETTING)
        .bind(tenant.unwrap_or_default())
        .execute(conn)
        .await?;
    Ok(())
}
