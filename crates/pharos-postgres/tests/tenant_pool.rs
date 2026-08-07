//! Every connection the pool hands out must already carry the tenant, whether
//! it was freshly opened or reused from the idle pool.
//!
//! The failure this guards against is silent: a connection without
//! [`TENANT_SETTING`] makes row-level security match nothing, so reads come back
//! empty — a phantom "not found", with no error anywhere — and writes fail with
//! a policy violation. It only shows up once the pool has to open a connection,
//! which means under concurrency and on the first requests after every restart.
//!
//! Each test here fails if one of the three hooks in `tenant_pool` is removed;
//! that is the point of them.

use std::sync::{Arc, Mutex};

use pharos_postgres::{Pool, TENANT_SETTING, tenant_pool};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::{ContainerAsync, GenericImage, ImageExt, runners::AsyncRunner};
use uuid::Uuid;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

/// Lets a test move the "current tenant" between calls, standing in for
/// whatever task-local or extension an application would read.
#[derive(Clone, Default)]
struct TenantInScope(Arc<Mutex<Option<String>>>);

impl TenantInScope {
    fn set(&self, tenant: Option<&str>) {
        if let Ok(mut slot) = self.0.lock() {
            *slot = tenant.map(str::to_owned);
        }
    }

    fn reader(&self) -> impl Fn() -> Option<String> + Send + Sync + 'static {
        let cell = Arc::clone(&self.0);
        move || cell.lock().ok().and_then(|slot| slot.clone())
    }
}

async fn start_postgres()
-> Result<(ContainerAsync<GenericImage>, String), Box<dyn std::error::Error + Send + Sync>> {
    let container = GenericImage::new("postgres", "16-alpine")
        .with_exposed_port(5432.tcp())
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_PASSWORD", "postgres")
        .start()
        .await?;

    let host = container.get_host().await?.to_string();
    let port = container.get_host_port_ipv4(5432).await?;
    Ok((
        container,
        format!("postgres://postgres:postgres@{host}:{port}/postgres"),
    ))
}

/// Reads the setting through whichever connection the pool hands out.
async fn tenant_on_connection(pool: &Pool) -> Result<String, sqlx::Error> {
    sqlx::query_scalar("SELECT current_setting($1, true)")
        .bind(TENANT_SETTING)
        .fetch_one(pool)
        .await
}

/// A brand-new connection returns straight from `connect()` to its requester and
/// passes through `after_connect` alone — never `before_acquire`. Dropping that
/// hook makes this fail.
#[tokio::test]
async fn a_freshly_opened_connection_already_carries_the_tenant() -> TestResult {
    let (_container, url) = start_postgres().await?;
    let scope = TenantInScope::default();
    let tenant = Uuid::now_v7().to_string();
    scope.set(Some(&tenant));

    // Lazy pool: this very first acquire is what opens the connection.
    let pool = tenant_pool(&url, 4, scope.reader())?;

    assert_eq!(
        tenant_on_connection(&pool).await?,
        tenant,
        "a new connection reached the caller without the tenant; RLS would deny everything silently"
    );
    Ok(())
}

/// With one connection in the pool, the second checkout is necessarily a reuse,
/// which passes through `before_acquire` alone. Dropping that hook leaves the
/// first tenant on the connection.
#[tokio::test]
async fn a_reused_connection_swaps_to_the_new_tenant() -> TestResult {
    let (_container, url) = start_postgres().await?;
    let scope = TenantInScope::default();
    let pool = tenant_pool(&url, 1, scope.reader())?;

    let first = Uuid::now_v7().to_string();
    scope.set(Some(&first));
    assert_eq!(tenant_on_connection(&pool).await?, first);

    let second = Uuid::now_v7().to_string();
    scope.set(Some(&second));
    assert_eq!(
        tenant_on_connection(&pool).await?,
        second,
        "the physical connection kept the previous request's tenant"
    );
    Ok(())
}

/// Outside any tenant the setting must be empty rather than stale: the policies
/// deny by default on an empty value.
#[tokio::test]
async fn no_tenant_in_scope_leaves_the_setting_empty() -> TestResult {
    let (_container, url) = start_postgres().await?;
    let scope = TenantInScope::default();
    let pool = tenant_pool(&url, 1, scope.reader())?;

    scope.set(Some(&Uuid::now_v7().to_string()));
    tenant_on_connection(&pool).await?;

    scope.set(None);
    assert_eq!(
        tenant_on_connection(&pool).await?,
        "",
        "a request with no tenant inherited one from the connection's past"
    );
    Ok(())
}

/// The payoff, end to end: a real policy on a real table, queried by a role that
/// RLS actually applies to (it is bypassed for superusers and table owners).
#[tokio::test]
async fn row_level_security_scopes_reads_to_the_tenant_in_scope() -> TestResult {
    let (_container, url) = start_postgres().await?;

    let admin = sqlx::PgPool::connect(&url).await?;
    // The setting name is a crate constant, not external input.
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "CREATE TABLE notes (tenant_id uuid NOT NULL, body text NOT NULL);
         ALTER TABLE notes ENABLE ROW LEVEL SECURITY;
         CREATE POLICY tenant_isolation ON notes
             USING      (tenant_id = NULLIF(current_setting('{s}', true), '')::uuid)
             WITH CHECK (tenant_id = NULLIF(current_setting('{s}', true), '')::uuid);
         CREATE ROLE app LOGIN PASSWORD 'app';
         GRANT SELECT, INSERT ON notes TO app;",
        s = TENANT_SETTING
    )))
    .execute(&admin)
    .await?;

    let (alice, bob) = (Uuid::now_v7(), Uuid::now_v7());
    sqlx::query("INSERT INTO notes (tenant_id, body) VALUES ($1, 'a'), ($2, 'b')")
        .bind(alice)
        .bind(bob)
        .execute(&admin)
        .await?;

    // Same URL, unprivileged role — RLS does not apply to the superuser above.
    let app_url = url.replacen("postgres://postgres:postgres@", "postgres://app:app@", 1);
    let scope = TenantInScope::default();
    let pool = tenant_pool(&app_url, 1, scope.reader())?;

    let visible = |pool: Pool| async move {
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM notes")
            .fetch_one(&pool)
            .await
    };

    scope.set(Some(&alice.to_string()));
    assert_eq!(
        visible(pool.clone()).await?,
        1,
        "should see only its own row"
    );

    scope.set(None);
    assert_eq!(
        visible(pool.clone()).await?,
        0,
        "with no tenant in scope the policy must deny by default"
    );
    Ok(())
}
