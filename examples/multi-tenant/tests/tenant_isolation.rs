//! Integration test: row-level tenant isolation against a real PostgreSQL.
//!
//! Requires a running Docker daemon: `cargo test -p multi-tenant`.

use multi_tenant::{NOTE_AGGREGATE_TYPE, Note};
use pharos_app::TenantContext;
use pharos_core::Repository;
use pharos_postgres::{
    TenantJsonRepository, connect_pool, migrate_postgres_tenant_aggregate_schema,
};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::{GenericImage, ImageExt, runners::AsyncRunner};
use uuid::Uuid;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

#[tokio::test]
async fn tenants_cannot_see_each_others_notes() -> TestResult {
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
    let pool = connect_pool(
        &format!("postgres://postgres:postgres@{host}:{port}/postgres"),
        8,
    )?;
    migrate_postgres_tenant_aggregate_schema(&pool).await?;

    let acme = TenantContext::new(Uuid::now_v7());
    let globex = TenantContext::new(Uuid::now_v7());
    let acme_notes = TenantJsonRepository::<Note>::new(pool.clone(), &acme, NOTE_AGGREGATE_TYPE);
    let globex_notes =
        TenantJsonRepository::<Note>::new(pool.clone(), &globex, NOTE_AGGREGATE_TYPE);

    // Same id, two tenants — no conflict, because tenant_id is part of the key.
    let welcome_id = Uuid::now_v7();
    let mut acme_note = Note::create(welcome_id, "Welcome to Acme", "Acme onboarding.");
    acme_notes.save(&mut acme_note).await?;
    let mut globex_note = Note::create(welcome_id, "Welcome to Globex", "Globex handbook.");
    globex_notes.save(&mut globex_note).await?;

    // Each tenant reads only its own row.
    let acme_view = acme_notes.find_by_id(&welcome_id).await?;
    let globex_view = globex_notes.find_by_id(&welcome_id).await?;
    assert_eq!(
        acme_view.map(|n| n.title().to_string()),
        Some("Welcome to Acme".to_string())
    );
    assert_eq!(
        globex_view.map(|n| n.title().to_string()),
        Some("Welcome to Globex".to_string())
    );

    // Deleting Acme's note leaves Globex's intact.
    acme_notes.delete(&welcome_id).await?;
    assert!(acme_notes.find_by_id(&welcome_id).await?.is_none());
    assert!(globex_notes.find_by_id(&welcome_id).await?.is_some());

    Ok(())
}
