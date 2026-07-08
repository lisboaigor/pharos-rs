//! Runnable multi-tenant demo using in-memory, per-tenant repositories.
//!
//! Each tenant gets its own [`InMemoryRepository`], scoped by a
//! [`TenantContext`]. The same note id is stored for two tenants with
//! different content, and each tenant reads back only its own note — exactly
//! the isolation the PostgreSQL `TenantJsonRepository` provides at the row
//! level (see `tests/tenant_isolation.rs`).

use std::collections::HashMap;

use multi_tenant::Note;
use pharos_app::{TenantContext, TenantId};
use pharos_core::Repository;
use pharos_memory::InMemoryRepository;
use uuid::Uuid;

/// Holds one repository per tenant, keyed by tenant id.
#[derive(Default)]
struct TenantNotes {
    by_tenant: HashMap<TenantId, InMemoryRepository<Note>>,
}

impl TenantNotes {
    /// Returns the repository for a tenant, creating it on first use.
    fn for_tenant(&mut self, tenant: &TenantContext) -> &InMemoryRepository<Note> {
        self.by_tenant.entry(tenant.tenant_id()).or_default()
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // In a real service these arrive at the edge (header, JWT claim) and are
    // parsed once with `TenantContext::parse`.
    let acme = TenantContext::new(Uuid::now_v7());
    let globex = TenantContext::new(Uuid::now_v7());

    let mut notes = TenantNotes::default();

    // Both tenants create a note under the same id.
    let welcome_id = Uuid::now_v7();
    let mut acme_note = Note::create(welcome_id, "Welcome to Acme", "Internal Acme onboarding.");
    notes.for_tenant(&acme).save(&mut acme_note).await?;

    let mut globex_note = Note::create(
        welcome_id,
        "Welcome to Globex",
        "Globex confidential handbook.",
    );
    notes.for_tenant(&globex).save(&mut globex_note).await?;

    // Each tenant reads back only its own note.
    let acme_view = notes
        .for_tenant(&acme)
        .find_by_id(&welcome_id)
        .await?
        .ok_or("acme note not found")?;
    let globex_view = notes
        .for_tenant(&globex)
        .find_by_id(&welcome_id)
        .await?
        .ok_or("globex note not found")?;

    println!("acme   sees: {} — {}", acme_view.title(), acme_view.body());
    println!(
        "globex sees: {} — {}",
        globex_view.title(),
        globex_view.body()
    );

    assert_eq!(acme_view.title(), "Welcome to Acme");
    assert_eq!(globex_view.title(), "Welcome to Globex");
    println!("tenants are isolated: each only sees its own welcome note.");

    Ok(())
}
