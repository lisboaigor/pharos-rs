//! Runnable multi-tenant demo using in-memory, per-tenant repositories.
//!
//! Each tenant gets its own [`InMemoryRepository`], scoped by a
//! [`TenantContext`]. The same note id (`"welcome"`) is stored for two tenants
//! with different content, and each tenant reads back only its own note —
//! exactly the isolation the PostgreSQL `TenantJsonRepository` provides at the
//! row level (see `tests/tenant_isolation.rs`).

use std::collections::HashMap;

use multi_tenant::Note;
use pharos_app::TenantContext;
use pharos_core::Repository;
use pharos_infra::InMemoryRepository;

/// Holds one repository per tenant, keyed by tenant id.
#[derive(Default)]
struct TenantNotes {
    by_tenant: HashMap<String, InMemoryRepository<Note>>,
}

impl TenantNotes {
    /// Returns the repository for a tenant, creating it on first use.
    fn for_tenant(&mut self, tenant: &TenantContext) -> &InMemoryRepository<Note> {
        self.by_tenant
            .entry(tenant.tenant_id().to_string())
            .or_default()
    }
}

#[tokio::main]
async fn main() {
    let acme = TenantContext::new("acme");
    let globex = TenantContext::new("globex");

    let mut notes = TenantNotes::default();

    // Both tenants create a note under the same id.
    let mut acme_note = Note::create("welcome", "Welcome to Acme", "Internal Acme onboarding.");
    notes
        .for_tenant(&acme)
        .save(&mut acme_note)
        .await
        .expect("save acme note");

    let mut globex_note = Note::create(
        "welcome",
        "Welcome to Globex",
        "Globex confidential handbook.",
    );
    notes
        .for_tenant(&globex)
        .save(&mut globex_note)
        .await
        .expect("save globex note");

    // Each tenant reads back only its own note.
    let acme_view = notes
        .for_tenant(&acme)
        .find_by_id(&"welcome".to_string())
        .await
        .expect("read acme note")
        .expect("acme note exists");
    let globex_view = notes
        .for_tenant(&globex)
        .find_by_id(&"welcome".to_string())
        .await
        .expect("read globex note")
        .expect("globex note exists");

    println!("acme   sees: {} — {}", acme_view.title(), acme_view.body());
    println!(
        "globex sees: {} — {}",
        globex_view.title(),
        globex_view.body()
    );

    assert_eq!(acme_view.title(), "Welcome to Acme");
    assert_eq!(globex_view.title(), "Welcome to Globex");
    println!("tenants are isolated: each only sees its own 'welcome' note.");
}
