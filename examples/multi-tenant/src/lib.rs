//! Multi-tenant example for Pharos RS.
//!
//! This example shows how a [`TenantContext`](pharos_app::TenantContext) keeps
//! tenant identity explicit and how one repository instance per tenant gives
//! row-level isolation: data written for one tenant is never visible to
//! another.
//!
//! The domain is deliberately tiny — a `Note` aggregate — so the focus stays on
//! the tenancy wiring. The same `Note` repository type works against an
//! in-memory store (see `main.rs`) and against the tenant-scoped PostgreSQL
//! adapter (see the integration test), both scoped by tenant.

use chrono::{DateTime, Utc};
use pharos_core::{AggregateEvents, AggregateRoot, DomainEvent, Entity};
use serde::{Deserialize, Serialize};

/// Stable aggregate-type discriminator used by the PostgreSQL adapters.
pub const NOTE_AGGREGATE_TYPE: &str = "Note";

/// A note owned by a single tenant.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Note {
    id: String,
    title: String,
    body: String,
    #[serde(default)]
    version: u64,
    #[serde(skip)]
    events: AggregateEvents<NoteEvent>,
}

impl Note {
    /// Creates a new note, raising a [`NoteEvent::Created`] domain event.
    pub fn create(
        id: impl Into<String>,
        title: impl Into<String>,
        body: impl Into<String>,
    ) -> Self {
        let id = id.into();
        let title = title.into();
        let mut events = AggregateEvents::default();
        events.raise(NoteEvent::Created {
            note_id: id.clone(),
            title: title.clone(),
            occurred_at: Utc::now(),
        });
        Self {
            id,
            title,
            body: body.into(),
            version: 0,
            events,
        }
    }

    /// Returns the note title.
    pub fn title(&self) -> &str {
        &self.title
    }

    /// Returns the note body.
    pub fn body(&self) -> &str {
        &self.body
    }
}

impl Entity for Note {
    type Id = String;

    fn id(&self) -> &Self::Id {
        &self.id
    }
}

impl AggregateRoot for Note {
    type Event = NoteEvent;

    fn pending_events(&self) -> &[Self::Event] {
        self.events.pending()
    }

    fn drain_events(&mut self) -> Vec<Self::Event> {
        self.events.drain()
    }

    fn version(&self) -> u64 {
        self.version
    }

    fn set_version(&mut self, version: u64) {
        self.version = version;
    }
}

/// Domain events raised by [`Note`].
#[derive(Debug, Clone)]
pub enum NoteEvent {
    /// A note was created.
    Created {
        /// Note identifier.
        note_id: String,
        /// Note title.
        title: String,
        /// When the note was created.
        occurred_at: DateTime<Utc>,
    },
}

impl DomainEvent for NoteEvent {
    fn event_type(&self) -> &'static str {
        match self {
            NoteEvent::Created { .. } => "NoteCreated",
        }
    }

    fn occurred_at(&self) -> DateTime<Utc> {
        match self {
            NoteEvent::Created { occurred_at, .. } => *occurred_at,
        }
    }

    fn aggregate_id(&self) -> &str {
        match self {
            NoteEvent::Created { note_id, .. } => note_id,
        }
    }
}
