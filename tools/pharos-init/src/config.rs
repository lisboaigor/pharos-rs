use console::style;

use std::path::PathBuf;

use crate::prompt::{ask, ask_with_default, select};

// ── top-level intent that the user picks ──────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub enum SystemKind {
    /// One process, one bounded context, no inter-service events.
    SingleService,
    /// One process, multiple bounded contexts wired through an in-process bus.
    ModularMonolith,
    /// Multiple services that react to each other's events through a durable outbox.
    EventDriven,
    /// High-throughput pipelines: strict schemas, binary protocol, ordered partitions.
    HighThroughput,
}

// ── derived technical choices (never shown to the user) ───────────────────────

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq)]
pub enum Persistence {
    InMemory,
    PostgresJson,
    PostgresRelational,
    PostgresTenant,
}

#[derive(Debug, Clone, PartialEq)]
pub enum EventDelivery {
    InProcess,
    Outbox,
    AtomicOutbox,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Broker {
    Redis,
    Kafka,
    None,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Serialization {
    Json,
    Protobuf,
    None,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Http {
    Axum,
    None,
}

// ── public config ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct ProjectConfig {
    pub project_name: String,
    pub context_name: String,
    /// Absolute path to the parent directory where the project folder will be created.
    pub location: PathBuf,
    #[allow(dead_code)]
    pub kind: SystemKind,
    // ── all fields below are auto-derived; never ask the user about them ──
    pub persistence: Persistence,
    pub event_delivery: EventDelivery,
    pub broker: Broker,
    pub serialization: Serialization,
    pub http: Http,
}

impl ProjectConfig {
    pub fn aggregate(&self) -> String {
        to_pascal(&self.context_name)
    }
    pub fn module(&self) -> String {
        to_snake(&self.context_name)
    }

    /// Full path where the project folder will be written.
    pub fn output_path(&self) -> PathBuf {
        self.location.join(&self.project_name)
    }

    pub fn uses_postgres(&self) -> bool {
        matches!(
            self.persistence,
            Persistence::PostgresJson
                | Persistence::PostgresRelational
                | Persistence::PostgresTenant
        )
    }
    pub fn uses_infra(&self) -> bool {
        matches!(self.persistence, Persistence::InMemory)
    }
    pub fn uses_outbox(&self) -> bool {
        matches!(
            self.event_delivery,
            EventDelivery::Outbox | EventDelivery::AtomicOutbox
        )
    }
    pub fn uses_axum(&self) -> bool {
        matches!(self.http, Http::Axum)
    }
    pub fn uses_proto(&self) -> bool {
        matches!(self.serialization, Serialization::Protobuf)
    }
    pub fn uses_redis(&self) -> bool {
        matches!(self.broker, Broker::Redis)
    }
    pub fn uses_kafka(&self) -> bool {
        matches!(self.broker, Broker::Kafka)
    }

    /// Human-readable summary of auto-derived choices (for the success screen).
    pub fn summary(&self) -> Vec<(&'static str, String)> {
        let mut rows = Vec::new();

        let persistence_label = match self.persistence {
            Persistence::InMemory => "in-memory (no persistence)",
            Persistence::PostgresJson => "persistent, document-per-aggregate",
            Persistence::PostgresRelational => "persistent, relational schema",
            Persistence::PostgresTenant => "persistent, tenant-isolated",
        };
        rows.push(("State", persistence_label.into()));

        let delivery_label = match self.event_delivery {
            EventDelivery::InProcess => "in-process (same process, synchronous handlers)",
            EventDelivery::Outbox => "durable outbox → message queue",
            EventDelivery::AtomicOutbox => "atomic state + outbox in one transaction",
        };
        rows.push(("Events", delivery_label.into()));

        if !matches!(self.broker, Broker::None) {
            let broker_label = match self.broker {
                Broker::Redis => "message queue",
                Broker::Kafka => "partitioned message stream",
                Broker::None => unreachable!(),
            };
            rows.push(("Transport", broker_label.into()));
        }

        if !matches!(self.serialization, Serialization::None) {
            let serial_label = match self.serialization {
                Serialization::Json => "JSON",
                Serialization::Protobuf => "Protobuf  (binary, schema-safe)",
                Serialization::None => unreachable!(),
            };
            rows.push(("Wire format", serial_label.into()));
        }

        let http_label = match self.http {
            Http::Axum => "HTTP API server",
            Http::None => "background worker / CLI",
        };
        rows.push(("Interface", http_label.into()));

        rows
    }
}

// ── interactive collection — 3 questions only ─────────────────────────────────

pub fn collect() -> Result<ProjectConfig, dialoguer::Error> {
    let project_name = ask("Project name")?;
    let context_name = ask("Main bounded context  (e.g. order, player, tournament)")?;

    let cwd = std::env::current_dir()
        .map(|p| p.to_string_lossy().into_owned())
        .unwrap_or_else(|_| ".".to_string());
    let location_str = ask_with_default("Location", &cwd)?;
    let location = expand_tilde(&location_str);

    // ── Q1: what kind of system? ──────────────────────────────────────────────
    println!();
    println!(
        "  {}",
        style("What kind of system are you building?").bold()
    );
    println!();

    let kind = match select(
        "",
        &[
            "A single service                     one bounded context, straightforward",
            "A modular monolith                   bounded contexts in one process, in-process events",
            "Services that react to each other    reliable cross-service event delivery",
            "A high-throughput pipeline           strict schemas, ordered partitions, binary protocol",
        ],
    )? {
        0 => SystemKind::SingleService,
        1 => SystemKind::ModularMonolith,
        2 => SystemKind::EventDriven,
        _ => SystemKind::HighThroughput,
    };

    // ── Q2: HTTP API? ─────────────────────────────────────────────────────────
    println!();
    println!("  {}", style("How does it receive work?").bold());
    println!();

    let serves_http = select(
        "",
        &[
            "It exposes an HTTP API",
            "It runs as a background worker or CLI",
        ],
    )? == 0;

    // ── Q3: persistent storage? (only meaningful for single/modular) ──────────
    let stores_data = match kind {
        // event-driven and high-throughput always need durable storage
        SystemKind::EventDriven | SystemKind::HighThroughput => true,
        _ => {
            println!();
            println!(
                "  {}",
                style("Does it need to remember state between restarts?").bold()
            );
            println!();
            select(
                "",
                &["No  —  in-memory is enough", "Yes  —  it stores data"],
            )? == 1
        }
    };

    // ── derive all technical choices ──────────────────────────────────────────
    let (persistence, event_delivery, broker, serialization) = match kind {
        SystemKind::SingleService | SystemKind::ModularMonolith => (
            if stores_data {
                Persistence::PostgresJson
            } else {
                Persistence::InMemory
            },
            EventDelivery::InProcess,
            Broker::None,
            if serves_http {
                Serialization::Json
            } else {
                Serialization::None
            },
        ),
        SystemKind::EventDriven => (
            Persistence::PostgresJson,
            EventDelivery::Outbox,
            Broker::Redis,
            Serialization::Json,
        ),
        SystemKind::HighThroughput => (
            Persistence::PostgresJson,
            EventDelivery::AtomicOutbox,
            Broker::Kafka,
            Serialization::Protobuf,
        ),
    };

    let http = if serves_http { Http::Axum } else { Http::None };

    Ok(ProjectConfig {
        project_name,
        context_name,
        location,
        kind,
        persistence,
        event_delivery,
        broker,
        serialization,
        http,
    })
}

// ── path helpers ──────────────────────────────────────────────────────────────

fn expand_tilde(path: &str) -> PathBuf {
    if let Some(rest) = path.strip_prefix("~/") {
        if let Some(home) = std::env::var_os("HOME") {
            return PathBuf::from(home).join(rest);
        }
    } else if path == "~"
        && let Some(home) = std::env::var_os("HOME")
    {
        return PathBuf::from(home);
    }
    PathBuf::from(path)
}

// ── name transforms ───────────────────────────────────────────────────────────

fn to_pascal(s: &str) -> String {
    s.split(['-', '_', ' '])
        .map(|w| {
            let mut c = w.chars();
            match c.next() {
                None => String::new(),
                Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
            }
        })
        .collect()
}

fn to_snake(s: &str) -> String {
    let kebab = s.replace('-', "_");
    let mut out = String::new();
    for (i, ch) in kebab.char_indices() {
        if ch.is_uppercase() && i != 0 {
            out.push('_');
        }
        out.push(ch.to_ascii_lowercase());
    }
    out
}
