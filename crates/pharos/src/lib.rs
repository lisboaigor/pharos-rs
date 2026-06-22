//! Pharos RS — DDD, CQRS, and event-driven building blocks for Rust.
//!
//! This is the convenience meta-crate. It re-exports the focused workspace
//! crates so applications can depend on a single `pharos` entry while still
//! opting into only what they need through feature flags.
//!
//! | Module | Crate | Feature |
//! | --- | --- | --- |
//! | [`core`] | `pharos-core` | always |
//! | [`app`] | `pharos-app` | always |
//! | [`macros`] | `pharos-macros` | `macros` (default) |
//! | [`infra`] | `pharos-infra` | `infra` (default) |
//! | [`postgres`] | `pharos-postgres` | `postgres` |
//! | [`redis`] | `pharos-redis` | `redis` |
//! | [`axum`] | `pharos-axum` | `axum` |
//! | [`saga`] | `pharos-saga` | `saga` |
//! | [`es`] | `pharos-es` | `es` |
//! | [`kafka`] | `pharos-kafka` | `kafka` |
//! | [`nats`] | `pharos-nats` | `nats` |
//! | [`testing`] | `pharos-testing` | `testing` |
//!
//! # Choosing features
//!
//! If you do not want to pick flags one by one, use a bundle:
//!
//! - `starter` — the recommended default path: PostgreSQL outbox + HTTP
//!   transport (`macros`, `infra`, `postgres`, `axum`, `tower`). Start here for
//!   the common production setup.
//! - `full` — every adapter Pharos ships, for exploration or workspaces that
//!   touch many seams.
//!
//! ```toml
//! pharos = { version = "0.1", features = ["starter"] }
//! ```
//!
//! See `docs/guide/decision-matrix.md` for choosing between persistence, event
//! delivery, and transport options.
//!
//! Most code should pull names from the [`prelude`]:
//!
//! ```
//! use pharos::prelude::*;
//! ```

/// Domain primitives: [`Entity`](pharos_core::Entity),
/// [`AggregateRoot`](pharos_core::AggregateRoot),
/// [`DomainEvent`](pharos_core::DomainEvent), and the
/// [`Repository`](pharos_core::Repository) boundary.
pub use pharos_core as core;

/// Application contracts: commands, queries, the in-process
/// [`EventBus`](pharos_app::EventBus), outbox/inbox seams, and messaging.
pub use pharos_app as app;

/// Derive macros and `id_type!` for reducing domain boilerplate.
#[cfg(feature = "macros")]
pub use pharos_macros as macros;

/// In-memory adapters for the application contracts.
#[cfg(feature = "infra")]
pub use pharos_infra as infra;

/// Pooled PostgreSQL adapters: outbox, inbox, and JSON aggregate repository.
#[cfg(feature = "postgres")]
pub use pharos_postgres as postgres;

/// Redis list-backed messaging adapter.
#[cfg(feature = "redis")]
pub use pharos_redis as redis;

/// Axum extractors and helpers for HTTP-facing command/query routes.
#[cfg(feature = "axum")]
pub use pharos_axum as axum;

/// Saga and process-manager primitives.
#[cfg(feature = "saga")]
pub use pharos_saga as saga;

/// Event-sourcing primitives and repository adapters.
#[cfg(feature = "es")]
pub use pharos_es as es;

/// Kafka messaging and schema-registry adapters.
#[cfg(feature = "kafka")]
pub use pharos_kafka as kafka;

/// NATS messaging adapters.
#[cfg(feature = "nats")]
pub use pharos_nats as nats;

/// Test helpers such as [`EventCapture`](pharos_testing::EventCapture).
#[cfg(feature = "testing")]
pub use pharos_testing as testing;

/// Commonly used types, re-exported for `use pharos::prelude::*;`.
pub mod prelude {
    pub use pharos_app::{
        ApplicationError, BackoffStrategy, Command, CommandHandler, DispatchConfig, DispatchResult,
        EventBus, EventHandler, IntegrationEvent, Message, OutboxDispatcher, Query, QueryHandler,
        RetryPolicy, TenantContext, save_and_enqueue, save_and_publish,
    };
    pub use pharos_core::{
        AggregateEvents, AggregateRoot, DomainError, DomainEvent, DomainResult, Entity, Repository,
        RepositoryError, ValueObject,
    };

    #[cfg(feature = "macros")]
    pub use pharos_macros::{AggregateRoot, DomainEvent, Entity, id_type};
}
