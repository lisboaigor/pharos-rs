//! Pharos RS — DDD, CQRS, and event-driven building blocks for Rust.
//!
//! This is the convenience meta-crate. It re-exports the stable core API so
//! applications can depend on a single `pharos` entry and pull names from the
//! [`prelude`].
//!
//! ```toml
//! pharos = { version = "0.1", features = ["macros"] }
//! ```
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

/// Commonly used types, re-exported for `use pharos::prelude::*;`.
pub mod prelude {
    pub use pharos_app::{
        ApplicationError, BackoffStrategy, Command, CommandHandler, DispatchConfig, DispatchResult,
        EventBus, EventHandler, IntegrationEvent, Message, MessageCodec, OutboxDispatcher, Query,
        QueryHandler, RetryPolicy, TenantContext, save_and_enqueue, save_and_publish,
    };
    pub use pharos_core::{
        AggregateEvents, AggregateRoot, DomainError, DomainEvent, DomainResult, Entity, Repository,
        RepositoryError, ValueObject,
    };

    #[cfg(feature = "macros")]
    pub use pharos_macros::{AggregateRoot, DomainEvent, Entity, id_type};
}
