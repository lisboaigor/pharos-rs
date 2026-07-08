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
//!
//! # Using the derives through the facade
//!
//! The derive macros inspect the calling crate's `Cargo.toml` to decide which
//! paths to emit: a direct dependency on `pharos-core`/`pharos-app` wins, and
//! a facade-only crate is routed through this crate's `core`/`app` re-exports
//! automatically. Depending on `pharos` alone is therefore enough:
//!
//! ```
//! use pharos::prelude::*;
//!
//! #[derive(Debug, Clone, Entity)]
//! struct Customer {
//!     #[id]
//!     id: u64,
//! }
//!
//! # fn main() {}
//! ```

/// Domain primitives: [`Entity`](pharos_core::Entity),
/// [`AggregateRoot`](pharos_core::AggregateRoot),
/// [`DomainEvent`](pharos_core::DomainEvent), and the
/// [`Repository`](pharos_core::Repository) boundary.
pub use pharos_core as core;

/// Application contracts: commands, queries, the in-process
/// [`EventBus`](pharos_app::EventBus), outbox/inbox seams, and messaging.
pub use pharos_app as app;

/// Broker-facing messaging contracts: messages, publishers/consumers, retry,
/// outbox, inbox, dead-letter, consumer groups, and schema registry.
/// (Also reachable through [`app`], which re-exports everything here.)
pub use pharos_messaging as messaging;

/// Derive macros and `id_type!` for reducing domain boilerplate.
#[cfg(feature = "macros")]
pub use pharos_macros as macros;

/// Commonly used types, re-exported for `use pharos::prelude::*;`.
pub mod prelude {
    pub use pharos_app::{
        ApplicationError, BackoffStrategy, CausationId, Command, CommandHandler, CorrelationId,
        DispatchConfig, DispatchError, DispatchResult, EventBus, EventHandler, FieldViolation,
        IntegrationEvent, Message, MessageCodec, OutboxDispatcher, PublishErrorPolicy, Query,
        QueryHandler, RetryPolicy, TenantContext, ValidationError, dispatch, query_dispatch,
        republish_pending, save_and_enqueue, save_and_publish,
    };
    pub use pharos_core::{
        AggregateEvents, AggregateRoot, DomainError, DomainEvent, DomainResult, Entity, Repository,
        RepositoryError, ValueObject,
    };

    #[cfg(feature = "macros")]
    pub use pharos_macros::{AggregateRoot, Command, DomainEvent, Entity, Query, id_type};
}
