//! Canonical order example suite for Pharos RS.
//!
//! The web binary (`cargo run -p order --bin web`) wires the same
//! command/query handlers over axum + tower. The integration tests then
//! demonstrate the broader framework seams.
//!
//! # What this example covers
//!
//! - DDD aggregate modeling with typed UUID v7 identifiers.
//! - CQRS-style command and query handlers.
//! - Exposing those handlers over HTTP with axum + tower (see [`web`] and the
//!   `web` binary, `cargo run -p order --bin web`).
//! - In-process domain event publication.
//! - Atomic aggregate-save-plus-outbox-enqueue through
//!   `TransactionalRepository`/`TransactionalStore`
//!   (`pharos_memory::InMemoryUnitOfWork`) — the same seam you'd implement
//!   against a real database; see `docs/guide/writing-an-adapter.md`.
//! - Mapping domain events to integration events.
//! - JSON serialization, outbox, dispatcher, inbox idempotency,
//!   retry/dead-letter, schema registry, consumer groups, transport, and
//!   observability descriptors through focused tests under
//!   `examples/order/tests`.
//!
//! ```mermaid
//! flowchart TD
//!     Domain[Order aggregate]
//!     App[Command and query handlers]
//!     Bus[In-process EventBus]
//!     Repo[Repository]
//!     Outbox[Outbox seam]
//!     Broker[Messaging adapter]
//!     Consumer[Inbox idempotency]
//!
//!     App --> Domain
//!     App --> Repo
//!     Domain --> Bus
//!     Domain --> Outbox
//!     Outbox --> Broker
//!     Broker --> Consumer
//! ```
//!
//! This crate exposes the example domain and application handlers against
//! `pharos-memory`'s in-process adapters, so integration tests can exercise
//! every framework seam without a container. Swapping in a real database
//! (a SeaORM adapter, or your own) is a change to the composition root, not
//! to any handler or domain type — see `docs/guide/writing-an-adapter.md`.

pub mod application;
pub mod domain;
pub mod web;
