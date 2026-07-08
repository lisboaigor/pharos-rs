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
//! - Explicit relational PostgreSQL persistence for a normalized order schema.
//! - Mapping domain events to integration events.
//! - JSON serialization, outbox, dispatcher, Redis messaging, inbox
//!   idempotency, retry/dead-letter, schema registry, consumer groups,
//!   unit-of-work, transport, and observability descriptors through focused
//!   tests under `examples/order/tests`.
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
//! This crate exposes the example domain, application handlers, and explicit
//! infrastructure adapters so integration tests can exercise real persistence
//! without turning the framework into an ORM.

pub mod application;
pub mod domain;
pub mod infrastructure;
pub mod web;
