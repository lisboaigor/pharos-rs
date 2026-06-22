//! Canonical order example suite for Pharos RS.
//!
//! The binary keeps the first-run experience simple: create an order, add an
//! item, confirm it, persist it, and publish in-process domain events. The
//! integration tests then demonstrate the broader framework seams without
//! overloading `main.rs`.
//!
//! # What this example covers
//!
//! - DDD aggregate modeling with typed UUID v7 identifiers.
//! - CQRS-style command and query handlers.
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
