//! Core domain primitives for Pharos RS.
//!
//! `pharos-core` contains the framework's domain-only abstractions. It has no
//! dependency on application services, infrastructure adapters, brokers, or
//! databases. This keeps domain models portable and easy to test.
//!
//! # Main concepts
//!
//! - [`Entity`]: a domain object with stable identity.
//! - [`AggregateRoot`]: an entity that owns consistency boundaries and pending
//!   domain events.
//! - [`AggregateEvents`]: a small buffer used by aggregates to raise and drain
//!   events.
//! - [`DomainEvent`]: an immutable fact that happened in the domain.
//! - [`Repository`]: persistence boundary for aggregate roots.
//! - [`ValueObject`]: marker for immutable value objects.
//!
//! # Relationship between core concepts
//!
//! ```mermaid
//! classDiagram
//!     class Entity {
//!         type Id
//!         id() Id
//!     }
//!
//!     class AggregateRoot {
//!         type Event
//!         pending_events()
//!         drain_events()
//!     }
//!
//!     class AggregateEvents~E~ {
//!         raise(event)
//!         pending()
//!         drain()
//!     }
//!
//!     class DomainEvent {
//!         event_type()
//!         occurred_at()
//!         aggregate_id()
//!     }
//!
//!     class Repository~A~ {
//!         find_by_id(id)
//!         save(aggregate)
//!         delete(id)
//!     }
//!
//!     Entity <|-- AggregateRoot
//!     AggregateRoot --> DomainEvent
//!     AggregateRoot --> AggregateEvents
//!     Repository --> AggregateRoot
//! ```
//!
//! # Domain event correlation
//!
//! [`DomainEvent`] intentionally exposes the fields needed by observability and
//! integration layers: event type, occurrence time, and aggregate id. Higher
//! layers use these values to create tracing spans, integration event envelopes,
//! and outbox messages.

pub mod aggregate;
pub mod domain_event;
pub mod entity;
pub mod errors;
pub mod repository;
pub mod value_object;

pub use aggregate::{AggregateEvents, AggregateRoot};
pub use domain_event::DomainEvent;
pub use entity::Entity;
pub use errors::{DomainError, DomainResult};
pub use repository::{Repository, RepositoryError};
pub use value_object::ValueObject;
