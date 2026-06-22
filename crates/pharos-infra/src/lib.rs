//! In-memory infrastructure adapters for Pharos RS.
//!
//! `pharos-infra` provides ready-to-use, dependency-free implementations of the
//! contracts from `pharos-core` and `pharos-app`. These adapters keep all state
//! in process, which makes them ideal for unit tests, examples, and local
//! development.
//!
//! In-process event dispatch is provided directly by [`pharos_app::EventBus`],
//! a concrete type, so this crate does not ship a separate event-bus adapter.
//!
//! Database- and broker-backed adapters live in dedicated crates:
//!
//! - `pharos-postgres` — pooled PostgreSQL outbox, inbox, and JSON repository.
//! - `pharos-redis` — Redis list-backed messaging.
//!
//! # Included adapters
//!
//! | Adapter | Backing technology | Implements |
//! | --- | --- | --- |
//! | [`InMemoryRepository`] | `DashMap` | `Repository` |
//! | [`InMemoryMessageBroker`] | In-memory queues | messaging traits |
//! | [`InMemoryOutboxRepository`] | `DashMap` | `OutboxRepository` |
//! | [`InMemoryInboxStore`] | `DashMap` | `InboxStore` |
//! | [`InMemoryDeadLetterQueue`] | In-memory | `DeadLetterQueue` |
//! | [`InMemorySchemaRegistry`] | In-memory | `SchemaRegistry` |
//! | [`InMemoryConsumerGroupCoordinator`] | In-memory | `ConsumerGroupCoordinator` |
//!
//! # Adapter map
//!
//! ```mermaid
//! flowchart TD
//!     App[pharos-app contracts]
//!     Repository[Repository]
//!     Outbox[OutboxRepository]
//!     Inbox[InboxStore]
//!     Messaging[MessagePublisher/Consumer/Acknowledger]
//!
//!     InMemoryRepo[InMemoryRepository]
//!     InMemoryOutbox[InMemoryOutboxRepository]
//!     InMemoryInbox[InMemoryInboxStore]
//!     InMemoryBroker[InMemoryMessageBroker]
//!
//!     App --> Repository
//!     App --> Outbox
//!     App --> Inbox
//!     App --> Messaging
//!
//!     Repository --> InMemoryRepo
//!     Outbox --> InMemoryOutbox
//!     Inbox --> InMemoryInbox
//!     Messaging --> InMemoryBroker
//! ```

pub mod in_memory_consumer_group;
pub mod in_memory_dead_letter;
pub mod in_memory_inbox;
pub mod in_memory_messaging;
pub mod in_memory_outbox;
pub mod in_memory_repository;
pub mod in_memory_schema_registry;

#[cfg(test)]
mod outbox_dispatcher_tests;

pub use in_memory_consumer_group::InMemoryConsumerGroupCoordinator;
pub use in_memory_dead_letter::InMemoryDeadLetterQueue;
pub use in_memory_inbox::InMemoryInboxStore;
pub use in_memory_messaging::InMemoryMessageBroker;
pub use in_memory_outbox::InMemoryOutboxRepository;
pub use in_memory_repository::{InMemoryRepoError, InMemoryRepository};
pub use in_memory_schema_registry::InMemorySchemaRegistry;
