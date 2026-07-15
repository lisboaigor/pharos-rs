//! Pooled PostgreSQL adapters for Pharos RS.
//!
//! `pharos-postgres` provides production-oriented PostgreSQL implementations of
//! the `pharos-app` and `pharos-core` contracts, all sharing a bounded
//! connection [`Pool`] instead of a single serialized connection.
//!
//! # Included adapters
//!
//! | Adapter | Implements |
//! | --- | --- |
//! | [`PostgresOutboxRepository`] | `OutboxRepository` |
//! | [`PostgresInboxStore`] | `InboxStore` |
//! | [`PostgresDeadLetterQueue`] | `DeadLetterQueue` |
//! | [`PostgresJsonRepository`] | `Repository` |
//! | [`PostgresUnitOfWork`] | transactional boundary |
//! | [`PgEventStore`] / [`PgSnapshotStore`] | `EventStore` / `SnapshotStore` (`pharos-es`) |
//! | [`PgSagaStore`] | `SagaStore` + `SagaTimeoutStore` (`pharos-saga`) |
//!
//! For an aggregate save and its outbox inserts that must commit together, use
//! [`save_aggregate_and_enqueue`] (or compose [`save_aggregate_in_tx`] and
//! [`insert_outbox_in_tx`] inside your own [`PostgresUnitOfWork::transaction`]).
//!
//! # Getting started
//!
//! Build a [`Pool`] once and share it (it is cheap to clone) across every
//! adapter:
//!
//! ```no_run
//! use pharos_postgres::{PostgresOutboxRepository, connect_pool};
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let pool = connect_pool("host=localhost user=postgres dbname=app", 16)?;
//! let outbox = PostgresOutboxRepository::new(pool.clone());
//! outbox.migrate().await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Schema
//!
//! Install [`POSTGRES_EVENTING_SCHEMA`] (outbox/inbox),
//! [`POSTGRES_DEAD_LETTER_SCHEMA`] (dead-letter queue), and
//! [`POSTGRES_AGGREGATE_SCHEMA`] (JSON repository), or translate them into your
//! migration tool for production usage.

mod dead_letter;
mod event_store;
mod eventing;
mod json_repository;
mod pool;
mod saga_store;
mod tenant_repository;
mod transaction;

pub use dead_letter::{
    POSTGRES_DEAD_LETTER_SCHEMA, PostgresDeadLetterQueue, migrate_postgres_dead_letter_schema,
};
pub use event_store::{
    POSTGRES_EVENT_STORE_SCHEMA, PgEventStore, PgSnapshotStore, PostgresEventStoreError,
    migrate_postgres_event_store_schema,
};
pub use eventing::{
    POSTGRES_EVENTING_SCHEMA, PostgresInboxStore, PostgresOutboxRepository,
    migrate_postgres_eventing_schema,
};
pub use json_repository::{
    POSTGRES_AGGREGATE_SCHEMA, PostgresJsonRepository, PostgresRepositoryError,
    migrate_postgres_aggregate_schema,
};
pub use pool::{PgPoolError, Pool, connect_pool};
pub use saga_store::{
    POSTGRES_SAGA_SCHEMA, PgSagaStore, PostgresSagaStoreError, migrate_postgres_saga_schema,
};
pub use tenant_repository::{
    POSTGRES_TENANT_AGGREGATE_SCHEMA, TenantJsonRepository,
    migrate_postgres_tenant_aggregate_schema,
};
pub use transaction::{
    PostgresTransactionError, PostgresUnitOfWork, SaveAndEnqueueError, TransactionalRepository,
    insert_outbox_in_tx, save_aggregate_and_enqueue, save_aggregate_in_tx, save_and_enqueue_in,
};
