# Writing an adapter

Pharos RS ships no database, broker, or ORM dependency. What it ships instead
is a small set of traits — `Repository`, `TransactionalStore` +
`TransactionalRepository`, `OutboxRepository`, `InboxStore`,
`DeadLetterQueue`, `SagaStore`, `EventStore`/`SnapshotStore`,
`MessagePublisher`/`MessageConsumer`/`MessageAcknowledger`,
`ConsumerGroupCoordinator`, `SchemaRegistry` — plus, for local development
and tests, in-memory implementations of every one of them in `pharos-memory`.
Production storage is yours to wire in.

This guide walks through implementing the two traits every application
needs first — `Repository<A>` and the `TransactionalStore` +
`TransactionalRepository<A, S>` pair — against [SeaORM] 2.0, then shows how
to verify the result with `pharos-testing`'s conformance kit instead of
hoping it's correct. The same shape applies to any other ORM or hand-rolled
SQL layer; SeaORM is the example because it's async, has first-class
Postgres/MySQL/SQLite support, and its `DatabaseTransaction` maps directly
onto `TransactionalStore::Tx`.

[SeaORM]: https://www.sea-ql.org/SeaORM/

If you'd rather see the shape of the tables these traits typically sit on
top of before writing Rust, read `reference-schema.sql` in this directory
first — it's the schema `pharos-postgres` used to create for you, kept as
copyable reference with the non-obvious decisions (why `seq`, not
`created_at`, orders an outbox claim; why a saga's `save` is a
compare-and-swap; why `SELECT ... FOR UPDATE SKIP LOCKED` is load-bearing)
explained inline.

## What `Repository<A>` asks of you

```rust,ignore
pub trait Repository<A: AggregateRoot>: Send + Sync + 'static {
    type Error: Error + Send + Sync + 'static;

    async fn find_by_id(&self, id: &A::Id) -> Result<Option<A>, Self::Error>;
    async fn save(&self, aggregate: &mut A) -> Result<(), RepositoryError<Self::Error>>;
    async fn delete(&self, id: &A::Id) -> Result<(), Self::Error>;
}
```

(This is `#[trait_variant::make(Send)]`-generated in the actual source —
you get plain `async fn` to implement against; the `+ Send` bound on each
method's future is added automatically, which is what lets `Repository<A>`
be used by code generic over the trait itself, like
`DefaultAggregateStore` below.)

The one rule everything else follows from: `save` enforces **optimistic
concurrency**. `aggregate.version()` is the version the caller loaded (or
`0` for a never-saved aggregate); `save` must only mutate storage when the
stored version still matches, and must refuse with
`RepositoryError::ConcurrencyConflict { expected, actual }` — leaving
storage untouched — whenever it doesn't. On success it advances
`aggregate`'s in-memory version to what it just persisted.

### The entity and a JSONB-shaped table

A SeaORM entity for the JSONB repository shape from `reference-schema.sql`:

```rust,ignore
// entities/pharos_aggregates.rs — generated once with `sea-orm-cli generate
// entity`, or hand-written; either way it's an ordinary SeaORM entity, with
// nothing Pharos-specific about it.
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "pharos_aggregates")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub aggregate_type: String,
    #[sea_orm(primary_key, auto_increment = false)]
    pub aggregate_id: String,
    pub payload: Json,
    pub version: i64,
    pub updated_at: DateTimeUtc,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
```

### The repository

```rust,ignore
use pharos_core::{AggregateRoot, Entity, Repository, RepositoryError};
use sea_orm::{ActiveValue::Set, ConnectionTrait, DbErr, EntityTrait};
use serde::{Serialize, de::DeserializeOwned};
use std::marker::PhantomData;

pub struct SeaOrmRepository<A> {
    db: sea_orm::DatabaseConnection,
    aggregate_type: &'static str,
    _marker: PhantomData<fn() -> A>,
}

impl<A> SeaOrmRepository<A> {
    pub fn new(db: sea_orm::DatabaseConnection, aggregate_type: &'static str) -> Self {
        Self { db, aggregate_type, _marker: PhantomData }
    }
}

impl<A> Repository<A> for SeaOrmRepository<A>
where
    A: AggregateRoot + Serialize + DeserializeOwned + Send + Sync + 'static,
    A::Id: ToString + Send + Sync,
{
    type Error = DbErr;

    async fn find_by_id(&self, id: &A::Id) -> Result<Option<A>, Self::Error> {
        let row = pharos_aggregates::Entity::find_by_id((
            self.aggregate_type.to_string(),
            id.to_string(),
        ))
        .one(&self.db)
        .await?;
        Ok(row.map(|m| serde_json::from_value(m.payload).expect("stored payload is valid")))
    }

    async fn save(&self, aggregate: &mut A) -> Result<(), RepositoryError<Self::Error>> {
        let expected = aggregate.version();
        let new_version = expected + 1;
        let payload = serde_json::to_value(&*aggregate).map_err(|e| {
            RepositoryError::Storage(DbErr::Custom(e.to_string()))
        })?;

        let affected = if expected == 0 {
            // Create: `INSERT ... ON CONFLICT DO NOTHING`, SeaORM's
            // `insert` won't give you the "0 rows" signal on conflict by
            // itself, so use `Statement::from_sql_and_values` against the
            // exact query in `reference-schema.sql`'s "Aggregate
            // repository" section, or a raw exec through `ConnectionTrait`.
            self.db.execute(insert_on_conflict_do_nothing(
                self.aggregate_type,
                &aggregate.id().to_string(),
                &payload,
                new_version,
            )).await.map_err(RepositoryError::Storage)?.rows_affected()
        } else {
            self.db.execute(update_where_version(
                self.aggregate_type,
                &aggregate.id().to_string(),
                &payload,
                new_version,
                expected,
            )).await.map_err(RepositoryError::Storage)?.rows_affected()
        };

        if affected == 0 {
            let actual = self.stored_version(&aggregate.id().to_string()).await
                .map_err(RepositoryError::Storage)?;
            return Err(RepositoryError::ConcurrencyConflict { expected, actual });
        }
        aggregate.set_version(new_version);
        Ok(())
    }

    async fn delete(&self, id: &A::Id) -> Result<(), Self::Error> {
        pharos_aggregates::Entity::delete_by_id((self.aggregate_type.to_string(), id.to_string()))
            .exec(&self.db)
            .await?;
        Ok(())
    }
}
```

The compare-and-swap itself (`insert_on_conflict_do_nothing`,
`update_where_version`, `stored_version`) is exactly the SQL in
`reference-schema.sql`'s first section — SeaORM's query builder doesn't have
a first-class "tell me how many rows an UPSERT actually touched" API, so
this is one of the few places reaching for `Statement::from_sql_and_values`
(raw SQL through `ConnectionTrait`, still inside SeaORM's connection
pooling and transaction machinery) is the pragmatic choice over the entity
API. This is the entire adapter-specific surface; everything above it —
`CommandHandler`s, `DefaultAggregateStore`, `save_and_publish` — never
changes.

## What `TransactionalStore` + `TransactionalRepository` ask of you

Plain `Repository::save` is enough until you need the aggregate save and an
outbox insert to commit atomically — see `save_and_enqueue_in` in
`pharos-app`. That composition needs two more traits:

```rust,ignore
pub trait TransactionalStore: Send + Sync {
    type Tx: Send;
    type Error: Error + Send + Sync + 'static;

    async fn begin(&self) -> Result<Self::Tx, Self::Error>;
    async fn commit(&self, tx: Self::Tx) -> Result<(), Self::Error>;
    async fn insert_outbox_in_tx<'a>(&'a self, tx: &'a mut Self::Tx, message: &'a OutboxMessage)
        -> Result<(), Self::Error>;
}

pub trait TransactionalRepository<A, Store: TransactionalStore>: Send + Sync {
    type Error: Error + Send + Sync + 'static;

    async fn save_in_tx<'c>(&'c self, tx: &'c mut Store::Tx, aggregate: &'c mut A)
        -> Result<(), RepositoryError<Self::Error>>;
}
```

(Both traits are declared with `#[trait_variant::make(Send)]` and plain
`async fn` in the actual framework source — shown here without the
attribute for readability; see `writing an adapter`'s note above on
`Send`, or `pharos-core`'s own `Repository` trait for why.)

`Tx` is SeaORM's own `DatabaseTransaction` — an owned, `'static` handle,
exactly what the trait wants:

```rust,ignore
use pharos_app::unit_of_work::{TransactionalRepository, TransactionalStore};
use pharos_app::OutboxMessage;
use sea_orm::{DatabaseConnection, DatabaseTransaction, DbErr, TransactionTrait};

pub struct SeaOrmUnitOfWork {
    db: DatabaseConnection,
}

impl TransactionalStore for SeaOrmUnitOfWork {
    type Tx = DatabaseTransaction;
    type Error = DbErr;

    async fn begin(&self) -> Result<Self::Tx, Self::Error> {
        self.db.begin().await
    }

    async fn commit(&self, tx: Self::Tx) -> Result<(), Self::Error> {
        tx.commit().await
        // Dropping `tx` without calling `commit` rolls back — SeaORM's
        // `DatabaseTransaction::drop` issues `ROLLBACK` automatically, so
        // every early return on the caller's error path (the `?` in
        // `save_and_enqueue_in`) already does the right thing for free.
    }

    async fn insert_outbox_in_tx<'a>(
        &'a self,
        tx: &'a mut Self::Tx,
        message: &'a OutboxMessage,
    ) -> Result<(), Self::Error> {
        pharos_outbox::ActiveModel::from(message).insert(tx).await?;
        Ok(())
    }
}

impl<A> TransactionalRepository<A, SeaOrmUnitOfWork> for SeaOrmRepository<A>
where
    A: pharos_core::AggregateRoot + Serialize + DeserializeOwned + Send + Sync + 'static,
    A::Id: ToString + Send + Sync,
{
    type Error = DbErr;

    async fn save_in_tx<'c>(
        &'c self,
        tx: &'c mut DatabaseTransaction,
        aggregate: &'c mut A,
    ) -> Result<(), RepositoryError<Self::Error>> {
        // Same compare-and-swap as `Repository::save` above, run against
        // `tx` instead of `&self.db` — the only difference is which
        // connection/transaction handle the query executes on.
        # todo!()
    }
}
```

This is why `pharos-memory`'s `InMemoryUnitOfWork` implements `Repository`,
`TransactionalStore`, and `TransactionalRepository` all on one type sharing
one `DashMap`: a real backend's two halves — the plain repository and its
transactional counterpart — are backed by the *same* connection pool for
exactly the same reason. Nothing requires you to split them into two types
the way the sketch above does; one struct implementing all three traits
against the same `DatabaseConnection`/pool is equally valid and often
simpler.

## Wiring it into `DefaultAggregateStore`

Once both traits are implemented, `pharos-app`'s `DefaultAggregateStore`
gives you delivery-mode switching (in-process vs. durable outbox), message
enrichment, and the atomic save+enqueue seam for free — this is the same
type `pharos-postgres`'s `PostgresAggregateStore` used to be, generalized:

```rust,ignore
let repo = SeaOrmRepository::<Order>::new(db.clone(), "Order");
let store = SeaOrmUnitOfWork { db: db.clone() };

// In-process delivery:
let orders = DefaultAggregateStore::new(repo.clone(), store.clone(), bus.clone());

// Durable outbox delivery:
let orders = DefaultAggregateStore::new(repo, store, bus)
    .with_outbox("OrderEvent", Some(signal))
    .with_enricher(Arc::new(TenantHeader));
```

## Proving it's correct

Don't hand-write these tests yourself — `pharos-testing`'s `contract`
feature is the framework's own experience with every non-obvious failure
mode above (a lost-update race, a double-claimed outbox message, a saga
`save` that upserts unconditionally) turned into something you run:

```toml
[dev-dependencies]
pharos-testing = { version = "0.6", features = ["contract"] }
```

```rust,ignore
#[tokio::test]
async fn repository_contract() -> Result<(), Box<dyn std::error::Error>> {
    let repo = SeaOrmRepository::<pharos_testing::contract::ContractAggregate>::new(
        db.clone(), "ContractAggregate",
    );
    pharos_testing::contract::repository::run(&repo).await
}

#[tokio::test]
async fn transactional_contract() -> Result<(), Box<dyn std::error::Error>> {
    let repo = SeaOrmRepository::<pharos_testing::contract::ContractAggregate>::new(
        db.clone(), "ContractAggregate",
    );
    let store = SeaOrmUnitOfWork { db: db.clone() };
    pharos_testing::contract::transactional::transactional_repository(&repo, &store).await
}
```

Run these against a real database (a `testcontainers`-launched Postgres, or
whatever your test harness already spins up) — the conformance kit's
concurrency assertions (`outbox::no_double_claim`,
`saga::timeout`'s lease-exclusivity check) only mean something against real
transaction isolation, not against a mock. `pharos-testing`'s own test
suite (`crates/pharos-testing/tests/contract_kit.rs`) is the reference: it
runs every suite against `pharos-memory`'s adapters, which is exactly the
pattern above with the SeaORM types swapped for in-memory ones.

A suite failing tells you precisely which guarantee your adapter doesn't
hold yet — the assertion message names it (`"a rejected save must not
mutate the stored state"`, `"two concurrent pending calls must claim the
message exactly once"`) instead of leaving you to infer it from a stack
trace days later, under load, in production.

## Extending beyond the aggregate store

The same pattern — implement the trait, run the matching
`pharos_testing::contract` suite — covers every other seam an application
eventually needs:

| Need | Trait | Conformance suite |
| --- | --- | --- |
| Durable outbox | `OutboxRepository` | `contract::outbox::{run, no_double_claim}` |
| Idempotent consumers | `InboxStore` | `contract::inbox::run` |
| Parked failures | `DeadLetterQueue` | `contract::dead_letter::run` |
| Long-running workflows | `SagaStore` + `SagaTimeoutStore` | `contract::saga::{run, timeout}` |
| Event sourcing | `EventStore` + `SnapshotStore` | `contract::event_store::{run, snapshot_store}` |
| Broker messaging | `MessagePublisher`/`MessageConsumer`/`MessageAcknowledger` | `contract::messaging::{publish_and_consume, acknowledger}` |
| Kafka-style consumer groups | `ConsumerGroupCoordinator` | `contract::messaging::consumer_group` |
| Versioned integration events | `SchemaRegistry` | `contract::schema_registry::run` |

None of these require any of the others — a CQRS application with no event
sourcing implements only `Repository`/`TransactionalStore`; a single-process
application with no broker never touches `MessagePublisher` at all. Add each
one only when the application actually needs it, and let its conformance
suite tell you when the implementation is done.
