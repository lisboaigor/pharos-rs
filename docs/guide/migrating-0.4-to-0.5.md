# Migrating from 0.4 to 0.5

0.5.0 closes out a framework-wide correctness audit. Most of it is
non-breaking (new methods, new optional behavior, bug fixes), but five
changes touch public API shape. This page covers only those five — see the
commit log for the full set of fixes.

## 1. The transactional seam moved from `pharos-postgres` to `pharos-app`

`pharos_postgres::{TransactionalRepository, SaveAndEnqueueError, save_and_enqueue_in}`
are gone. The same names now live in `pharos_app`, backed by a new
`pharos_app::TransactionalStore` trait instead of a bare `Pool`:

```rust
// before
use pharos_postgres::{save_and_enqueue_in, TransactionalRepository};
save_and_enqueue_in(&pool, &repo, &mut aggregate, map_event).await?;

// after
use pharos_app::{save_and_enqueue_in, TransactionalRepository, TransactionalStore};
use pharos_postgres::PostgresUnitOfWork;
let store = PostgresUnitOfWork::new(pool.clone());
save_and_enqueue_in(&store, &repo, &mut aggregate, map_event).await?;
```

If you implemented `TransactionalRepository<A>` for a custom repository,
switch to `TransactionalRepository<A, PostgresUnitOfWork>` and rename the
`tx` parameter's type from `&mut sqlx::Transaction<'_, Postgres>` to
`&mut <PostgresUnitOfWork as TransactionalStore>::Tx<'_>` (the same
concrete type today, but named through the trait). See
`examples/order/src/infrastructure/postgres_order_repository.rs` for a
worked example, and [the persistence ladder](persistence-ladder.md) for why
the seam is backend-agnostic now.

## 2. `pharos-app`'s messaging integration is now feature-gated

The `messaging` feature (default-on) gates `pharos_app::resilience`, the
outbox/inbox re-exports, `save_and_enqueue`, and
`ApplicationError::Outbox`. If you depend on `pharos-app` with
`default-features = false`, add `messaging` explicitly:

```toml
pharos-app = { path = "...", default-features = false, features = ["messaging"] }
```

Consumers on default features are unaffected.

## 3. `EventUpcaster` is now `EventUpcasterRegistry`

The single-hop `EventUpcaster` trait is gone. `EventUpcasterRegistry` is a
chainable, version-keyed registry built with `with_upcaster`, and it
rejects both a gap in the chain and a payload newer than any known
upcaster instead of silently passing either through:

```rust
// before
struct MyUpcaster;
impl EventUpcaster for MyUpcaster {
    fn upcast(&self, payload: &mut Value) { /* mutate in place */ }
}

// after
let registry = EventUpcasterRegistry::new()
    .with_upcaster("LedgerEntryPosted", 0, |mut payload| {
        // payload at schema_version 0 -> 1
        Ok::<_, std::convert::Infallible>(payload)
    });
```

Pair this with `#[event(schema_version = N)]` on your `#[derive(DomainEvent)]`
event so appended rows carry the version the registry keys on.

## 4. `SagaStore::save` now enforces optimistic concurrency

```rust
// before
async fn save(&self, instance: SagaInstance<I, S>) -> Result<(), Self::Error>;

// after
async fn save(&self, instance: SagaInstance<I, S>) -> Result<(), SagaSaveError<Self::Error>>;
```

`SagaInstance` gained a `version: u64` field. A custom `SagaStore`
implementation must:

- treat `instance.version == 0` as "never persisted" — create the row, or
  return `SagaSaveError::ConcurrencyConflict` if one already exists;
- otherwise update only when the stored version still equals
  `instance.version`, storing `instance.version + 1`, and return
  `SagaSaveError::ConcurrencyConflict` if it doesn't.

An implementation that keeps upserting unconditionally compiles (with a
`SagaSaveError::Storage` wrapper around the old error) but reintroduces the
lost-update race this change exists to close. See
`crates/pharos-postgres/src/saga_store.rs` for the compare-and-swap
reference implementation, and `SagaRunner::MAX_CONFLICT_RETRIES` for how the
runner retries a lost race automatically.

## 5. `ProcessOutcome` gained a variant

`process_idempotent`/`process_idempotent_with_retry` can now return
`ProcessOutcome::StillProcessing` (another consumer holds the message's
lease; it was nacked and requeued rather than dropped). An exhaustive
`match` on `ProcessOutcome` needs a new arm.

## Everything else

Non-breaking: `allocate_minor_units`, `DomainEvent::schema_version`,
`ProcessProfile` (read-only router typestate), bounded in-memory caches in
`pharos-redis`/`pharos-realtime`, credential redaction in `pharos-kafka`/
`pharos-redis` `Debug` impls, `pharos-init --minimal`, and a handful of
correctness fixes (NATS publish now waits for `flush`, the
`INTERNAL_ONLY` command guard now also covers `CommandHandlerState::dispatch`,
outbox/inbox timestamps and ordering are now DB-clock-sourced end to end).
None of these require call-site changes.
