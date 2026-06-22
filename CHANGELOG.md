# Changelog

All notable changes to Pharos RS are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/), and the project aims to follow
[Semantic Versioning](https://semver.org/).

## [Unreleased]

This release reworks the core APIs for correctness and ergonomics. It contains
breaking changes; see **Migration** below.

### Added

- **Opinionated feature bundles** on the `pharos` meta-crate: `starter`
  (`macros` + `infra` + `postgres` + `axum` + `tower`, the recommended
  PostgreSQL outbox + HTTP path) and `full` (every adapter). Removes
  flag-by-flag setup for the common cases.
- **Ergonomics documentation**: `docs/guide/decision-matrix.md` (which path to
  use for persistence, event delivery, and transport), `docs/guide/cookbook.md`
  (copy-paste templates for command handlers, transactional save + enqueue,
  idempotent consumers, tenant propagation, and HTTP routes), and
  `docs/guide/pitfalls.md` (common mistakes and fixes). All three are linked
  from `docs/README.md` and the published mdbook site.
- **Optimistic concurrency control.** `AggregateRoot` now exposes `version()` /
  `set_version()`, derived from a `#[version] version: u64` field.
  `Repository::save` enforces the expected version and returns
  `RepositoryError::ConcurrencyConflict` on a stale write, preventing lost
  updates.
- **`ApplicationError`** — a concrete error returned by `save_and_publish` /
  `save_and_enqueue`, so callers can match on failures (including
  `ConcurrencyConflict`) instead of inspecting a boxed error.
- **`BackoffStrategy`** with exponential backoff and jitter for `RetryPolicy`,
  alongside the existing fixed delay.
- **`DispatchResult`** returned by `OutboxDispatcher::dispatch_pending`, reporting
  how many messages were published and collecting per-message errors.
- **`pharos-testing`** crate with `EventCapture` and the
  `assert_event_published!` macro for asserting on published domain events.
- **`pharos`** meta-crate that re-exports the workspace behind feature flags
  (`macros`, `infra`, `postgres`, `redis`, `testing`, `tower`) and ships a
  `prelude`.
- **`pharos-postgres`** crate: the PostgreSQL outbox, inbox, and JSON aggregate
  repository, now backed by a pooled connection (`sqlx::PgPool`) instead of
  a single serialized connection. Build a pool with `connect_pool(url, max_size)`
  and share it (it is cheap to clone) across every adapter.
- **`pharos-redis`** crate: the Redis list-backed messaging adapter, split out of
  `pharos-infra`.
- **Tower adapters** (`pharos-app`, `tower` feature): `CommandHandlerService` and
  `QueryHandlerService` expose handlers as `tower::Service`s so Tower middleware
  (timeouts, rate/concurrency limits, retries) can compose around them.
- **`PostgresUnitOfWork`** — a real transactional unit of work that threads a
  PostgreSQL transaction into a work closure, committing on success and rolling
  back on error.
- **Atomic aggregate save + outbox insert** via `save_aggregate_and_enqueue`
  (and the composable `save_aggregate_in_tx` / `insert_outbox_in_tx`): the
  aggregate write (with optimistic concurrency) and every outbox row commit in
  one transaction.
- **Configurable outbox dispatcher.** `DispatchConfig` adds a batch size and a
  `RetryPolicy`; failed publishes stay `pending` until their retry budget is
  exhausted, then move to `failed` for dead-lettering. `dispatch_batch()` runs a
  configured batch.
- **Row-level multi-tenancy.** `TenantContext` (in `pharos-app`) and
  `TenantJsonRepository` (in `pharos-postgres`) scope every read and write by
  `tenant_id`, so one tenant can never see or overwrite another's rows.
- **New examples:** `multi-tenant` (tenant isolation) and `modular-monolith`
  (multiple bounded contexts in one process, wired through the event bus).
- **Production guide** at `docs/guide/production.md` with a deployment checklist.
- **Versioned PostgreSQL migration history** under
  `crates/pharos-postgres/migrations/` covering eventing, aggregate, tenant,
  and dead-letter schemas.
- **Operational guides** for JSONB rollback
  (`docs/guide/jsonb-schema-rollback.md`) and benchmark baselines
  (`docs/guide/benchmarks.md`).
- **`pharos-axum`** crate with typed Axum extractors/helpers for exposing
  `CommandHandler`s and `QueryHandler`s over HTTP.
- **`pharos-saga`** crate with `Saga`, `SagaStore`, `CommandDispatcher`, and
  `SagaRunner` for process-manager style workflows.
- **`pharos-es`** crate with `EventStore`, `SnapshotStore`, `StoredEvent`, and
  `EventSourcedRepository` for event-sourced aggregates.
- **`pharos-kafka`** crate with Kafka messaging adapters and remote schema
  registry adapters for Confluent-compatible registries and Apicurio.
- **`pharos-nats`** crate with core NATS messaging adapters.
- **PostgreSQL dead-letter queue** via `PostgresDeadLetterQueue`.
- **Dedicated docs site source** under `docs/site/`, plus a new 30-minute
  tutorial at `docs/guide/30-minutes.md`.
- **Starter workspace template** under `templates/workspace/`.
- **Documented RFC process** under `docs/rfc/` and `.github/ISSUE_TEMPLATE/rfc.md`.
- **Container-backed test execution.** The Docker integration tests are a
  first-class part of the suite (never `#[ignore]`d), so `cargo test` requires a
  running Docker daemon; CI runs them on every push and pull request.
- Dual licensing under **MIT OR Apache-2.0**.

### Changed

- **`EventBus` is now a concrete, cloneable struct** (in `pharos-app`) instead of
  a trait object. Publishing is fully typed — `bus.publish(&event)` — with no
  `Any`/`TypeId` leaking into the domain.
- **`DomainEvent::aggregate_id()` returns `&str`** instead of `String`, removing a
  heap allocation per published event. The `#[aggregate_id]` field must be
  string-like (`String`/`&str`).
- **`Repository::save` takes `&mut A`** and returns
  `Result<(), RepositoryError<Self::Error>>`.
- PostgreSQL outbox `pending` query now uses `FOR UPDATE SKIP LOCKED` so multiple
  dispatcher workers can claim disjoint batches.
- PostgreSQL inbox `begin_processing` now uses a single atomic
  `INSERT ... ON CONFLICT ... RETURNING`, closing the previous select-then-insert
  race.
- `OutboxDispatcher::dispatch_pending` no longer aborts the batch on the first
  failure; it processes every fetched message and collects errors.
- `PostgresJsonRepository` now requires an explicit, stable aggregate-type
  discriminator (`with_aggregate_type`); the `new` constructor that derived it
  from `type_name` was removed to avoid silent data orphaning on refactors.

### Removed

- `pharos-core::AsAny` and the `InProcessEventBus` adapter (superseded by the
  concrete `EventBus`).
- `Default` for `id_type!`-generated IDs (a random-UUID `Default` is a footgun
  during deserialization). Construct IDs with `new()` / `from_uuid(...)`.

## Migration

- **Aggregates:** add a `#[version] version: u64` field and initialize it to `0`
  in your factory; pass it through rehydration constructors.
- **Saving:** change `repo.save(&aggregate)` to `repo.save(&mut aggregate)`.
- **Event bus:** replace `Arc<dyn EventBus>` / `InProcessEventBus` with the
  concrete `EventBus`; it is `Clone`, so pass it by value.
- **Events:** make the `#[aggregate_id]` field a `String`; update any code that
  relied on `aggregate_id()` returning an owned `String` (it now returns `&str`).
- **Error handling:** `save_and_publish` / `save_and_enqueue` now return
  `ApplicationError`; match it instead of a `Box<dyn Error>`.
- **IDs:** replace `Id::default()` with `Id::new()` or `Id::from_uuid(...)`.
- **PostgreSQL JSON repository:** replace `PostgresJsonRepository::new(client)`
  with `PostgresJsonRepository::with_aggregate_type(client, "YourStableName")`,
  and add the `version BIGINT NOT NULL` column (see the bundled schema).
