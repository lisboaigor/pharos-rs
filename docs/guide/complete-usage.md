# Complete usage guide

This guide walks through the full lifecycle of a service built with Pharos RS:

1. model the domain;
2. handle commands and queries;
3. persist aggregates;
4. publish and consume events safely;
5. run with production controls.

## 1. Choose dependencies

For most applications, start with the meta-crate:

```toml
[dependencies]
pharos = { version = "0.1", features = ["macros", "infra"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
serde = { version = "1", features = ["derive"] }
chrono = { version = "0.4", features = ["serde"] }
```

Enable optional integrations as needed:

- PostgreSQL adapters: `pharos` feature `postgres`
- Redis messaging: `pharos` feature `redis`
- Axum helpers: `pharos` feature `axum`
- Saga/process manager: `pharos` feature `saga`
- Event sourcing primitives: `pharos` feature `es`
- Kafka or NATS: `pharos` features `kafka` / `nats`

## 2. Model an aggregate with OCC

```rust
use chrono::{DateTime, Utc};
use pharos::prelude::*;

id_type!(OrderId);

#[derive(Debug, Clone, DomainEvent)]
pub struct OrderPlaced {
    #[aggregate_id]
    order_id: String,
    occurred_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Entity, AggregateRoot)]
pub struct Order {
    #[id]
    id: OrderId,
    #[version]
    version: u64,
    #[events]
    events: AggregateEvents<OrderPlaced>,
    status: String,
}
```

Key rule: always include `#[version] version: u64` and save aggregates through
`Repository::save(&mut aggregate)` to enforce optimistic concurrency.

## 3. Wire application handlers

- Commands and queries live in `pharos-app` contracts.
- Keep handlers focused on orchestration, not business rules.
- Keep business rules in aggregate methods.

Pattern:

1. load aggregate from repository;
2. execute aggregate behavior;
3. persist and publish (`save_and_publish`) or enqueue (`save_and_enqueue`).

## 4. Pick your persistence path

Use in-memory for tests and local exploration:

- `InMemoryRepository`
- `InMemoryOutboxRepository`
- `InMemoryInboxStore`

Use PostgreSQL in production:

- `connect_pool(url, max_size)`
- `PostgresJsonRepository` / `TenantJsonRepository`
- `PostgresOutboxRepository`
- `PostgresInboxStore`
- `PostgresDeadLetterQueue`

Apply schema via versioned SQL history under:

- `crates/pharos-postgres/migrations/`

## 5. Outbox and delivery guarantees

For reliability, prefer transactional aggregate save + outbox insert using
`save_aggregate_and_enqueue` (or the lower-level `save_aggregate_in_tx` and
`insert_outbox_in_tx`).

Run `OutboxDispatcher` in a background loop with `DispatchConfig`.

Important behavior already built in:

- concurrent workers claim disjoint rows using `FOR UPDATE SKIP LOCKED`;
- one failed message does not abort the whole batch;
- retry policy supports exponential backoff with jitter;
- exhausted retries can be dead-lettered.

## 6. Idempotent consumers

Wrap message handling with inbox checks:

1. call `begin_processing`;
2. process only if decision allows work;
3. mark completed or failed.

This avoids duplicate side effects in at-least-once delivery paths.

## 7. Multi-tenant flows

For tenant isolation:

- use `TenantContext` at the request edge;
- propagate it through the application service;
- build tenant-scoped repositories with `TenantJsonRepository`.

A full running reference is available in `examples/multi-tenant`.

## 8. Observability and operations

- Install your own `tracing` subscriber in the binary.
- Install your own metrics backend.
- Use correlation and causation ids in integration events.
- Follow `guide/observability.md` for OTLP wiring.

Operational references:

- `guide/production.md`
- `guide/jsonb-schema-rollback.md`
- `guide/benchmarks.md`

## 9. Recommended project layout

```text
src/
  domain/
  application/
  infrastructure/
  interfaces/
```

Suggested mapping:

- domain: aggregates, entities, value objects, domain events
- application: command/query handlers, mappers, policies
- infrastructure: repositories, brokers, adapters
- interfaces: HTTP, CLI, consumers, schedulers

## 10. Validation gate before merge

Run the same quality gate used in CI:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features
cargo test --workspace --all-features -- --test-threads=1
```

## 11. Where to look for complete examples

- canonical order flow: `examples/order`
- multi-tenant isolation: `examples/multi-tenant`
- multi-context composition: `examples/modular-monolith`

Use these examples as executable documentation for the intended composition style.
