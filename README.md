<div align="center">

# Pharos RS

<img src="assets/pharos-cover.png" alt="Pharos RS — a lightweight Rust framework for domain-driven, CQRS-friendly, event-driven applications" width="100%" />

</div>

Pharos RS is a lightweight Rust framework for building domain-driven, CQRS-friendly, event-driven applications.

The public API is intentionally small: a domain core, an application-contract crate, and a convenience facade that reexports the stable surface so users can get started quickly without learning every crate at once.

## Highlights

- Domain modeling primitives: `Entity`, `AggregateRoot`, `ValueObject`, `DomainEvent`
- Validated value objects via `value_object!`; strongly typed UUID v7 IDs via `id_type!`
- `Money`/`Currency` in `i128` minor units — checked arithmetic, lossless allocation, crypto magnitudes (wei) included
- Command/query handlers with validation + tracing applied by the `dispatch` seam
- Internal-only commands (`#[command(internal)]`): the HTTP entry points refuse to route them, keeping saga-issued payouts/refunds off the wire while in-process dispatch still works
- Repository abstraction with optimistic concurrency control
- Atomic aggregate save + outbox in one transaction (`pharos_app::{TransactionalStore, TransactionalRepository, save_and_enqueue_in}`) — backend-agnostic (the trait and the composing function never name a concrete connection type); `pharos-memory`'s `InMemoryUnitOfWork` is the shipped `TransactionalStore` implementation for tests, production needs your own (see [Writing an adapter](docs/guide/writing-an-adapter.md))
- In-process domain event bus with configurable error policy, retry, and dead-letter decorators
- Integration event envelope with typed correlation/causation, tenant, trace and schema metadata
- Schema evolution through JSON upcasters (`VersionedJsonCodec`)
- Outbox dispatcher with per-key ordered concurrency and a failed→DLQ sweep
- Idempotent consumers in one call (`process_idempotent`)
- Durable event sourcing and sagas on PostgreSQL (`PgEventStore`, `PgSnapshotStore`, `PgSagaStore`)
- Saga deadlines: schedule timeouts on `Start`/`Advance` and sweep them with `SagaRunner::run_due_timeouts`
- Saga commands go through a durable outbox, not a direct dispatcher call (`SagaRunner` + `DurableCommandPublisher` + `pharos_messaging::OutboxDispatcher`): `SagaTransition::Fail` carries follow-up `commands`, enqueued right after the terminal transition is saved — the save and the enqueue are still two separate steps, not one transaction, so a crash between them still drops the compensation, but once enqueued a command is durable and retried on delivery failure instead of being lost on the first failed dispatch
- Cross-context sagas: `SagaRunner::handle_any` folds events from several bounded contexts into one saga instance without changing the `Saga` trait
- Tower as the cross-cutting pipeline seam (timeouts, limits, authorization)
- Observability with `tracing` spans and `metrics` counters throughout

## Crate Guide

| Crate | Purpose |
| ----- | ------- |
| `pharos-core` | Domain primitives: entities, aggregates, repositories, value objects, and domain events |
| `pharos-app` | Application contracts: command/query handlers, event bus, integration events, upcasters |
| `pharos-messaging` | Broker-facing contracts: messages, publishers/consumers, retry, outbox/inbox, DLQ |
| `pharos-macros` | Derive macros and `id_type!` for reducing boilerplate |
| `pharos` | Convenience facade that reexports the stable API and prelude |

## Architecture at a glance

```mermaid
flowchart TD
    Domain[Domain Model]
    Core[pharos-core]
    Macros[pharos-macros]
    App[pharos-app]
    Example[examples/order]

    Domain --> Core
    Macros --> Core
    App --> Core
    Example --> Core
    Example --> App

    Core --> Entity[Entity]
    Core --> Aggregate[AggregateRoot]
    Core --> DomainEvent[DomainEvent]
    Core --> Repository[Repository]

    App --> CQRS[Command and Query Handlers]
    App --> Eventing[Event Bus and Handlers]
    App --> Outbox[Outbox and Inbox Contracts]
    App --> Messaging[Messaging Contracts]
```

## Workspace layout

```text
pharos-rs/
├── crates/
│   ├── pharos-core      # domain primitives
│   ├── pharos-macros    # derive macros + id_type!
│   ├── pharos-messaging # broker contracts, retry, outbox/inbox, DLQ, consumer groups
│   ├── pharos-app       # CQRS, EventBus, integration events, upcasters, Tower adapters
│   ├── pharos-memory    # in-memory adapters for tests and local development
│   ├── pharos-axum      # Axum extractors/helpers for handlers
│   ├── pharos-saga      # saga/process-manager primitives
│   ├── pharos-es        # event sourcing primitives
│   ├── pharos-proto     # Protobuf binary serialization for integration events
│   ├── pharos-testing   # EventCapture, test helpers, and the adapter conformance kit
│   └── pharos           # convenience meta-crate (re-exports + prelude)
└── examples/
    ├── order
    ├── multi-tenant
    └── modular-monolith
```

`pharos-rs` ships no database, broker, or ORM dependency — only the traits
above and `pharos-memory`'s in-process implementations of them, for tests and
local development. Bringing your own storage is a matter of implementing a
couple of traits and proving them correct with `pharos-testing`'s conformance
kit; see [`docs/guide/writing-an-adapter.md`](docs/guide/writing-an-adapter.md)
(SeaORM as the running example) and
[`docs/guide/reference-schema.sql`](docs/guide/reference-schema.sql) (a
copyable PostgreSQL schema to start from).

## Getting started

### Manual setup

Most applications should start with the `pharos` facade and import from its prelude:

> Pharos RS is not published to crates.io (every crate sets `publish = false`)
> and is currently at 0.6.0, pre-1.0. Depend on it by git revision and pin a
> commit — there is no semver resolution or docs.rs to fall back on. Coming
> from 0.4? See the [0.4→0.5 migration guide](docs/guide/migrating-0.4-to-0.5.md).
> Pinned to a commit before the storage/broker crates and `pharos-init` were
> removed? See the [0.6→0.7 migration guide](docs/guide/migrating-0.6-to-0.7.md).

```toml
pharos = { git = "https://github.com/lisboaigor/pharos-rs", rev = "<commit>", features = ["macros"] }
```

```rust
use pharos::prelude::*;
```

If you want lower-level control, depend on `pharos-core`, `pharos-app`, or `pharos-macros` directly.

### Feature flags

| Feature (crate)                | Default | Enables                                                        |
| ------------------------------ | ------- | -------------------------------------------------------------- |
| `macros` (`pharos`)            | yes     | `#[derive(...)]` and `id_type!` (`pharos-macros`)              |
| `tower` (`pharos-app`)         | no      | `CommandHandlerService`/`QueryHandlerService` (pipeline seam)  |
| `retry` (`pharos-app`)         | no      | `Retrying` event-handler decorator (Tokio timer)               |
| `tenant-task-local` (`pharos-app`) | no  | `CURRENT_TENANT` task-local (explicit `TenantContext` is canonical) |

## Documentation

- [Documentation index](docs/README.md)
- [30-minute tutorial](docs/guide/30-minutes.md)
- [Complete usage guide](docs/guide/complete-usage.md)
- [Architecture guide](docs/guide/architecture.md) — event-driven model, outbox, inbox, patterns
- [Crate reference](docs/guide/crates.md) — per-crate API tables
- [Production operations](docs/guide/production.md)
- [Observability setup](docs/guide/observability.md)
- [Decision matrix](docs/guide/decision-matrix.md)
- [Cookbook](docs/guide/cookbook.md)
- [Pitfalls](docs/guide/pitfalls.md)

## Examples

| Example                     | Shows                                                                                              |
| --------------------------- | -------------------------------------------------------------------------------------------------- |
| `examples/order`            | Canonical DDD/CQRS/outbox suite — run with `cargo run -p order`                                    |
| `examples/multi-tenant`     | `TenantContext` + per-tenant repositories and row-level isolation — `cargo run -p multi-tenant`    |
| `examples/modular-monolith` | Two bounded contexts in one process via the in-process event bus — `cargo run -p modular-monolith` |

## Commands

```sh
# build
cargo build --workspace

# test
cargo test --workspace --all-features

# docs
cargo docs   # alias for: cargo doc --workspace --no-deps
```

## Design principles

- Explicit rather than magical
- Framework-light
- Idiomatic Rust
- Compatible with DDD and CQRS patterns
- Extensible through traits and adapters
- Useful for modular monoliths and as a foundation for distributed event-driven systems

## Acknowledgements

Pharos RS stands on top of the Rust ecosystem. Thanks to the maintainers and contributors of these third-party libraries used directly across this workspace:

- [axum](https://crates.io/crates/axum)
- [chrono](https://crates.io/crates/chrono)
- [criterion](https://crates.io/crates/criterion)
- [dashmap](https://crates.io/crates/dashmap)
- [futures](https://crates.io/crates/futures)
- [garde](https://crates.io/crates/garde)
- [http](https://crates.io/crates/http)
- [indoc](https://crates.io/crates/indoc)
- [metrics](https://crates.io/crates/metrics)
- [proc-macro2](https://crates.io/crates/proc-macro2)
- [prost](https://crates.io/crates/prost)
- [quote](https://crates.io/crates/quote)
- [serde](https://crates.io/crates/serde)
- [serde_json](https://crates.io/crates/serde_json)
- [syn](https://crates.io/crates/syn)
- [thiserror](https://crates.io/crates/thiserror)
- [tokio](https://crates.io/crates/tokio)
- [tower](https://crates.io/crates/tower)
- [trait-variant](https://crates.io/crates/trait-variant)
- [tracing](https://crates.io/crates/tracing)
- [tracing-subscriber](https://crates.io/crates/tracing-subscriber)
- [uuid](https://crates.io/crates/uuid)

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or [MIT license](LICENSE-MIT) at your option.
