# Pharos RS

Pharos RS is a lightweight Rust framework for building domain-driven, CQRS-friendly, event-driven applications.

It started as a clean DDD/domain-events foundation and now includes the core pieces needed to evolve toward distributed event-driven systems: integration event envelopes, outbox/inbox contracts, broker-facing traits, default PostgreSQL and Redis adapters, structured tracing, metrics hooks, and UUID v7 identifiers.

## Highlights

- Domain modeling primitives: `Entity`, `AggregateRoot`, `ValueObject`, `DomainEvent`
- Command/query handler contracts
- Repository abstraction
- In-process domain event bus
- Integration event envelope with correlation, causation, trace and schema metadata
- Outbox pattern contracts and dispatcher
- Inbox/idempotency contracts
- Broker-facing publisher/consumer/acknowledger traits
- JSON event serialization
- Default PostgreSQL adapters for outbox, inbox/idempotency and JSON aggregate persistence
- Explicit relational PostgreSQL repository example for normalized aggregate persistence
- Default Redis adapter for broker-like message flows
- Unit-of-work, schema registry, dead-letter queue, consumer group and transport contracts
- In-memory adapters for tests, examples and local development
- Observability with `tracing` and metrics counters through `metrics`
- Strongly typed UUID v7 IDs through `id_type!`

## Documentation

- Documentation index: [`docs/README.md`](docs/README.md)
- Complete usage guide: [`docs/guide/complete-usage.md`](docs/guide/complete-usage.md)
- Production operations guide: [`docs/guide/production.md`](docs/guide/production.md)
- Observability setup: [`docs/guide/observability.md`](docs/guide/observability.md)
- Ergonomics review: [`docs/guide/ergonomics-review.md`](docs/guide/ergonomics-review.md)

## Architecture at a glance

```mermaid
flowchart TD
    Domain[Domain Model]
    Core[pharos-core]
    Macros[pharos-macros]
    App[pharos-app]
    Infra[pharos-infra]
    Postgres[pharos-postgres]
    Redis[pharos-redis]
    Example[examples/order]

    Domain --> Core
    Macros --> Core
    App --> Core
    Infra --> App
    Infra --> Core
    Postgres --> App
    Postgres --> Core
    Redis --> App
    Example --> Core
    Example --> App
    Example --> Infra
    Example --> Postgres

    Core --> Entity[Entity]
    Core --> Aggregate[AggregateRoot]
    Core --> DomainEvent[DomainEvent]
    Core --> Repository[Repository]

    App --> CQRS[Command and Query Handlers]
    App --> Eventing[Event Bus and Handlers]
    App --> Outbox[Outbox and Inbox Contracts]
    App --> Messaging[Messaging Contracts]
    App --> Serialization[Event Serialization]

    Infra --> InMemory[In-memory Adapters]
    Postgres --> PgAdapters[Pooled Outbox, Inbox, JSON Repository]
    Redis --> RedisMsg[Redis Messaging]
```

## Workspace layout

```text
pharos-rs/
├── crates/
│   ├── pharos-core      # domain primitives
│   ├── pharos-macros    # derive macros + id_type!
│   ├── pharos-app       # CQRS, EventBus, outbox/inbox, messaging, Tower adapters
│   ├── pharos-infra     # in-memory adapters
│   ├── pharos-postgres  # pooled PostgreSQL outbox, inbox, JSON repository
│   ├── pharos-redis     # Redis messaging adapter
│   ├── pharos-axum      # Axum extractors/helpers for handlers
│   ├── pharos-saga      # saga/process-manager primitives
│   ├── pharos-es        # event sourcing primitives
│   ├── pharos-kafka     # Kafka + schema registry adapters
│   ├── pharos-nats      # NATS messaging adapters
│   ├── pharos-testing   # EventCapture and test helpers
│   └── pharos           # convenience meta-crate (re-exports + prelude)
└── examples/
    ├── order
    ├── multi-tenant
    └── modular-monolith
```

Most applications depend on the single `pharos` meta-crate and import from its
prelude:

```rust
use pharos::prelude::*;
```

Feature flags on `pharos`:

| Feature    | Default | Enables                                                  |
|------------|---------|----------------------------------------------------------|
| `macros`   | yes     | `#[derive(...)]` and `id_type!` (`pharos-macros`)        |
| `infra`    | yes     | in-memory adapters (`pharos-infra`)                      |
| `postgres` | no      | pooled PostgreSQL adapters (`pharos-postgres`)           |
| `redis`    | no      | Redis messaging (`pharos-redis`)                         |
| `axum`     | no      | Axum extractors/helpers (`pharos-axum`)                  |
| `saga`     | no      | saga/process-manager primitives (`pharos-saga`)          |
| `es`       | no      | event sourcing primitives (`pharos-es`)                  |
| `kafka`    | no      | Kafka + schema registry adapters (`pharos-kafka`)        |
| `nats`     | no      | NATS messaging adapters (`pharos-nats`)                  |
| `tower`    | no      | `tower::Service` command/query adapters                  |
| `testing`  | no      | `EventCapture` and helpers (`pharos-testing`), for tests |

Prefer a bundle over choosing flags one by one:

| Bundle    | Enables                                                                                                 |
|-----------|---------------------------------------------------------------------------------------------------------|
| `starter` | recommended default path: `macros` + `infra` + `postgres` + `axum` + `tower` (PostgreSQL outbox + HTTP) |
| `full`    | every adapter Pharos ships, for exploration                                                             |

```toml
pharos = { version = "0.1", features = ["starter"] }
```

See `docs/guide/decision-matrix.md` to choose between persistence, event
delivery, and transport options.

## Crates

### `pharos-core`

Core domain primitives used by the entire framework:

| Type                           | Purpose                                                       |
|--------------------------------|---------------------------------------------------------------|
| `Entity`                       | Stable identity for domain objects                            |
| `AggregateRoot`                | Aggregate boundary with pending events and OCC `version()`    |
| `AggregateEvents<E>`           | Small event buffer for aggregates                             |
| `DomainEvent`                  | Immutable fact with type, timestamp and aggregate correlation |
| `Repository<A>`                | Persistence boundary for aggregate roots                      |
| `RepositoryError<E>`           | `save` error with a `ConcurrencyConflict` variant             |
| `ValueObject`                  | Marker for immutable value objects                            |
| `DomainError` / `DomainResult` | Shared domain-level result types                              |

Aggregates carry an optimistic-concurrency `version`. Derive it from a
`#[version] version: u64` field, and `Repository::save(&mut aggregate)` advances
it on success or returns `RepositoryError::ConcurrencyConflict` on a stale write:

```rust
#[derive(Debug, Clone, Entity, AggregateRoot)]
pub struct Order {
    #[id]      id: OrderId,
    #[version] version: u64,
    #[events]  events: AggregateEvents<OrderEvent>,
    // ... domain state ...
}
```

### `pharos-macros`

Procedural macros that reduce repetitive domain boilerplate:

- `#[derive(Entity)]`
- `#[derive(AggregateRoot)]`
- `#[derive(DomainEvent)]`
- `id_type!(...)`

The `id_type!` macro generates strongly typed UUID wrappers:

```rust
use pharos_macros::id_type;

id_type!(OrderId, CustomerId);

let order_id = OrderId::new_v7();
let another_id = OrderId::new(); // delegates to UUID v7
```

Generated ID API:

- `new()`
- `new_v7()`
- `from_uuid(...)`
- `as_uuid()`
- `From<uuid::Uuid>`
- `Display`

> Generated IDs intentionally do **not** implement `Default`. A `Default` that
> mints a fresh random UUID violates the "empty/zero" meaning of `Default` and
> silently produces phantom IDs during deserialization. Construct IDs explicitly
> with `new()` / `from_uuid(...)`.

### `pharos-app`

Application-layer contracts and orchestration helpers:

| Area               | Public API                                                                          |
|--------------------|-------------------------------------------------------------------------------------|
| Commands           | `Command`, `CommandHandler`                                                         |
| Queries            | `Query`, `QueryHandler`                                                             |
| Domain events      | `EventBus` (concrete), `EventHandler`, `save_and_publish`                           |
| Errors             | `ApplicationError`                                                                  |
| Outbox seam        | `save_and_enqueue`, `OutboxRepository`, `OutboxDispatcher`, `DispatchResult`        |
| Inbox/idempotency  | `InboxStore`, `IdempotencyStore`, `IdempotencyDecision`                             |
| Messaging          | `Message`, `Delivery`, `MessagePublisher`, `MessageConsumer`, `MessageAcknowledger` |
| Retry              | `RetryPolicy`, `RetryDecision`, `BackoffStrategy`                                   |
| Integration events | `IntegrationEvent<P>`                                                               |
| Serialization      | `EventSerializer`, `JsonEventSerializer`, `SerializedEvent`                         |
| Tower (`tower`)    | `CommandHandlerService`, `QueryHandlerService`                                      |

### `pharos-infra`

In-memory adapters, ideal for tests, examples, and local development:

| Adapter                            | Backing technology | Implements                                                   |
|------------------------------------|--------------------|--------------------------------------------------------------|
| `InMemoryRepository<A>`            | `DashMap`          | `Repository<A>`                                              |
| `InMemoryMessageBroker`            | In-memory queues   | `MessagePublisher`, `MessageConsumer`, `MessageAcknowledger` |
| `InMemoryOutboxRepository`         | `DashMap`          | `OutboxRepository`                                           |
| `InMemoryInboxStore`               | `DashMap`          | `InboxStore`                                                 |
| `InMemoryDeadLetterQueue`          | In-memory          | `DeadLetterQueue`                                            |
| `InMemorySchemaRegistry`           | In-memory          | `SchemaRegistry`                                             |
| `InMemoryConsumerGroupCoordinator` | In-memory          | `ConsumerGroupCoordinator`                                   |

### `pharos-postgres`

Pooled PostgreSQL adapters. Build a connection pool once with
`connect_pool(url, max_size)` and share it (it is cheap to clone) across every
adapter:

| Adapter                     | Backing technology | Implements                   |
|-----------------------------|--------------------|------------------------------|
| `PostgresOutboxRepository`  | PostgreSQL         | `OutboxRepository`           |
| `PostgresInboxStore`        | PostgreSQL         | `InboxStore`                 |
| `PostgresDeadLetterQueue`   | PostgreSQL         | `DeadLetterQueue`            |
| `PostgresJsonRepository<A>` | PostgreSQL JSONB   | `Repository<A>`              |
| `TenantJsonRepository<A>`   | PostgreSQL JSONB   | multi-tenant `Repository<A>` |
| `PostgresUnitOfWork`        | PostgreSQL         | `UnitOfWork`                 |

### `pharos-redis`

| Adapter              | Backing technology | Implements                                                   |
|----------------------|--------------------|--------------------------------------------------------------|
| `RedisMessageBroker` | Redis lists/sets   | `MessagePublisher`, `MessageConsumer`, `MessageAcknowledger` |

### `pharos-axum`

Axum integration for HTTP adapters over application handlers:

- `CommandHandlerState<C, H>` and `QueryHandlerState<Q, H>` extract typed handlers from router state.
- `run_command` and `run_query` adapt JSON bodies / query parameters to `CommandHandler` and `QueryHandler`.

### `pharos-saga`

Saga/process-manager primitives:

- `Saga`, `SagaTransition`, `SagaStore`, `CommandDispatcher`
- `SagaRunner` for loading state, reacting to an event, persisting state, and dispatching follow-up commands

### `pharos-es`

Event-sourcing primitives:

- `EventStore`, `SnapshotStore`, `StoredEvent`, `Snapshot`
- `EventSourced` and `EventSourcedRepository`

### `pharos-kafka`

Kafka and remote schema-registry adapters:

- `KafkaPublisher`, `KafkaConsumer`, `KafkaAcknowledger`
- `ConfluentSchemaRegistry`, `ApicurioSchemaRegistry`

### `pharos-nats`

Core NATS messaging adapters:

- `NatsPublisher`, `NatsConsumer`, `NatsAcknowledger`

## Event-driven model

Pharos separates domain events from integration events.

- **Domain events** are internal facts emitted by aggregates.
- **Integration events** are external contracts intended for brokers, other services, pipelines, or async workers.

```mermaid
flowchart TD
    Aggregate[Aggregate Root]
    DomainEvent[Domain Event]
    InternalBus[In-process EventBus]
    Handler[Domain Event Handler]
    Mapper[Integration Event Mapper]
    Envelope[IntegrationEvent Envelope]
    Serializer[EventSerializer]
    Message[Message]
    Outbox[OutboxRepository]
    Dispatcher[OutboxDispatcher]
    Broker[MessagePublisher]

    Aggregate -->|raises| DomainEvent
    DomainEvent -->|internal reaction| InternalBus
    InternalBus --> Handler
    DomainEvent -->|external contract| Mapper
    Mapper --> Envelope
    Envelope --> Serializer
    Serializer --> Message
    Message --> Outbox
    Outbox --> Dispatcher
    Dispatcher --> Broker
```

## Save and publish: in-process domain events

Use `save_and_publish` when your side effects run inside the same process.

```mermaid
sequenceDiagram
    participant UseCase as Command Handler
    participant Aggregate as Aggregate
    participant Repo as Repository
    participant Bus as EventBus
    participant Handler as Event Handler

    UseCase->>Aggregate: execute domain behavior
    Aggregate-->>UseCase: pending domain events
    UseCase->>Repo: save aggregate
    UseCase->>Bus: publish each domain event
    Bus->>Handler: handle event
    Handler-->>Bus: result
    Bus-->>UseCase: result
```

This is best for:

- modular monoliths
- local side effects
- tests and examples
- simple event-driven flows inside one process

## Save and enqueue: distributed event-driven seam

Use `save_and_enqueue` when domain events should become durable outbox messages before being published to external infrastructure.

```mermaid
sequenceDiagram
    participant UseCase as Command Handler
    participant Aggregate as Aggregate
    participant Repo as Repository
    participant Outbox as OutboxRepository
    participant Worker as OutboxDispatcher
    participant Broker as MessagePublisher

    UseCase->>Aggregate: execute domain behavior
    Aggregate-->>UseCase: pending domain events
    UseCase->>Repo: save aggregate
    UseCase->>Outbox: insert outbox message per event
    Worker->>Outbox: fetch pending messages
    Worker->>Broker: publish message
    Broker-->>Worker: ack publish
    Worker->>Outbox: mark published
```

In production, the aggregate save and outbox insert should usually participate in the same database transaction. Pharos exposes the seam; concrete transactional composition belongs to the application or a database-specific adapter.

## Inbox and idempotent consumers

Consumers in distributed systems must tolerate duplicate deliveries. `InboxStore` models that behavior.

```mermaid
stateDiagram-v2
    [*] --> StartProcessing
    StartProcessing --> AlreadyProcessing: duplicate while running
    StartProcessing --> Failed: mark_failed
    Failed --> RetryPreviousFailure: begin again
    RetryPreviousFailure --> Completed: mark_completed
    StartProcessing --> Completed: mark_completed
    Completed --> AlreadyCompleted: duplicate after success
```

Typical consumer flow:

```mermaid
flowchart TD
    Delivery[Broker Delivery]
    Begin[InboxStore.begin_processing]
    Decision{Decision}
    Work[Process message]
    Complete[mark_completed and ack]
    Fail[mark_failed and nack]
    Skip[Skip duplicate]

    Delivery --> Begin
    Begin --> Decision
    Decision -->|StartProcessing| Work
    Decision -->|RetryPreviousFailure| Work
    Decision -->|AlreadyProcessing| Skip
    Decision -->|AlreadyCompleted| Skip
    Work -->|ok| Complete
    Work -->|error| Fail
```

## Integration event envelope

`IntegrationEvent<P>` provides a stable external envelope:

```mermaid
classDiagram
    class IntegrationEvent~P~ {
        Uuid event_id
        String event_type
        u32 schema_version
        DateTime occurred_at
        Option~String~ aggregate_id
        Option~String~ correlation_id
        Option~String~ causation_id
        String source
        Option~String~ tenant_id
        Option~String~ trace_id
        P payload
        BTreeMap metadata
    }
```

Recommended usage:

- `event_type`: stable routing name, e.g. `OrderConfirmed`
- `schema_version`: increment when the public payload contract changes
- `correlation_id`: business flow identifier
- `causation_id`: command/message/event that caused this event
- `trace_id`: distributed trace propagation
- `source`: service or bounded context emitting the event

## Default PostgreSQL adapters

`pharos-postgres` includes:

- `PostgresOutboxRepository`
- `PostgresInboxStore`
- `PostgresJsonRepository<A>`
- `POSTGRES_EVENTING_SCHEMA` / `POSTGRES_AGGREGATE_SCHEMA`
- `connect_pool(url, max_size)`

All adapters share a bounded `sqlx::PgPool`, so concurrent
requests reuse connections instead of serializing on a single one. Build the pool
once and clone it into each adapter:

```rust
use pharos_postgres::{PostgresOutboxRepository, connect_pool};

let pool = connect_pool("postgres://postgres:postgres@localhost:5432/app", 16).await?;
let outbox = PostgresOutboxRepository::new(pool.clone());
outbox.migrate().await?;
```

For production, apply the versioned SQL history under
`crates/pharos-postgres/migrations/` with your migration tool instead of
running schema creation automatically on startup.

### PostgreSQL schema overview

```mermaid
erDiagram
    PHAROS_OUTBOX {
        uuid id PK
        uuid message_id
        text topic
        text message_key
        jsonb headers
        bytea payload
        text content_type
        text status
        integer attempts
        timestamptz created_at
        timestamptz updated_at
        text last_error
    }

    PHAROS_INBOX {
        uuid message_id PK
        text consumer PK
        text status
        timestamptz received_at
        timestamptz updated_at
        text last_error
    }
```

## Default Redis adapter

`pharos-redis`'s `RedisMessageBroker` maps framework messaging traits to Redis lists and sets:

| Operation | Redis command                        |
|-----------|--------------------------------------|
| publish   | `RPUSH <topic> <encoded-delivery>`   |
| consume   | `LPOP <topic>`                       |
| ack       | `SADD pharos:acked <message_id>`     |
| nack      | `SADD pharos:nacked <message_id>`    |
| requeue   | `RPUSH <topic> <encoded-redelivery>` |

```mermaid
sequenceDiagram
    participant Publisher
    participant Redis as Redis List
    participant Consumer
    participant AckSet as pharos:acked
    participant NackSet as pharos:nacked

    Publisher->>Redis: RPUSH topic delivery
    Consumer->>Redis: LPOP topic
    alt success
        Consumer->>AckSet: SADD message_id
    else failure with requeue
        Consumer->>NackSet: SADD message_id
        Consumer->>Redis: RPUSH topic redelivery
    end
```

The Redis adapter is intentionally simple and broker-like. For stronger operational guarantees, Kafka/RabbitMQ/NATS/SQS adapters can be implemented behind the same `MessagePublisher`, `MessageConsumer`, and `MessageAcknowledger` traits.

## Observability

Pharos is instrumented with `tracing` across the key seams:

- command handlers
- query handlers
- `save_and_publish`
- `save_and_enqueue`
- event publication
- event handler execution
- repository operations
- outbox dispatch
- in-memory broker operations
- PostgreSQL outbox/inbox operations
- Redis messaging operations

Metrics counters are emitted through the `metrics` crate for key operations such as:

- domain events published
- outbox messages enqueued
- outbox publish success/failure
- broker publish/consume/ack/nack
- PostgreSQL outbox/inbox lifecycle operations
- Redis publish/consume/ack/nack operations

```mermaid
flowchart TD
    Operation[Framework Operation]
    Span[tracing span]
    Event[structured tracing event]
    Metrics[metrics counter]
    Subscriber[Tracing Subscriber]
    Backend[Logs, Jaeger, Tempo, OpenTelemetry]
    MetricsBackend[Prometheus or metrics backend]

    Operation --> Span
    Operation --> Event
    Operation --> Metrics
    Span --> Subscriber
    Event --> Subscriber
    Subscriber --> Backend
    Metrics --> MetricsBackend
```

## UUID v7 support

The workspace uses the `uuid` crate with UUID v7 enabled. IDs generated by `id_type!` expose `new_v7()` publicly, and `new()` delegates to UUID v7 generation.

```rust
use pharos_macros::id_type;

id_type!(OrderId);

let id = OrderId::new_v7();
let default_id = OrderId::new();
```

UUID v7 is useful for event-driven systems because identifiers are time-ordered while still globally unique.

## Example applications

The workspace ships three examples:

| Example                     | Shows                                                                                                          |
|-----------------------------|----------------------------------------------------------------------------------------------------------------|
| `examples/order`            | The canonical DDD/CQRS/outbox suite (below).                                                                   |
| `examples/multi-tenant`     | `TenantContext` + per-tenant repositories and row-level isolation (`cargo run -p multi-tenant`).               |
| `examples/modular-monolith` | Two bounded contexts in one process, wired through the in-process event bus (`cargo run -p modular-monolith`). |

For a deployment checklist, see [`docs/guide/production.md`](docs/guide/production.md).

The canonical order example suite lives in `examples/order`.

The executable stays intentionally small and demonstrates the happy-path in-process flow:

- creating an order
- adding items
- confirming an order
- raising and publishing domain events
- event handlers reacting to domain events
- query handler usage
- structured tracing output

Run it with:

```sh
cargo run -p order
```

The broader framework features are demonstrated as focused example/integration tests instead of being packed into `main.rs`:

| Feature area                                                     | Example file                                                     |
|------------------------------------------------------------------|------------------------------------------------------------------|
| DDD, aggregate behavior, value objects, macros                   | `examples/order/src/domain/*`                                    |
| Commands, queries, in-process event handlers                     | `examples/order/src/application/*`                               |
| Normalized relational PostgreSQL repository                      | `examples/order/src/infrastructure/postgres_order_repository.rs` |
| PostgreSQL repository Docker validation                          | `examples/order/tests/postgres_order_repository.rs`              |
| Domain event to integration event mapping and JSON serialization | `examples/order/tests/integration_event_flow.rs`                 |
| `save_and_enqueue` and outbox message creation                   | `examples/order/tests/outbox_flow.rs`                            |
| Outbox dispatcher publishing to a broker-like adapter            | `examples/order/tests/outbox_dispatcher_flow.rs`                 |
| Redis publish/consume/nack/requeue/ack against Docker            | `examples/order/tests/redis_messaging_flow.rs`                   |
| Inbox/idempotent consumer decisions                              | `examples/order/tests/inbox_idempotency_flow.rs`                 |
| Retry policy and dead-letter queue                               | `examples/order/tests/dead_letter_flow.rs`                       |
| Versioned event schemas                                          | `examples/order/tests/schema_registry_flow.rs`                   |
| Consumer group partition assignments                             | `examples/order/tests/consumer_group_flow.rs`                    |
| Unit-of-work seam                                                | `examples/order/tests/unit_of_work_flow.rs`                      |
| HTTP/gRPC transport contract seam                                | `examples/order/tests/transport_contract_flow.rs`                |
| OpenTelemetry and metrics configuration descriptors              | `examples/order/tests/observability_config_flow.rs`              |

```mermaid
flowchart TD
    Main[order main.rs]
    Domain[Domain model]
    CQRS[Command and query handlers]
    InProcess[In-process EventBus]
    Tests[Focused example tests]
    Integration[IntegrationEvent and serializer]
    Outbox[Outbox and dispatcher]
    Messaging[Redis and in-memory messaging]
    Inbox[Inbox idempotency]
    Operations[DLQ schemas consumer groups transport observability]

    Main --> Domain
    Main --> CQRS
    Main --> InProcess
    Tests --> Integration
    Tests --> Outbox
    Tests --> Messaging
    Tests --> Inbox
    Tests --> Operations
```

## Relational persistence pattern

Pharos intentionally does not try to become an ORM. For relational models, the recommended pattern is to implement `Repository<A>` explicitly for each aggregate using SQL that matches the real schema.

The order example includes `PostgresOrderRepository`, which persists the aggregate in normalized tables:

```mermaid
erDiagram
    ORDERS {
        uuid id PK
        uuid customer_id
        text status
        timestamptz updated_at
    }

    ORDER_ITEMS {
        uuid id PK
        uuid order_id FK
        text description
        integer quantity
        bigint unit_price_cents
        integer position
    }

    ORDERS ||--o{ ORDER_ITEMS : contains
```

This repository:

- stores `Order` state in `orders`
- stores aggregate-internal `OrderItem`s in `order_items`
- uses PostgreSQL constraints and a foreign key
- wraps `save` and `delete` in real PostgreSQL transactions
- rehydrates the aggregate through a controlled domain constructor
- is validated by a Docker integration test against PostgreSQL

This is the preferred production direction for relational persistence: explicit repositories and migrations per aggregate, with framework traits providing the boundary.

## Commands

### Build

```sh
cargo build --workspace
```

### Test

The container-backed integration tests are a first-class part of the suite (they
are never `#[ignore]`d), so **running the tests requires a running Docker
daemon**. They validate the standard adapters against real infrastructure:

- PostgreSQL outbox (incl. `FOR UPDATE SKIP LOCKED`)
- PostgreSQL inbox/idempotency
- PostgreSQL JSON aggregate repository (optimistic concurrency)
- Atomic aggregate save + outbox insert in one transaction
- `PostgresUnitOfWork` commit/rollback
- Row-level tenant isolation (`TenantJsonRepository`)
- PostgreSQL normalized order repository
- Redis messaging

Containers are ephemeral: each test spins up its own PostgreSQL or Redis instance
via `testcontainers` and tears it down automatically when the test finishes.

```sh
cargo test --workspace --all-features
# or, to bound concurrent containers on a small machine:
cargo test-docker   # = cargo test --workspace --all-features -- --test-threads=1
```

Target a single suite:

```sh
cargo test -p pharos-postgres --test docker_integration
cargo test -p pharos-redis --test docker_integration
cargo test -p multi-tenant --test tenant_isolation
```

CI runs the full suite, including the container tests, on every push and pull
request (`.github/workflows/ci.yml`).

### Generate docs

```sh
cargo docs   # alias for: cargo doc --workspace --no-deps
```

Generated docs are written to `target/doc/`. Use this (or `cargo doc
--no-deps`) rather than a plain `cargo doc`: the Mermaid header injected via
`.cargo/config.toml` uses a workspace-relative path that only resolves for
workspace crates, so documenting dependencies would fail. Mermaid diagrams in
the docs render client-side from a CDN, so viewing them needs network access.

## Design principles

```mermaid
mindmap
  root((Pharos RS))
    Small Core
      DDD primitives
      Repository boundary
      Domain events
    Application Contracts
      CQRS handlers
      Outbox seam
      Inbox idempotency
      Messaging traits
    Infrastructure Adapters
      In-memory
      PostgreSQL
      Redis
      Extensible adapters
    Observability
      tracing
      metrics
      correlation fields
```

Pharos aims to stay:

- explicit rather than magical
- framework-light
- idiomatic Rust
- compatible with DDD and CQRS patterns
- extensible through traits and adapters
- useful for modular monoliths and as a foundation for distributed event-driven systems

## Current limitations recalculated

The previous limitations have mostly moved from “missing framework concepts” to “production-hardening and specialized adapter work”. Pharos now includes:

- database-backed aggregate persistence via `PostgresJsonRepository<A>`
- an explicit normalized relational repository pattern via the order example's `PostgresOrderRepository`
- unit-of-work contracts via `UnitOfWork`/`NoopUnitOfWork`, plus a real
  transactional `PostgresUnitOfWork`
- atomic aggregate save + outbox insert in one transaction via
  `save_aggregate_and_enqueue`
- row-level multi-tenancy via `TenantContext` and `TenantJsonRepository`
- a configurable outbox dispatcher (`DispatchConfig`) with automatic retry and
  dead-lettering
- observability configuration descriptors for OpenTelemetry and metrics backends
- HTTP/gRPC transport contracts via `TransportAdapter`
- schema registry contracts and an in-memory implementation
- dead-letter queue contracts and an in-memory implementation
- consumer group / partition assignment contracts and an in-memory coordinator
- PostgreSQL outbox/inbox adapters and Redis broker-like messaging

Remaining limitations:

| Area                        | Current status                                                                                                | Remaining limitation                                                     |
|-----------------------------|---------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------|
| Specialized broker adapters | Generic messaging traits plus Redis adapter exist                                                             | No first-party Kafka, RabbitMQ, NATS or SQS client adapters yet          |
| Aggregate persistence       | PostgreSQL JSONB repository, tenant-scoped repository, and explicit normalized order repository example exist | No custom relational aggregate repositories generated automatically      |
| Transactions                | `PostgresUnitOfWork` and atomic `save_aggregate_and_enqueue` exist                                            | No higher-level transactional pipeline wrapping command handlers yet     |
| OpenTelemetry               | Configuration descriptor exists                                                                               | No built-in OTLP pipeline installer/exporter dependency wired by default |
| Metrics                     | Metrics config descriptor and counters exist                                                                  | No built-in Prometheus server/exporter dependency wired by default       |
| Transport                   | HTTP/gRPC contracts exist                                                                                     | No Axum/Tonic server adapters yet                                        |
| Schema registry             | Contract and in-memory registry exist                                                                         | No Confluent/Apicurio/remote registry adapter yet                        |
| Dead-lettering              | Contract and in-memory queue exist                                                                            | No PostgreSQL/Redis/broker-backed DLQ adapter yet                        |
| Consumer groups             | Contract and in-memory coordinator exist                                                                      | No broker-native group coordination adapters yet                         |

In short: the framework now exposes the seams and default local/PostgreSQL/Redis implementations, but specialized production adapters for specific ecosystems should still be added as separate infrastructure modules or crates.

## Recommended production path

```mermaid
flowchart TD
    Start[Use core/app contracts]
    Domain[Model aggregates and domain events]
    Outbox[Use save_and_enqueue]
    Transaction[Make aggregate save and outbox insert transactional]
    Dispatcher[Run OutboxDispatcher worker]
    Broker[Use Redis adapter or implement Kafka/RabbitMQ/NATS/SQS]
    Consumer[Use InboxStore for idempotency]
    Observe[Wire tracing and metrics backends]

    Start --> Domain
    Domain --> Outbox
    Outbox --> Transaction
    Transaction --> Dispatcher
    Dispatcher --> Broker
    Broker --> Consumer
    Consumer --> Observe
```

## License

This repository currently does not declare a license file. If you intend to publish or share it externally, add an explicit license before doing so.
