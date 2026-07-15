# Crate reference

This page describes the public API of each crate in the Pharos RS workspace.

## `pharos-core`

Core domain primitives used by the entire framework:

| Type                           | Purpose                                                       |
| ------------------------------ | ------------------------------------------------------------- |
| `Entity`                       | Stable identity for domain objects                            |
| `AggregateRoot`                | Aggregate boundary with pending events and OCC `version()`    |
| `AggregateEvents<E>`           | Small event buffer for aggregates                             |
| `DomainEvent`                  | Immutable fact with type, timestamp and aggregate correlation |
| `Repository<A>`                | Persistence boundary for aggregate roots                      |
| `RepositoryError<E>`           | `save` error with a `ConcurrencyConflict` variant             |
| `ValueObject`                  | Marker for immutable value objects                            |
| `Money` / `Currency`           | Currency-aware amounts in `i128` minor units (covers wei); checked arithmetic, lossless `allocate` |
| `DomainError` / `DomainResult` | Shared domain-level result types                              |
| `value_object!`                | Validated value objects with a single construction point      |

`Money` never touches floats and every operation is checked: mixing currencies
or overflowing returns a `MoneyError`. With the optional `serde` feature the
amount serializes as a decimal string, so wei-scale values survive JSON
consumers that lose precision above 2^53.

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

## `pharos-macros`

Procedural macros that reduce repetitive domain boilerplate:

- `#[derive(Entity)]`, `#[derive(AggregateRoot)]`, `#[derive(DomainEvent)]`
- `#[derive(Command)]` / `#[derive(Query)]` (NAME, `#[trace]` span fields, garde validation)
- `id_type!(...)`

Emitted paths auto-detect the caller's dependencies: direct `pharos-core`/
`pharos-app` deps win, and facade-only crates are routed through
`pharos::core`/`pharos::app` automatically.

The `id_type!` macro generates strongly typed UUID wrappers:

```rust
use pharos_macros::id_type;

id_type!(OrderId, CustomerId);

let order_id = OrderId::new_v7();
let another_id = OrderId::new(); // delegates to UUID v7
```

Generated ID API: `new()`, `new_v7()`, `from_uuid(...)`, `as_uuid()`, `From<uuid::Uuid>`, `Display`.

> Generated IDs intentionally do **not** implement `Default`. A `Default` that
> mints a fresh random UUID violates the "empty/zero" meaning of `Default` and
> silently produces phantom IDs during deserialization. Construct IDs explicitly
> with `new()` / `from_uuid(...)`.

## `pharos-app`

Application-layer contracts and orchestration helpers:

| Area               | Public API                                                                                           |
| ------------------ | ---------------------------------------------------------------------------------------------------- |
| Commands           | `Command`, `CommandHandler`, `dispatch` (runs validation + tracing), `DispatchError`                 |
| Validation         | `ValidationError`, `FieldViolation` (validator-agnostic)                                             |
| Queries            | `Query`, `QueryHandler`, `query_dispatch`                                                            |
| Domain events      | `EventBus` (concrete, `PublishErrorPolicy`), `EventHandler`, `save_and_publish`, `republish_pending` |
| Errors             | `ApplicationError` (typed sources)                                                                   |
| Outbox seam        | `save_and_enqueue` (contracts re-exported from `pharos-messaging`)                                   |
| Integration events | `IntegrationEvent<P>`, `CorrelationId`, `CausationId`, `caused_by`                                   |
| Serialization      | `EventSerializer`, `JsonEventSerializer`, `SerializedEvent`                                          |
| Schema evolution   | `JsonUpcasterRegistry`, `VersionedJsonCodec`                                                         |
| Unified codec      | `MessageCodec<P>` — format-agnostic encode/decode trait (JSON and Protobuf both impl it)             |
| Tower (`tower`)    | `CommandHandlerService`, `QueryHandlerService` — the cross-cutting pipeline seam                     |
| Tenant             | `TenantContext`/`TenantId` (canonical); `CURRENT_TENANT` behind `tenant-task-local`                  |
| Resilience         | `Retrying` (feature `retry`) and `DeadLettering` event-handler decorators                            |

## `pharos-messaging`

Broker-facing contracts, versioned independently of the CQRS surface
(`pharos-app` re-exports everything here):

| Area                | Public API                                                                                                         |
| ------------------- | ------------------------------------------------------------------------------------------------------------------ |
| Messaging           | `Message`, `Delivery`, `MessagePublisher`, `MessageConsumer`, `MessageAcknowledger`                                |
| Retry               | `RetryPolicy`, `RetryDecision`, `BackoffStrategy` (RNG-based jitter)                                               |
| Outbox              | `OutboxMessage`, `OutboxRepository`, `OutboxDispatcher`, `DispatchConfig` (batch size, retry, publish concurrency) |
| Inbox/idempotency   | `InboxStore`, `IdempotencyDecision`                                                                                |
| Dead letter         | `DeadLetterQueue`, `DeadLetterMessage`, `sweep_failed_to_dead_letter`                                              |
| Idempotent consumer | `process_idempotent` — the full begin/handle/mark/ack flow in one call                                             |
| Consumer groups     | `ConsumerGroupCoordinator`, `PartitionAssignment`                                                                  |
| Schema registry     | `SchemaRegistry`, `EventSchema`                                                                                    |

## `pharos-memory`

In-memory adapters, ideal for tests, examples, and local development:

| Adapter                            | Backing technology | Implements                                                   |
| ---------------------------------- | ------------------ | ------------------------------------------------------------ |
| `InMemoryRepository<A>`            | `DashMap`          | `Repository<A>`                                              |
| `InMemoryMessageBroker`            | In-memory queues   | `MessagePublisher`, `MessageConsumer`, `MessageAcknowledger` |
| `InMemoryOutboxRepository`         | `DashMap`          | `OutboxRepository`                                           |
| `InMemoryInboxStore`               | `DashMap`          | `InboxStore`                                                 |
| `InMemoryDeadLetterQueue`          | In-memory          | `DeadLetterQueue`                                            |
| `InMemorySchemaRegistry`           | In-memory          | `SchemaRegistry`                                             |
| `InMemoryConsumerGroupCoordinator` | In-memory          | `ConsumerGroupCoordinator`                                   |

## `pharos-postgres`

Pooled PostgreSQL adapters. Build a connection pool once with
`connect_pool(url, max_size)` and share it (it is cheap to clone) across every
adapter:

| Adapter                                              | Backing technology | Implements                                                                       |
| ---------------------------------------------------- | ------------------ | -------------------------------------------------------------------------------- | ---- | ------ |
| `PostgresOutboxRepository`                           | PostgreSQL         | `OutboxRepository`                                                               |
| `PostgresInboxStore`                                 | PostgreSQL         | `InboxStore`                                                                     |
| `PostgresDeadLetterQueue`                            | PostgreSQL         | `DeadLetterQueue`                                                                |
| `PostgresJsonRepository<A>`                          | PostgreSQL JSONB   | `Repository<A>`                                                                  |
| `TenantJsonRepository<A>`                            | PostgreSQL JSONB   | multi-tenant `Repository<A>`                                                     |
| `PostgresUnitOfWork`                                 | PostgreSQL         | transactional boundary (`transaction(                                            | conn | ...)`) |
| `TransactionalRepository<A>` + `save_and_enqueue_in` | PostgreSQL         | atomic aggregate save + outbox for any repository (JSONB or explicit relational) |
| `PgEventStore<I, E>` / `PgSnapshotStore<I, S>`       | PostgreSQL JSONB   | `EventStore` / `SnapshotStore` (`pharos-es`) with PK-arbitrated OCC on append    |
| `PgSagaStore<I, S>`                                  | PostgreSQL JSONB   | `SagaStore` + `SagaTimeoutStore` (`pharos-saga`) with a partial index for due deadlines |

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

## `pharos-redis`

| Adapter              | Backing technology | Implements                                                   |
| -------------------- | ------------------ | ------------------------------------------------------------ |
| `RedisMessageBroker` | Redis lists/sets   | `MessagePublisher`, `MessageConsumer`, `MessageAcknowledger` |

Redis command mapping:

| Operation | Redis command                        |
| --------- | ------------------------------------ |
| publish   | `RPUSH <topic> <encoded-delivery>`   |
| consume   | `LPOP <topic>`                       |
| ack       | `SADD pharos:acked <message_id>`     |
| nack      | `SADD pharos:nacked <message_id>`    |
| requeue   | `RPUSH <topic> <encoded-redelivery>` |

The Redis adapter is intentionally simple and broker-like. For stronger
operational guarantees, implement a Kafka/RabbitMQ/NATS/SQS adapter behind the
same `MessagePublisher`, `MessageConsumer`, and `MessageAcknowledger` traits.

## `pharos-axum`

Axum integration for HTTP adapters over application handlers:

- `CommandHandlerState<C, H>` and `QueryHandlerState<Q, H>` extract typed handlers from router state.
- `run_command` and `run_query` adapt JSON bodies / query parameters to `CommandHandler` and `QueryHandler`.

## `pharos-saga`

Saga/process-manager primitives:

- `Saga` (with `on_timeout`), `SagaTransition`, `SagaStore`, `SagaTimeoutStore`, `CommandDispatcher`
- `SagaRunner` for loading state, reacting to an event, persisting state, and dispatching follow-up commands
- Deadlines: `SagaInstance::running_until` and the `deadline` on `Start`/`Advance` schedule a timeout; `SagaRunner::run_due_timeouts` fires `Saga::on_timeout` for elapsed instances (call it from a periodic task — the app owns the scheduler)

## `pharos-es`

Event-sourcing primitives:

- `EventStore`, `SnapshotStore`, `StoredEvent`, `Snapshot`
- `EventSourced` and `EventSourcedRepository`
- Durable adapters live in `pharos-postgres` (`PgEventStore`, `PgSnapshotStore`)

## `pharos-kafka`

Kafka and remote schema-registry adapters:

- `KafkaPublisher`, `KafkaConsumer`, `KafkaAcknowledger`
- `ConfluentSchemaRegistry`, `ApicurioSchemaRegistry`

## `pharos-nats`

Core NATS messaging adapters:

- `NatsPublisher`, `NatsConsumer`, `NatsAcknowledger`

## `pharos-proto`

Protobuf binary serialization for [`IntegrationEvent<P>`](pharos_app::IntegrationEvent)
envelopes. The payload type `P` must derive [`prost::Message`] and [`Default`]
(prost auto-derives `Default` and `Debug` — do not add them manually).

| Type / Constant            | Purpose                                                                  |
| -------------------------- | ------------------------------------------------------------------------ |
| `ProtobufEventSerializer`  | Serialize / deserialize `IntegrationEvent<P>` as Protobuf bytes          |
| `IntegrationEventEnvelope` | Protobuf wire format for the full envelope (tags 1–12, stable)           |
| `APPLICATION_PROTOBUF`     | Content type `"application/x-protobuf"` for `SerializedEvent`            |
| `MessageCodec` (re-export) | Re-exported from `pharos-app` so codec-generic code avoids the extra dep |
| `prost` (re-export)        | Re-exported so downstream crates can derive `prost::Message`             |

Enable with the `proto` feature (included in the `full` bundle):

```toml
pharos = { ..., features = ["proto"] }
# or add prost directly for payload derive macros
prost = "0.13"
```

```rust
#[derive(Clone, prost::Message)]   // Default and Debug come from prost automatically
pub struct OrderPlacedProto {
    #[prost(string, tag = "1")]
    pub order_id: String,
    #[prost(uint64, tag = "2")]
    pub amount_cents: u64,
}

let serializer = pharos::proto::ProtobufEventSerializer;
let event      = IntegrationEvent::new("OrderPlaced", 1, "orders", OrderPlacedProto { .. });
let wire       = serializer.serialize(&event)?;
// wire.content_type == "application/x-protobuf"

let roundtrip: IntegrationEvent<OrderPlacedProto> = serializer.deserialize(&wire)?;
```

> **Tag stability** — field tags in `IntegrationEventEnvelope` (1–12) are stable;
> never reuse a removed tag. The same discipline applies to your payload types.

## UUID v7 support

The workspace uses the `uuid` crate with UUID v7 enabled. IDs generated by
`id_type!` expose `new_v7()` publicly, and `new()` delegates to UUID v7
generation. UUID v7 is useful for event-driven systems because identifiers are
time-ordered while still globally unique.

```rust
use pharos_macros::id_type;

id_type!(OrderId);

let id = OrderId::new_v7();
let default_id = OrderId::new();
```
