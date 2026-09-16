# Crate reference

This page describes the public API of each crate in the Pharos RS workspace.

## `pharos-core`

Core domain primitives used by the entire framework:

| Type                           | Purpose                                                                                            |
| ------------------------------ | -------------------------------------------------------------------------------------------------- |
| `Entity`                       | Stable identity for domain objects                                                                 |
| `AggregateRoot`                | Aggregate boundary with pending events and OCC `version()`                                         |
| `AggregateEvents<E>`           | Small event buffer for aggregates                                                                  |
| `DomainEvent`                  | Immutable fact with type, timestamp and aggregate correlation                                      |
| `Repository<A>`                | Persistence boundary for aggregate roots                                                           |
| `RepositoryError<E>`           | `save` error with a `ConcurrencyConflict` variant                                                  |
| `ValueObject`                  | Marker for immutable value objects                                                                 |
| `Money` / `Currency`           | Currency-aware amounts in `i128` minor units (covers wei); checked arithmetic, lossless `allocate` |
| `DomainError` / `DomainResult` | Shared domain-level result types                                                                   |
| `value_object!`                | Validated value objects with a single construction point                                           |

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

`pharos-memory` is the **only** storage/messaging adapter the workspace
ships. There is no PostgreSQL, Redis, Kafka, or NATS crate here anymore —
bring your own, against the same traits, following
[Writing an adapter](writing-an-adapter.md) and
[`reference-schema.sql`](reference-schema.sql).

## `pharos-testing`

Test helpers, plus the `contract` feature's adapter conformance kit:

| Area                    | Public API                                                                                                                                              |
| ----------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Event capture           | `EventCapture<E>`, `capturing_event_bus`, `TestSubscriber`                                                                                              |
| Adapter conformance kit | `contract::{repository, transactional, outbox, inbox, dead_letter, saga, event_store, messaging, schema_registry}` (feature `contract`, off by default) |

The conformance kit is plain `async fn`s, not `#[tokio::test]`s themselves —
call them from your own adapter's test, against fixture types
(`contract::fixtures::{ContractAggregate, ContractEvent}`) rather than your
domain types, so the same suite runs against any implementation of the
trait it targets. `pharos-memory`'s own adapters are verified this way in
`tests/contract_kit.rs`; use the same suite to prove your own
Postgres/Redis/Kafka/… adapter is correct before shipping it — see
[Writing an adapter](writing-an-adapter.md).

## `pharos-axum`

Axum integration for HTTP adapters over application handlers:

- `CommandHandlerState<C, H>` and `QueryHandlerState<Q, H>` extract typed handlers from router state.
- `run_command` and `run_query` adapt JSON bodies / query parameters to `CommandHandler` and `QueryHandler`.
- `run_command` (and `run_command_from_state`) refuse a command whose `Command::INTERNAL_ONLY` is `true` — set by `#[command(internal)]` — returning `404 Not Found` before the handler runs, so a saga-only command (payout, refund) accidentally wired to a route is never HTTP-reachable. In-process dispatch through a saga's `CommandDispatcher` is unaffected.

## `pharos-saga`

Saga/process-manager primitives:

- `Saga` (with `on_timeout`), `SagaTransition`, `SagaStore` (optimistic concurrency on `SagaInstance::version`, via `SagaSaveError`), `SagaTimeoutStore`, `CommandDispatcher`, `DurableCommandPublisher`
- `SagaRunner` for loading state, reacting to an event, persisting state, and enqueuing follow-up commands onto a durable outbox (`OutboxRepository`) — never dispatching them directly. Run a `pharos_messaging::OutboxDispatcher` against that same outbox, paired with a `DurableCommandPublisher` wrapping the real `CommandDispatcher`, to actually deliver them with the dispatcher's existing retry/backoff/dead-letter machinery
- Deadlines: `SagaInstance::running_until` and the `deadline` on `Start`/`Advance` schedule a timeout; `SagaRunner::run_due_timeouts` claims elapsed instances (`SagaTimeoutStore::claim_due`, lease-based) and fires `Saga::on_timeout`. Claiming makes the sweep safe to run on multiple service instances concurrently; call it from a periodic task — the app owns the scheduler
- Compensation on failure: `SagaTransition::Fail { reason, commands }` carries follow-up commands the runner enqueues like a `Complete`'s (after persisting the `Failed` state, before surfacing `SagaRunnerError::Failed`), so an abandoned or expired saga's refund/release survives independently of the process that computed it. `store.save` and the enqueue are still two separate awaits, not one transaction — a crash between them still drops the compensation — but once the enqueue itself succeeds, the command is durable and retried on delivery failure, unlike dispatching straight to a `CommandDispatcher`. Pass an empty `Vec` when failing needs no compensation
- Cross-context sagas: `SagaRunner::handle_any<E: Into<Saga::Event>>` accepts events from several bounded contexts. Define one unifying `Event` enum with a `From` impl per source and register one `EventBus` handler per source type, each forwarding through `handle_any` — the `Saga` trait stays single-event and the fan-in lives in the wiring

## `pharos-es`

Event-sourcing primitives:

- `EventStore`, `SnapshotStore`, `StoredEvent`, `Snapshot`
- `EventSourced` and `EventSourcedRepository`
- No durable adapter ships in the workspace — implement `EventStore`/
  `SnapshotStore` against your own storage; `reference-schema.sql` includes
  the event-stream/snapshot tables as a starting point.

## `pharos-realtime`

Real-time WebSocket / pub-sub primitives — fan-out-to-many-subscribers
delivery (a live game, a chat room, a dashboard with many viewers), a
different delivery contract from `pharos-messaging`'s point-to-point broker
consumption:

| Area           | Public API                                                                                                                                                                                                                     |
| -------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Rooms/messages | `RoomId`, `RealtimeMessage`, `Backlog`                                                                                                                                                                                         |
| Hub traits     | `RealtimePublisher`, `RealtimeSubscriber`, `RealtimeHub`                                                                                                                                                                       |
| In-memory hub  | `InMemoryHub` (single-node MVP backend on `tokio::sync::broadcast`)                                                                                                                                                            |
| Auth seams     | `ConnectionAuthenticator` (who is this?), `RoomAuthorizer` (may they touch this room?), `Identity`, `Access`                                                                                                                   |
| Axum glue      | `Realtime`, `RealtimeConfig`, `OnMessage`, `Reply` — WebSocket upgrade/pump: authenticate, authorize, join a room, then pump both directions while rechecking authorization and keeping the connection honest with a heartbeat |

## `pharos-observability`

Metrics, logs, and traces wiring, joined by one value: the OpenTelemetry SDK
mints a trace id, log records carry it, and metrics' exemplars carry it too.
See [Observability](observability.md) for the full walkthrough.

| Area              | Public API                                                                             |
| ----------------- | -------------------------------------------------------------------------------------- |
| Setup             | `init(service_name)`, `init_with(Config)`, `Observability` (flush-on-drop guard)       |
| HTTP              | `http::request_span`, `http::http_metrics`, `http::instrument`, `http::render_metrics` |
| Trace propagation | `propagation::inject`/`extract` — stamp/restore context across an outbox hop           |
| Filtering         | `filter::PHAROS_TARGETS`, `filter::build_filter`                                       |

`pharos-rs` emits the signals; wiring a collector stack (Prometheus, Loki,
Tempo, or a managed equivalent) to receive them is your own infrastructure's
concern.

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

Add it as a direct dependency (it is not gated behind a `pharos` facade
feature):

```toml
pharos-proto = { git = "..." }
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

let serializer = pharos_proto::ProtobufEventSerializer;
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
