# Architecture guide

This page describes Pharos RS's event-driven architecture and the key patterns
for connecting domain models to external infrastructure.

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

Use `save_and_enqueue` when domain events should become durable outbox messages
before being published to external infrastructure.

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

In production, the aggregate save and outbox insert should usually participate
in the same database transaction. Pharos exposes the seam; concrete transactional
composition belongs to the application or a database-specific adapter (see
`save_aggregate_and_enqueue` in `pharos-postgres`).

## Inbox and idempotent consumers

Consumers in distributed systems must tolerate duplicate deliveries. `InboxStore`
models that behavior.

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

## Relational persistence pattern

Pharos intentionally does not try to become an ORM. For relational models, the
recommended pattern is to implement `Repository<A>` explicitly for each aggregate
using SQL that matches the real schema.

The order example includes `PostgresOrderRepository`, which persists the aggregate
in normalized tables:

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

This is the preferred production direction for relational persistence: explicit
repositories and migrations per aggregate, with framework traits providing the
boundary.

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

See [`production.md`](production.md) for the full deployment checklist.

## Current status and limitations

| Area                        | Current status                                                                                                | Remaining limitation                                                     |
| --------------------------- | ------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------ |
| Specialized broker adapters | Generic messaging traits plus Redis adapter exist                                                             | No first-party Kafka, RabbitMQ, NATS or SQS client adapters yet          |
| Aggregate persistence       | PostgreSQL JSONB repository, tenant-scoped repository, and explicit normalized order repository example exist | No custom relational aggregate repositories generated automatically      |
| Transactions                | `PostgresUnitOfWork` and atomic `save_aggregate_and_enqueue` exist                                            | No higher-level transactional pipeline wrapping command handlers yet     |
| OpenTelemetry               | Configuration descriptor exists                                                                               | No built-in OTLP pipeline installer/exporter dependency wired by default |
| Metrics                     | Metrics config descriptor and counters exist                                                                  | No built-in Prometheus server/exporter dependency wired by default       |
| Transport                   | HTTP/gRPC contracts exist                                                                                     | No Axum/Tonic server adapters yet                                        |
| Schema registry             | Contract and in-memory registry exist                                                                         | No Confluent/Apicurio/remote registry adapter yet                        |
| Dead-lettering              | Contract and in-memory queue exist                                                                            | No PostgreSQL/Redis/broker-backed DLQ adapter yet                        |
| Consumer groups             | Contract and in-memory coordinator exist                                                                      | No broker-native group coordination adapters yet                         |

The framework exposes the seams and default local/PostgreSQL/Redis implementations.
Specialized production adapters for specific ecosystems should be added as separate
infrastructure modules or crates.
