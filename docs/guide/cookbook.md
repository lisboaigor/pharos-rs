# Cookbook

Copy-paste templates for the most common Pharos tasks. Each snippet mirrors a
pattern that is exercised by the runnable examples and integration tests, so the
shapes here stay honest. Pull names from the prelude:

```rust
use pharos::prelude::*;
```

## Command handler (in-process events)

The default path: load, mutate, then persist and publish in one call. This is
the shape used throughout `examples/order`.

```rust
use std::sync::Arc;
use pharos::prelude::*;

pub struct ConfirmOrder { pub order_id: uuid::Uuid }
impl Command for ConfirmOrder {}

pub struct ConfirmOrderHandler<R: Repository<Order>> {
    repo: Arc<R>,
    bus: EventBus,
}

impl<R: Repository<Order>> CommandHandler<ConfirmOrder> for ConfirmOrderHandler<R> {
    type Output = ();
    type Error = AppError;

    async fn handle(&self, cmd: ConfirmOrder) -> Result<Self::Output, Self::Error> {
        let mut order = self
            .repo
            .find_by_id(&OrderId::from_uuid(cmd.order_id))
            .await
            .map_err(AppError::infra)?
            .ok_or(AppError::NotFound)?;

        order.confirm()?; // raises a domain event on the aggregate

        // Persists (advancing the OCC version) and publishes drained events.
        save_and_publish(&*self.repo, &self.bus, &mut order)
            .await
            .map_err(AppError::infra)?;
        Ok(())
    }
}
```

## Command handler with transactional save + enqueue

When another process consumes your events, persist the aggregate and the outbox
rows **in the same database transaction** so a crash cannot leave them out of
sync. Map each domain event to a broker `Message`.

```rust
use pharos::postgres::save_aggregate_and_enqueue;

// `pool` is a deadpool/sqlx Pool shared by the handler.
async fn handle(&self, cmd: PlaceOrder) -> Result<(), AppError> {
    let mut order = Order::place(cmd.into())?;

    save_aggregate_and_enqueue(
        &self.pool,
        "Order",                                       // aggregate type tag
        &mut order,
        |event| {
            Message::new(
                "orders",                              // topic
                serde_json::to_vec(event).unwrap(),    // payload
                "application/json",                    // content type
            )
            .with_key(event.aggregate_id())            // partition/order key
        },
    )
    .await
    .map_err(AppError::infra)?;
    Ok(())
}
```

The framework-agnostic equivalent (any `Repository` + any `OutboxRepository`,
without a shared transaction) is the free function:

```rust
save_and_enqueue(&*self.repo, &*self.outbox, &mut order, |event| {
    Message::new("orders", serde_json::to_vec(event).unwrap(), "application/json")
        .with_key(event.aggregate_id())
})
.await?;
```

A background `OutboxDispatcher` then drains pending rows to the broker:

```rust
// DispatchConfig::default() already carries a sane batch size (100) and an
// exponential RetryPolicy; override only what you need.
let dispatcher = OutboxDispatcher::with_config(
    outbox,
    publisher,
    DispatchConfig::default().with_batch_size(200),
);

// Call on a timer/loop. DispatchResult reports success/failure counts.
let result = dispatcher.dispatch_batch().await;
if !result.is_ok() {
    tracing::warn!(failed = result.failure_count(), "some outbox messages failed");
}
```

## Idempotent consumer

At-least-once brokers redeliver. Guard every non-idempotent consumer with an
`InboxStore`, keyed by message id + consumer name.

```rust
async fn consume<I: InboxStore>(
    inbox: &I,
    consumer: &str,
    delivery: Delivery,
) -> Result<(), AppError> {
    match inbox.begin_processing(delivery.message_id, consumer).await? {
        IdempotencyDecision::StartProcessing
        | IdempotencyDecision::RetryPreviousFailure => {
            match handle_message(&delivery).await {
                Ok(()) => inbox.mark_completed(delivery.message_id, consumer).await?,
                Err(e) => {
                    inbox.mark_failed(delivery.message_id, consumer, e.to_string()).await?;
                    return Err(e);
                }
            }
        }
        // Already done or in flight elsewhere — safe to skip.
        IdempotencyDecision::AlreadyCompleted
        | IdempotencyDecision::AlreadyProcessing => {}
    }
    Ok(())
}
```

## Tenant propagation

Thread a `TenantContext` from the edge through the application layer into the
adapters. Never derive the tenant inside a handler — pass it in, and stamp
outgoing integration events so consumers stay scoped.

```rust
// At the edge: build the context from the authenticated request.
let tenant = TenantContext::new(claims.tenant_id);

// In the application layer: scope the repository and stamp events.
let repo = tenant_notes.for_tenant(&tenant);
repo.save(&mut note).await?;

let event = tenant.stamp(IntegrationEvent::new(
    "NoteCreated", note.version(), "notes", payload,
));
// event.tenant_id is now Some(tenant id) for downstream consumers.
```

With `pharos::postgres`, `TenantJsonRepository` enforces this at the row level:
queries are filtered by `tenant_id`, so one tenant can never read another's
rows even under the same aggregate id. See `examples/multi-tenant`.

## HTTP route over a handler (axum)

```rust
use pharos::axum::{CommandHandlerState, HandlerError, run_command};

async fn place_order(
    handler: CommandHandlerState<PlaceOrder, PlaceOrderHandler<Repo>>,
    payload: axum::Json<PlaceOrder>,
) -> Result<axum::Json<OrderView>, HandlerError> {
    run_command(handler, payload).await
}
```

## Protobuf integration event

Use `ProtobufEventSerializer` when publishing to Kafka or any high-throughput
pipeline where JSON overhead is a concern. The payload type must derive
`prost::Message`; prost auto-derives `Default` and `Debug`.

```rust
use pharos::proto::{ProtobufEventSerializer, APPLICATION_PROTOBUF};
use pharos::prelude::IntegrationEvent;

// 1. Define the payload type in your bounded context.
//    prost generates Default and Debug — do not add them manually.
#[derive(Clone, prost::Message)]
pub struct OrderConfirmedProto {
    #[prost(string, tag = "1")]
    pub order_id: String,
    #[prost(uint64, tag = "2")]
    pub total_cents: u64,
}

// 2. Map a domain event to the integration event and serialize.
let payload = OrderConfirmedProto {
    order_id:    order_id.to_string(),
    total_cents: total.as_cents(),
};

let integration_event = IntegrationEvent::from_domain_event(
    &domain_event,
    1,
    "orders",
    payload,
)
.with_correlation_id(correlation_id);

let serializer = ProtobufEventSerializer;
let wire       = serializer.serialize(&integration_event)?;
// wire.content_type == "application/x-protobuf"

// 3. In the outbox map_event closure, use the wire bytes directly.
save_and_enqueue(&*self.repo, &*self.outbox, &mut order, |event| {
    let wire = serializer.serialize(&to_integration_event(event)).unwrap();
    Message::new("orders", wire.payload, wire.content_type)
        .with_key(event.aggregate_id())
})
.await?;

// 4. On the consumer side, deserialize with the same serializer.
let recovered: IntegrationEvent<OrderConfirmedProto> =
    serializer.deserialize(&received_wire)?;
```

## Format-agnostic codec with `MessageCodec<P>`

`MessageCodec<P>` is the unifying trait implemented by both `JsonEventSerializer`
and `ProtobufEventSerializer`. Write infrastructure helpers that accept any codec
so callers can swap the wire format without touching the logic:

```rust
use pharos::prelude::{IntegrationEvent, MessageCodec};
use pharos::app::serialization::SerializedEvent;

// This function works with JSON, Protobuf, or any custom codec.
fn to_wire<P, C: MessageCodec<P>>(
    codec: &C,
    event: &IntegrationEvent<P>,
) -> SerializedEvent {
    codec.encode(event).expect("encoding should not fail")
}

// JSON path — payload must impl Serialize + DeserializeOwned
use pharos::app::serialization::JsonEventSerializer;
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
struct OrderPlacedJson { order_id: String }

let json_event = IntegrationEvent::new("OrderPlaced", 1, "orders",
    OrderPlacedJson { order_id: "ord-1".into() });
let wire = to_wire(&JsonEventSerializer, &json_event);
// wire.content_type == "application/json"

// Protobuf path — payload must impl prost::Message + Default
use pharos::proto::ProtobufEventSerializer;

#[derive(Clone, prost::Message)]  // prost derives Default and Debug automatically
struct OrderPlacedProto {
    #[prost(string, tag = "1")]
    pub order_id: String,
}

let proto_event = IntegrationEvent::new("OrderPlaced", 1, "orders",
    OrderPlacedProto { order_id: "ord-1".into() });
let wire = to_wire(&ProtobufEventSerializer, &proto_event);
// wire.content_type == "application/x-protobuf"

// Decode symmetrically:
let recovered: IntegrationEvent<OrderPlacedProto> =
    ProtobufEventSerializer.decode(&wire).unwrap();
```

The hybrid approach works well when you own the internal topology (use JSON for
HTTP APIs, human-readable logs, and early development) but interact with external
systems that mandate a binary protocol (use Protobuf for Kafka or gRPC pipelines).

## See also

- [Decision matrix](decision-matrix.md) — when to choose each pattern above.
- [Pitfalls](pitfalls.md) — what goes wrong if you skip these shapes.
- `examples/order`, `examples/multi-tenant`, `examples/modular-monolith`.
