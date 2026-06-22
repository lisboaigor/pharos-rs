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

## See also

- [Decision matrix](decision-matrix.md) — when to choose each pattern above.
- [Pitfalls](pitfalls.md) — what goes wrong if you skip these shapes.
- `examples/order`, `examples/multi-tenant`, `examples/modular-monolith`.
