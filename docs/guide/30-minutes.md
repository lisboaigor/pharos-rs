# Pharos in 30 minutes

This tutorial goes from `cargo new` to a small but real domain model with:

- a typed aggregate root
- optimistic concurrency through `Repository::save(&mut aggregate)`
- an in-process `EventBus`
- an outbox seam for external publication later

## 1. Create the workspace

Start with the template in `templates/workspace/`, or create a fresh app and add:

```toml
[dependencies]
pharos = { version = "0.1", features = ["macros", "infra"] }
serde = { version = "1", features = ["derive"] }
thiserror = "2"
chrono = { version = "0.4", features = ["serde"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

## 2. Model an aggregate

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
    customer_name: String,
}

impl Order {
    pub fn place(customer_name: impl Into<String>) -> Self {
        let id = OrderId::new();
        let mut events = AggregateEvents::default();
        events.raise(OrderPlaced {
            order_id: id.to_string(),
            occurred_at: Utc::now(),
        });
        Self {
            id,
            version: 0,
            events,
            customer_name: customer_name.into(),
        }
    }
}
```

## 3. Persist it

For local development and tests, use the in-memory repository:

```rust
use pharos::infra::InMemoryRepository;

let repo = InMemoryRepository::<Order>::new();
let mut order = Order::place("Igor");
repo.save(&mut order).await?;
assert_eq!(order.version(), 1);
```

For production PostgreSQL, switch to `pharos-postgres` and keep the domain type unchanged.

## 4. React to domain events

```rust
use pharos::app::{EventBus, EventHandler};

struct NotifyBilling;

impl EventHandler<OrderPlaced> for NotifyBilling {
    type Error = std::convert::Infallible;

    async fn handle(&self, event: &OrderPlaced) -> Result<(), Self::Error> {
        println!("billing saw order {}", event.aggregate_id());
        Ok(())
    }
}

let bus = EventBus::new();
bus.subscribe::<OrderPlaced, _>(NotifyBilling);
```

Then persist and publish in one application step:

```rust
pharos::app::save_and_publish(&repo, &bus, &mut order).await?;
```

## 5. Add the outbox seam

If the event must leave the process, switch from `save_and_publish` to `save_and_enqueue`:

```rust
use pharos::app::{Message, save_and_enqueue};
use pharos::infra::InMemoryOutboxRepository;

let outbox = InMemoryOutboxRepository::new();
save_and_enqueue(&repo, &outbox, &mut order, |event| {
    Message::new("orders", event.aggregate_id().as_bytes().to_vec(), "text/plain")
        .with_key(event.aggregate_id())
})
.await?;
```

In production, replace the in-memory outbox with `PostgresOutboxRepository` and dispatch it in the background with `OutboxDispatcher`.

## 6. Next steps

- Use `TenantContext` + `TenantJsonRepository` when the service is multi-tenant.
- Use `save_and_enqueue_in` (works for the JSONB repository and for explicit relational repositories via `TransactionalRepository`) when aggregate save and outbox insert must commit atomically — see the [persistence ladder](persistence-ladder.md).
- Use `pharos-axum` to expose handlers over HTTP with Axum.
- Use `pharos-saga` when an event should drive a long-lived workflow.
- Use `pharos-es` if the aggregate should be rehydrated from an event stream instead of a current-state row.

## 7. Validate

Run the same commands contributors use:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features
cargo test --workspace --all-features -- --test-threads=1
```
