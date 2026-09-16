//! `DefaultAggregateStore` driven end-to-end against `InMemoryUnitOfWork`:
//! this is the same store/delivery-mode machinery that used to live only in
//! `pharos-postgres`'s `PostgresAggregateStore`, now exercised against a
//! backend that ships in this workspace instead of one that needs a
//! container.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use pharos_app::{
    AggregateStore, DefaultAggregateStore, EventBus, EventHandler, OutboxRepository, OutboxSignal,
    OutboxStatus,
};
use pharos_core::{AggregateEvents, AggregateRoot, DomainEvent, Entity};
use pharos_memory::{InMemoryOutboxRepository, InMemoryUnitOfWork};
use serde::Serialize;
use tokio::sync::Mutex;

#[derive(Debug, Clone, Serialize)]
struct OrderPlaced {
    order_id: String,
    occurred_at: DateTime<Utc>,
}

impl DomainEvent for OrderPlaced {
    fn event_type(&self) -> &'static str {
        "OrderPlaced"
    }

    fn occurred_at(&self) -> DateTime<Utc> {
        self.occurred_at
    }

    fn aggregate_id(&self) -> &str {
        &self.order_id
    }
}

#[derive(Debug, Clone)]
struct Order {
    id: String,
    version: u64,
    events: AggregateEvents<OrderPlaced>,
}

impl Order {
    fn place(id: impl Into<String>) -> Self {
        let id = id.into();
        let mut events = AggregateEvents::default();
        events.raise(OrderPlaced {
            order_id: id.clone(),
            occurred_at: Utc::now(),
        });
        Self {
            id,
            version: 0,
            events,
        }
    }
}

impl Entity for Order {
    type Id = String;

    fn id(&self) -> &Self::Id {
        &self.id
    }
}

impl AggregateRoot for Order {
    type Event = OrderPlaced;

    fn pending_events(&self) -> &[Self::Event] {
        self.events.pending()
    }

    fn drain_events(&mut self) -> Vec<Self::Event> {
        self.events.drain()
    }

    fn restore_events(&mut self, events: Vec<Self::Event>) {
        self.events.restore(events);
    }

    fn version(&self) -> u64 {
        self.version
    }

    fn set_version(&mut self, version: u64) {
        self.version = version;
    }
}

struct CountingHandler {
    seen: Arc<Mutex<u32>>,
}

impl EventHandler<OrderPlaced> for CountingHandler {
    type Error = std::convert::Infallible;

    async fn handle(&self, _event: &OrderPlaced) -> Result<(), Self::Error> {
        *self.seen.lock().await += 1;
        Ok(())
    }
}

#[tokio::test]
async fn inline_delivery_saves_and_publishes_through_the_shared_backend()
-> Result<(), Box<dyn std::error::Error>> {
    let uow = InMemoryUnitOfWork::<Order>::new(Arc::new(InMemoryOutboxRepository::new()));
    let bus = EventBus::new();
    let seen = Arc::new(Mutex::new(0));
    bus.register::<OrderPlaced, _>(CountingHandler {
        seen: Arc::clone(&seen),
    });

    let store = DefaultAggregateStore::new(uow.clone(), uow.clone(), bus);
    let mut order = Order::place("order-1");

    store.save(&mut order).await?;

    assert_eq!(order.version(), 1);
    assert_eq!(*seen.lock().await, 1);
    assert!(
        uow.outbox().is_empty(),
        "inline delivery never touches the outbox"
    );

    let loaded = store
        .find(&"order-1".to_string())
        .await?
        .ok_or(pharos_app::StoreError::NotFound)?;
    assert_eq!(loaded.version(), 1);
    Ok(())
}

#[tokio::test]
async fn outbox_delivery_commits_aggregate_and_message_atomically()
-> Result<(), Box<dyn std::error::Error>> {
    let outbox = Arc::new(InMemoryOutboxRepository::new());
    let uow = InMemoryUnitOfWork::<Order>::new(Arc::clone(&outbox));
    let bus = EventBus::new();
    let signal = OutboxSignal::new();

    let store = DefaultAggregateStore::new(uow.clone(), uow.clone(), bus)
        .with_outbox("OrderPlaced", Some(signal.clone()));
    let mut order = Order::place("order-2");

    store.save(&mut order).await?;

    assert_eq!(order.version(), 1);
    let pending = outbox.pending(10).await?;
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].message.topic, "OrderPlaced");
    assert_eq!(pending[0].status, OutboxStatus::Pending);

    let loaded = store
        .find(&"order-2".to_string())
        .await?
        .ok_or(pharos_app::StoreError::NotFound)?;
    assert_eq!(loaded.version(), 1);

    tokio::time::timeout(std::time::Duration::from_secs(1), signal.notified())
        .await
        .map_err(|_| "outbox signal was not notified")?;
    Ok(())
}

#[tokio::test]
async fn concurrent_saves_on_the_same_aggregate_serialize_through_the_transaction_lock()
-> Result<(), Box<dyn std::error::Error>> {
    let outbox = Arc::new(InMemoryOutboxRepository::new());
    let uow = InMemoryUnitOfWork::<Order>::new(Arc::clone(&outbox));
    let bus = EventBus::new();
    let store = Arc::new(
        DefaultAggregateStore::new(uow.clone(), uow.clone(), bus).with_outbox("OrderPlaced", None),
    );

    // Seed the aggregate so both tasks race a *second* save (version 1 -> 2)
    // instead of both racing the initial create.
    let mut seed = Order::place("order-3");
    store.save(&mut seed).await?;

    let mut tasks = Vec::new();
    for _ in 0..8 {
        let store = Arc::clone(&store);
        tasks.push(tokio::spawn(async move {
            let mut order = store
                .find(&"order-3".to_string())
                .await?
                .ok_or(pharos_app::StoreError::NotFound)?;
            order.events.raise(OrderPlaced {
                order_id: order.id.clone(),
                occurred_at: Utc::now(),
            });
            store.save(&mut order).await
        }));
    }

    let mut succeeded = 0;
    let mut conflicted = 0;
    for task in tasks {
        let outcome: Result<(), pharos_app::StoreError> = task.await.map_err(|e| e.to_string())?;
        match outcome {
            Ok(()) => succeeded += 1,
            Err(pharos_app::StoreError::ConcurrencyConflict { .. }) => conflicted += 1,
            Err(other) => panic!("unexpected store error: {other}"),
        }
    }

    // The transaction lock serializes every save; each one reloads and
    // re-checks the version, so a stale reader loses to
    // `ConcurrencyConflict` instead of silently overwriting.
    assert_eq!(succeeded + conflicted, 8);
    assert!(succeeded >= 1);
    let final_order = store
        .find(&"order-3".to_string())
        .await?
        .ok_or(pharos_app::StoreError::NotFound)?;
    assert_eq!(final_order.version() as usize, 1 + succeeded);
    Ok(())
}
