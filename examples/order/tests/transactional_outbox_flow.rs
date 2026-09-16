//! Demonstrates the atomic save+enqueue seam (`TransactionalRepository` +
//! `TransactionalStore`, composed by `save_and_enqueue_in`) against
//! `pharos_memory::InMemoryUnitOfWork` — the same shape you'd implement
//! against a real database (see `docs/guide/writing-an-adapter.md`), unlike
//! `outbox_flow.rs`'s `save_and_enqueue`, which writes the aggregate and the
//! outbox as two independent, non-transactional steps.

use std::sync::Arc;

use order::domain::order::{Order, OrderStatus};
use order::domain::value_objects::{CustomerId, Money, Quantity};
use pharos_app::{Message, OutboxRepository, SaveAndEnqueueError, save_and_enqueue_in};
use pharos_core::{AggregateRoot, Entity, Repository, RepositoryError};
use pharos_memory::{InMemoryOutboxRepository, InMemoryUnitOfWork};

fn to_message(
    event: &order::domain::events::OrderEvent,
) -> Result<Message, std::convert::Infallible> {
    use pharos_core::DomainEvent;
    Ok(
        Message::new("order-events", b"{}".to_vec(), "application/json")
            .with_key(event.aggregate_id())
            .with_header("event_type", event.event_type()),
    )
}

#[tokio::test]
async fn save_and_enqueue_in_commits_the_order_and_the_outbox_message_together()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let outbox = Arc::new(InMemoryOutboxRepository::new());
    let uow = InMemoryUnitOfWork::<Order>::new(Arc::clone(&outbox));

    let mut order = Order::create(CustomerId::new())?;
    order.add_item(
        "Rust book".to_string(),
        Quantity::new(1)?,
        Money::from_cents(4_500),
    )?;
    order.confirm()?;
    let order_id = *order.id();

    save_and_enqueue_in(&uow, &uow, &mut order, to_message).await?;

    assert_eq!(order.version(), 1);
    assert!(
        order.pending_events().is_empty(),
        "events are drained only after the transaction commits"
    );

    let loaded = uow.find_by_id(&order_id).await?.ok_or("order not found")?;
    assert_eq!(loaded.status(), OrderStatus::Confirmed);

    let pending = outbox.pending(10).await?;
    assert!(
        !pending.is_empty(),
        "the outbox insert must be visible in the same read that sees the committed order"
    );

    Ok(())
}

#[tokio::test]
async fn a_rejected_save_leaves_neither_the_order_nor_an_outbox_message_behind()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let outbox = Arc::new(InMemoryOutboxRepository::new());
    let uow = InMemoryUnitOfWork::<Order>::new(Arc::clone(&outbox));

    // Never saved, so any nonzero expected version is stale.
    let mut order = Order::create(CustomerId::new())?;
    let order_id = *order.id();
    order.set_version(7);

    let result = save_and_enqueue_in(&uow, &uow, &mut order, to_message).await;

    assert!(matches!(
        result,
        Err(SaveAndEnqueueError::Repository(
            RepositoryError::ConcurrencyConflict { .. }
        ))
    ));
    assert!(
        uow.find_by_id(&order_id).await?.is_none(),
        "a rejected save must not create the order"
    );
    assert!(
        outbox.pending(10).await?.is_empty(),
        "a rejected save must never leave a dangling outbox message behind"
    );
    assert_eq!(
        order.version(),
        7,
        "a rejected save must revert the aggregate's in-memory version for a clean retry"
    );

    Ok(())
}
