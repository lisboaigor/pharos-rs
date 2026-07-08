//! Demonstrates asserting on published domain events with `pharos-testing`,
//! and verifies optimistic-concurrency behavior of the in-memory repository.

use std::sync::Arc;

use order::application::commands::{AddItem, ConfirmOrder, CreateOrder};
use order::domain::events::OrderEvent;
use order::domain::order::Order;
use order::domain::value_objects::OrderId;
use pharos_app::{EventBus, dispatch, save_and_publish};
use pharos_core::{AggregateRoot, Entity, Repository, RepositoryError};
use pharos_memory::InMemoryRepository;
use pharos_testing::{EventCapture, assert_event_published};
use uuid::Uuid;

use order::application::handlers::OrderHandlers;

#[tokio::test]
async fn full_command_flow_publishes_expected_domain_events()
-> Result<(), Box<dyn std::error::Error>> {
    let repo = Arc::new(InMemoryRepository::<Order>::new());
    let bus = EventBus::new();

    // Capture every OrderEvent the flow publishes.
    let capture = EventCapture::<OrderEvent>::new();
    capture.register_on(&bus);

    let handlers = OrderHandlers::new(repo.clone(), bus.clone());

    let order_id = dispatch(
        &handlers,
        CreateOrder {
            customer_id: Uuid::now_v7(),
        },
    )
    .await?;

    dispatch(
        &handlers,
        AddItem {
            order_id,
            description: "Keyboard".into(),
            quantity: 1,
            unit_price_reais: 100.0,
        },
    )
    .await?;

    dispatch(&handlers, ConfirmOrder { order_id }).await?;

    // OrderCreated, ItemAdded, OrderConfirmed.
    assert_event_published!(capture, 3);

    let events = capture.events();
    assert!(matches!(events[0], OrderEvent::OrderCreated { .. }));
    assert!(matches!(events[1], OrderEvent::ItemAdded { .. }));
    assert!(matches!(events[2], OrderEvent::OrderConfirmed { .. }));

    // The persisted aggregate advanced its version once per save.
    let stored = repo
        .find_by_id(&OrderId::from_uuid(order_id))
        .await?
        .ok_or("order not found in repository")?;
    assert_eq!(stored.version(), 3);
    Ok(())
}

#[tokio::test]
async fn stale_write_is_rejected_with_concurrency_conflict()
-> Result<(), Box<dyn std::error::Error>> {
    let repo = InMemoryRepository::<Order>::new();
    let bus = EventBus::new();

    // Persist an order once (version 0 -> 1).
    let mut writer_a = Order::create(crate_customer())?;
    save_and_publish(&repo, &bus, &mut writer_a).await?;
    assert_eq!(writer_a.version(), 1);

    // A second in-memory copy still believes it is at version 1 after another
    // writer has already advanced storage to version 2.
    let mut writer_b = repo
        .find_by_id(writer_a.id())
        .await?
        .ok_or("order not found")?;
    save_and_publish(&repo, &bus, &mut writer_a).await?; // storage -> 2

    // writer_b is now stale; its save must be rejected.
    let Err(conflict) = repo.save(&mut writer_b).await else {
        panic!("stale writer_b must be rejected with ConcurrencyConflict");
    };
    assert!(matches!(
        conflict,
        RepositoryError::ConcurrencyConflict {
            expected: 1,
            actual: Some(2)
        }
    ));
    Ok(())
}

fn crate_customer() -> order::domain::value_objects::CustomerId {
    order::domain::value_objects::CustomerId::new()
}
