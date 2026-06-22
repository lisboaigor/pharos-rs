//! Executable example: lifecycle of an order aggregate with domain events.
//! Run with `cargo run -p order`.

// The aggregate exposes a broader API (cancel, getters) than this
// demonstration flow exercises; tests cover the rest.
#![allow(dead_code)]

use std::sync::Arc;

use pharos_app::{CommandHandler, EventBus, QueryHandler};
use pharos_infra::InMemoryRepository;
use tracing::info;
use tracing_subscriber::{EnvFilter, fmt};

use order::application::commands::{AddItem, ConfirmOrder, CreateOrder};
use order::application::event_handlers::{NotifyCustomer, UpdateInventory};
use order::application::handlers::{AddItemHandler, ConfirmOrderHandler, CreateOrderHandler};
use order::application::queries::{GetOrderTotal, GetOrderTotalHandler};
use order::domain::events::OrderEvent;
use order::domain::order::Order;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
        .with_target(false)
        .compact()
        .init();

    // ── Infrastructure ──────────────────────────────────────────────────────
    // `EventBus` is a concrete, cheaply cloneable type; clones share handlers.
    let bus = EventBus::new();
    bus.register::<OrderEvent, _>(NotifyCustomer);
    bus.register::<OrderEvent, _>(UpdateInventory);

    let repo = Arc::new(InMemoryRepository::<Order>::new());

    // ── Handlers (explicit DI) ──────────────────────────────────────────────
    let create = CreateOrderHandler::new(repo.clone(), bus.clone());
    let add_item = AddItemHandler::new(repo.clone(), bus.clone());
    let confirm = ConfirmOrderHandler::new(repo.clone(), bus.clone());

    // ── Flow ────────────────────────────────────────────────────────────────
    let customer_id = uuid::Uuid::now_v7();
    let order_id = create.handle(CreateOrder { customer_id }).await?;
    info!(order_id = %order_id, "order created");

    add_item
        .handle(AddItem {
            order_id,
            description: "Mechanical keyboard".into(),
            quantity: 2,
            unit_price_reais: 350.00,
        })
        .await?;

    add_item
        .handle(AddItem {
            order_id,
            description: "Mousepad".into(),
            quantity: 1,
            unit_price_reais: 80.00,
        })
        .await?;

    confirm.handle(ConfirmOrder { order_id }).await?;

    let query = GetOrderTotalHandler::new(repo.clone());
    let total = query.handle(GetOrderTotal { order_id }).await?;

    match total {
        Some(total_centavos) => info!(
            order_id = %order_id,
            total_cents = total_centavos,
            orders_in_memory = repo.len(),
            "flow completed"
        ),
        None => info!(
            order_id = %order_id,
            orders_in_memory = repo.len(),
            "flow completed without a total"
        ),
    }
    Ok(())
}
