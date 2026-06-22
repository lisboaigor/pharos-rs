//! Runnable axum + tower web server for the order example.
//!
//! Run with `cargo run -p order --bin web`, then exercise it:
//!
//! ```sh
//! # create an order
//! curl -s -XPOST localhost:3000/orders \
//!   -H 'content-type: application/json' \
//!   -d "{\"customer_id\":\"$(uuidgen | tr 'A-Z' 'a-z')\"}"
//!
//! # add an item (use the id returned above)
//! curl -s -XPOST localhost:3000/orders/items \
//!   -H 'content-type: application/json' \
//!   -d '{"order_id":"<id>","description":"Keyboard","quantity":2,"unit_price_reais":350.0}'
//!
//! # read the total (in cents)
//! curl -s "localhost:3000/orders/total?order_id=<id>"
//! ```
//!
//! The HTTP wiring lives in [`order::web`]; this binary only builds the
//! in-process infrastructure and serves the router.

use order::application::event_handlers::{NotifyCustomer, UpdateInventory};
use order::domain::events::OrderEvent;
use order::web::{in_memory_state, router};
use pharos_app::EventBus;
use tracing::info;
use tracing_subscriber::{EnvFilter, fmt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
        .with_target(false)
        .compact()
        .init();

    // In-process domain event handlers run synchronously after each command,
    // exactly as in the CLI flow — the web layer changes nothing here.
    let bus = EventBus::new();
    bus.register::<OrderEvent, _>(NotifyCustomer);
    bus.register::<OrderEvent, _>(UpdateInventory);

    let app = router(in_memory_state(bus));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;
    info!(addr = %listener.local_addr()?, "order web server listening");
    axum::serve(listener, app).await?;
    Ok(())
}
