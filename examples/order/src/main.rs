//! Runnable axum + tower web server for the order example.
//!
//! Run with `cargo run -p order`, then exercise it:
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

use std::error::Error;

use order::application::event_handlers::{NotifyCustomer, UpdateInventory};
use order::domain::events::OrderEvent;
use order::web::{in_memory_state, router};
use pharos_app::EventBus;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    // One call installs logging, the filter that keeps the framework's own
    // spans, and — with OTEL_EXPORTER_OTLP_ENDPOINT set — trace export. The
    // guard flushes pending spans on the way out.
    let _observability = pharos_observability::init("order")?;
    let metrics = pharos_observability::http::http_metrics();

    // In-process domain event handlers run synchronously after each command.
    let bus = EventBus::new();

    bus.register::<OrderEvent, _>(NotifyCustomer);
    bus.register::<OrderEvent, _>(UpdateInventory);

    // `instrument` applies the observability layers in the one order where the
    // request span and its exemplars both work; getting it wrong fails silently.
    let app = pharos_observability::http::instrument(router(in_memory_state(bus)), metrics);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;

    info!(addr = %listener.local_addr()?, "order web server listening");

    axum::serve(listener, app).await?;

    Ok(())
}
