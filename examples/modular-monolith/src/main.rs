//! Runnable modular-monolith demo.
//!
//! Placing an order in the `orders` context drives the `billing` context to
//! issue an invoice — all in one process, connected by the in-process event bus.

use modular_monolith::Monolith;

#[tokio::main]
async fn main() {
    let app = Monolith::new();

    app.place_order("order-1", "Ada Lovelace", 4_200)
        .await
        .expect("place order");

    match app.invoice_for("order-1").await {
        Some(invoice) => println!(
            "billing reacted: invoice for {} of {} cents",
            invoice.order_id(),
            invoice.amount_cents()
        ),
        None => println!("no invoice was issued"),
    }
}
