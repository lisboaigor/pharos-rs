//! Verifies that an event from the orders context drives the billing context.

use modular_monolith::Monolith;

#[tokio::test]
async fn placing_an_order_issues_an_invoice_in_billing() {
    let app = Monolith::new();

    // No invoice before the order exists.
    assert!(app.invoice_for("order-9").await.is_none());

    app.place_order("order-9", "Grace Hopper", 9_900)
        .await
        .expect("place order");

    // The billing context reacted to OrderPlaced and issued a matching invoice.
    let invoice = app
        .invoice_for("order-9")
        .await
        .expect("billing should have issued an invoice");
    assert_eq!(invoice.order_id(), "order-9");
    assert_eq!(invoice.amount_cents(), 9_900);
}
