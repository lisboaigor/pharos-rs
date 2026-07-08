//! Modular monolith example for Pharos RS.
//!
//! Two bounded contexts — [`orders`] and [`billing`] — live in one process and
//! one binary. Each owns its aggregate and events and does not depend on the
//! other's domain model. They are wired together here, in the composition root,
//! through the in-process [`EventBus`]: placing an order publishes
//! [`OrderPlaced`], and a subscriber issues the matching
//! invoice in the billing context.
//!
//! This is the structural pattern for scaling to many bounded contexts in a
//! single deployable: keep contexts independent, wire them at the edges.

use std::sync::Arc;

use pharos_app::{ApplicationError, EventBus, EventHandler, save_and_publish};
use pharos_memory::InMemoryRepository;

pub mod billing;
pub mod orders;

use billing::{BillingError, Invoice, issue_invoice};
use orders::{Order, OrderPlaced};

/// Cross-context subscriber: when the orders context publishes [`OrderPlaced`],
/// the billing context issues an invoice.
///
/// It lives in the composition root, not inside `billing`, so neither context
/// has to know about the other.
pub struct IssueInvoiceOnOrderPlaced {
    invoices: Arc<InMemoryRepository<Invoice>>,
}

impl EventHandler<OrderPlaced> for IssueInvoiceOnOrderPlaced {
    type Error = BillingError;

    async fn handle(&self, event: &OrderPlaced) -> Result<(), Self::Error> {
        issue_invoice(&*self.invoices, &event.order_id, event.total_cents).await?;
        Ok(())
    }
}

/// The wired application: both contexts plus the event bus connecting them.
pub struct Monolith {
    orders: Arc<InMemoryRepository<Order>>,
    invoices: Arc<InMemoryRepository<Invoice>>,
    bus: EventBus,
}

impl Default for Monolith {
    fn default() -> Self {
        Self::new()
    }
}

impl Monolith {
    /// Wires the contexts together, subscribing billing to order events.
    pub fn new() -> Self {
        let orders = Arc::new(InMemoryRepository::new());
        let invoices = Arc::new(InMemoryRepository::new());
        let bus = EventBus::new();
        bus.register::<OrderPlaced, _>(IssueInvoiceOnOrderPlaced {
            invoices: Arc::clone(&invoices),
        });
        Self {
            orders,
            invoices,
            bus,
        }
    }

    /// Places an order and publishes its events, which drives billing.
    pub async fn place_order(
        &self,
        id: impl Into<String>,
        customer: impl Into<String>,
        total_cents: i64,
    ) -> Result<(), ApplicationError> {
        let mut order = Order::place(id, customer, total_cents);
        save_and_publish(&*self.orders, &self.bus, &mut order).await
    }

    /// Returns the invoice issued for an order, if billing has processed it.
    pub async fn invoice_for(&self, order_id: &str) -> Option<Invoice> {
        use pharos_core::Repository;
        self.invoices
            .find_by_id(&format!("inv-{order_id}"))
            .await
            .ok()
            .flatten()
    }
}
