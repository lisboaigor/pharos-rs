use pharos_app::EventHandler;
use tracing::info;

use crate::domain::events::OrderEvent;

/// Side effect: notifies the customer. In the example it only logs.
pub struct NotifyCustomer;

impl EventHandler<OrderEvent> for NotifyCustomer {
    type Error = std::convert::Infallible;

    async fn handle(&self, event: &OrderEvent) -> Result<(), Self::Error> {
        match event {
            OrderEvent::OrderCreated { customer_id, .. } => {
                info!(customer_id = %customer_id, "welcome notification sent to customer");
            }
            OrderEvent::OrderConfirmed { total_cents, .. } => {
                info!(
                    total_cents = *total_cents,
                    total_reais = format!("{}.{:02}", total_cents / 100, total_cents % 100),
                    "customer notified about confirmed order"
                );
            }
            _ => {}
        }
        Ok(())
    }
}

/// Side effect: adjusts inventory when items are added.
pub struct UpdateInventory;

impl EventHandler<OrderEvent> for UpdateInventory {
    type Error = std::convert::Infallible;

    async fn handle(&self, event: &OrderEvent) -> Result<(), Self::Error> {
        if let OrderEvent::ItemAdded {
            description,
            quantity,
            ..
        } = event
        {
            info!(description = %description, quantity = *quantity, "inventory reserved for order item");
        }
        Ok(())
    }
}
