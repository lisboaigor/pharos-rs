use chrono::Utc;

use pharos_core::{AggregateEvents, DomainError, DomainResult};
use pharos_macros::{AggregateRoot, Entity};

use super::events::OrderEvent;
use super::value_objects::{CustomerId, ItemId, Money, OrderId, Quantity};

// ── Item (aggregate-internal entity) ────────────────────────────────────────

#[derive(Debug, Clone, Entity)]
pub struct OrderItem {
    #[id]
    id: ItemId,
    pub description: String,
    pub quantity: Quantity,
    pub unit_price: Money,
}

impl OrderItem {
    /// Rebuilds an `OrderItem` from trusted persistence state.
    pub fn rehydrate(
        id: ItemId,
        description: String,
        quantity: Quantity,
        unit_price: Money,
    ) -> Self {
        Self {
            id,
            description,
            quantity,
            unit_price,
        }
    }

    pub fn subtotal(&self) -> Money {
        self.unit_price.mul(self.quantity.value())
    }
}

// ── State ───────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderStatus {
    Draft,
    Confirmed,
    Cancelled,
}

// ── Aggregate Root ──────────────────────────────────────────────────────────

/// `#[derive(Entity)]` reads the `#[id]` field; `#[derive(AggregateRoot)]`
/// reads `#[events]` and infers the event type. `Clone` exists only because
/// the example uses an in-memory repository.
#[derive(Debug, Clone, Entity, AggregateRoot)]
pub struct Order {
    #[id]
    id: OrderId,
    #[version]
    version: u64,
    customer_id: CustomerId,
    items: Vec<OrderItem>,
    status: OrderStatus,
    #[events]
    events: AggregateEvents<OrderEvent>,
}

impl Order {
    /// Rehydrates an order from trusted persistence state without emitting events.
    ///
    /// Infrastructure adapters use this to rebuild the aggregate from relational
    /// rows. Application code should prefer behavior methods such as `create`,
    /// `add_item`, `confirm`, and `cancel`.
    pub fn rehydrate(
        id: OrderId,
        version: u64,
        customer_id: CustomerId,
        items: Vec<OrderItem>,
        status: OrderStatus,
    ) -> Self {
        Self {
            id,
            version,
            customer_id,
            items,
            status,
            events: AggregateEvents::default(),
        }
    }

    /// Factory method — the single creation entry point. Emits `OrderCreated`.
    pub fn create(customer_id: CustomerId) -> DomainResult<Self> {
        let id = OrderId::new();
        let mut order = Self {
            id,
            version: 0,
            customer_id,
            items: Vec::new(),
            status: OrderStatus::Draft,
            events: AggregateEvents::default(),
        };
        order.events.raise(OrderEvent::OrderCreated {
            order_id: id.to_string(),
            customer_id: customer_id.as_uuid(),
            occurred_at: Utc::now(),
        });
        Ok(order)
    }

    pub fn add_item(
        &mut self,
        description: String,
        quantity: Quantity,
        unit_price: Money,
    ) -> DomainResult<ItemId> {
        self.ensure_draft()?;
        let item_id = ItemId::new();

        self.events.raise(OrderEvent::ItemAdded {
            order_id: self.id.to_string(),
            item_id: item_id.as_uuid(),
            description: description.clone(),
            quantity: quantity.value(),
            unit_price_cents: unit_price.cents(),
            occurred_at: Utc::now(),
        });

        self.items.push(OrderItem {
            id: item_id,
            description,
            quantity,
            unit_price,
        });
        Ok(item_id)
    }

    pub fn confirm(&mut self) -> DomainResult<()> {
        self.ensure_draft()?;
        if self.items.is_empty() {
            return Err(DomainError::BusinessRule(
                "an order without items cannot be confirmed".into(),
            ));
        }
        let total = self.total()?;
        self.status = OrderStatus::Confirmed;
        self.events.raise(OrderEvent::OrderConfirmed {
            order_id: self.id.to_string(),
            total_cents: total.cents(),
            occurred_at: Utc::now(),
        });
        Ok(())
    }

    pub fn cancel(&mut self, reason: String) -> DomainResult<()> {
        if self.status == OrderStatus::Cancelled {
            return Err(DomainError::BusinessRule(
                "order is already cancelled".into(),
            ));
        }
        self.status = OrderStatus::Cancelled;
        self.events.raise(OrderEvent::OrderCancelled {
            order_id: self.id.to_string(),
            reason,
            occurred_at: Utc::now(),
        });
        Ok(())
    }

    // ── Queries ─────────────────────────────────────────────────────────────

    pub fn status(&self) -> OrderStatus {
        self.status
    }

    pub fn items(&self) -> &[OrderItem] {
        &self.items
    }

    pub fn customer_id(&self) -> CustomerId {
        self.customer_id
    }

    pub fn total(&self) -> DomainResult<Money> {
        self.items
            .iter()
            .try_fold(Money::zero(), |acc, item| acc.add(&item.subtotal()))
    }

    // ── Invariant ───────────────────────────────────────────────────────────

    fn ensure_draft(&self) -> DomainResult<()> {
        if self.status != OrderStatus::Draft {
            return Err(DomainError::BusinessRule(
                "operation allowed only in Draft status".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pharos_core::AggregateRoot;

    fn new_order() -> Order {
        let Ok(order) = Order::create(CustomerId::new()) else {
            panic!("Order::create with a valid CustomerId is always valid");
        };
        order
    }

    #[test]
    fn create_emits_event() {
        let p = new_order();
        assert_eq!(p.status(), OrderStatus::Draft);
        assert_eq!(p.pending_events().len(), 1);
    }

    #[test]
    fn total_uses_integer_arithmetic() -> Result<(), Box<dyn std::error::Error>> {
        let mut p = new_order();
        p.add_item("A".into(), Quantity::new(3)?, Money::brl(10.10)?)?;
        // 3 * 1010 cents = 3030, with no floating-point error
        assert_eq!(p.total()?.cents(), 3030);
        Ok(())
    }

    #[test]
    fn confirm_without_items_fails() {
        let mut p = new_order();
        assert!(p.confirm().is_err());
    }

    #[test]
    fn does_not_add_item_after_confirmation() -> Result<(), Box<dyn std::error::Error>> {
        let mut p = new_order();
        p.add_item("A".into(), Quantity::new(1)?, Money::brl(5.0)?)?;
        p.confirm()?;
        let r = p.add_item("B".into(), Quantity::new(1)?, Money::brl(5.0)?);
        assert!(r.is_err());
        Ok(())
    }

    #[test]
    fn cancel_changes_status_and_emits_event() -> Result<(), Box<dyn std::error::Error>> {
        let mut p = new_order();
        let _ = p.drain_events(); // discard OrderCreated
        p.cancel("customer changed their mind".into())?;
        assert_eq!(p.status(), OrderStatus::Cancelled);
        assert!(p.items().is_empty());
        assert_eq!(p.pending_events().len(), 1);
        // second cancellation fails
        assert!(p.cancel("again".into()).is_err());
        Ok(())
    }

    #[test]
    fn exposed_getters() -> Result<(), Box<dyn std::error::Error>> {
        let mut p = new_order();
        let customer = p.customer_id();
        p.add_item("X".into(), Quantity::new(1)?, Money::brl(1.0)?)?;
        assert_eq!(p.items().len(), 1);
        assert_eq!(p.customer_id(), customer);
        Ok(())
    }

    #[test]
    fn drain_clears_events() {
        let mut p = new_order();
        let evs = p.drain_events();
        assert_eq!(evs.len(), 1);
        assert!(p.pending_events().is_empty());
    }
}
