use std::sync::Arc;

use pharos_app::{Command, CommandHandler, EventBus, save_and_publish};
use pharos_core::{DomainError, Entity, Repository};
use uuid::Uuid;

use crate::application::commands::{AddItem, ConfirmOrder, CreateOrder};
use crate::application::error::AppError;
use crate::domain::order::Order;
use crate::domain::value_objects::{CustomerId, Money, OrderId, Quantity};

/// Loads an order or returns `NotFound`.
async fn load<R: Repository<Order>>(repo: &R, id: OrderId) -> Result<Order, AppError> {
    repo.find_by_id(&id)
        .await
        .map_err(AppError::infra)?
        .ok_or_else(|| AppError::Domain(DomainError::NotFound(format!("order {id}"))))
}

/// Command handlers for the `Order` aggregate.
///
/// A single struct serves every `Order` command: the dependencies (`repo`,
/// `bus`) are identical across them. Tracing is applied by
/// [`pharos_app::dispatch`], so each `handle` is pure business logic — no
/// `async move`, no `info_span!`, no `.instrument(..)`.
pub struct OrderHandlers<R: Repository<Order>> {
    repo: Arc<R>,
    bus: EventBus,
}

impl<R: Repository<Order>> OrderHandlers<R> {
    pub fn new(repo: Arc<R>, bus: EventBus) -> Self {
        Self { repo, bus }
    }
}

impl<R: Repository<Order>> CommandHandler<CreateOrder> for OrderHandlers<R> {
    type Output = Uuid;
    type Error = AppError;

    async fn handle(&self, cmd: CreateOrder) -> Result<Self::Output, Self::Error> {
        let mut order = Order::create(CustomerId::from_uuid(cmd.customer_id))?;
        let id = order.id().as_uuid();

        save_and_publish(&*self.repo, &self.bus, &mut order)
            .await
            .map_err(AppError::infra)?;

        Ok(id)
    }
}

impl<R: Repository<Order>> CommandHandler<AddItem> for OrderHandlers<R> {
    type Output = ();
    type Error = AppError;

    async fn handle(&self, cmd: AddItem) -> Result<Self::Output, Self::Error> {
        let mut order = load(&*self.repo, OrderId::from_uuid(cmd.order_id)).await?;

        order.add_item(
            cmd.description,
            Quantity::new(cmd.quantity)?,
            Money::brl(cmd.unit_price_reais)?,
        )?;

        save_and_publish(&*self.repo, &self.bus, &mut order)
            .await
            .map_err(AppError::infra)?;

        Ok(())
    }
}

impl<R: Repository<Order>> CommandHandler<ConfirmOrder> for OrderHandlers<R> {
    type Output = ();
    type Error = AppError;

    async fn handle(&self, cmd: ConfirmOrder) -> Result<Self::Output, Self::Error> {
        let mut order = load(&*self.repo, OrderId::from_uuid(cmd.order_id)).await?;

        order.confirm()?;

        save_and_publish(&*self.repo, &self.bus, &mut order)
            .await
            .map_err(AppError::infra)?;

        Ok(())
    }
}

// Ensures the command DTOs really implement Command (type-level check).
const _: fn() = || {
    fn assert_command<C: Command>() {}
    assert_command::<CreateOrder>();
    assert_command::<AddItem>();
    assert_command::<ConfirmOrder>();
};
