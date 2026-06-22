use std::sync::Arc;

use pharos_app::{Command, CommandHandler, EventBus, save_and_publish};
use pharos_core::{DomainError, Entity, Repository};
use tracing::{Instrument, info_span};

use crate::application::error::AppError;

use crate::application::commands::{AddItem, ConfirmOrder, CreateOrder};
use crate::domain::order::Order;
use crate::domain::value_objects::{CustomerId, Money, OrderId, Quantity};

/// Loads an order or returns `NotFound`.
async fn load<R: Repository<Order>>(repo: &R, id: OrderId) -> Result<Order, AppError> {
    repo.find_by_id(&id)
        .await
        .map_err(AppError::infra)?
        .ok_or_else(|| AppError::Domain(DomainError::NotFound(format!("order {id}"))))
}

// ── CreateOrder ─────────────────────────────────────────────────────────────

pub struct CreateOrderHandler<R: Repository<Order>> {
    repo: Arc<R>,
    bus: EventBus,
}

impl<R: Repository<Order>> CreateOrderHandler<R> {
    pub fn new(repo: Arc<R>, bus: EventBus) -> Self {
        Self { repo, bus }
    }
}

impl<R: Repository<Order>> CommandHandler<CreateOrder> for CreateOrderHandler<R> {
    type Output = uuid::Uuid;
    type Error = AppError;

    async fn handle(&self, cmd: CreateOrder) -> Result<Self::Output, Self::Error> {
        async move {
            let mut order = Order::create(CustomerId::from_uuid(cmd.customer_id))?;
            let id = order.id().as_uuid();
            save_and_publish(&*self.repo, &self.bus, &mut order)
                .await
                .map_err(AppError::infra)?;
            Ok(id)
        }
        .instrument(info_span!(
            "command.handle",
            command = "CreateOrder",
            customer_id = %cmd.customer_id,
        ))
        .await
    }
}

// ── AddItem ─────────────────────────────────────────────────────────────────

pub struct AddItemHandler<R: Repository<Order>> {
    repo: Arc<R>,
    bus: EventBus,
}

impl<R: Repository<Order>> AddItemHandler<R> {
    pub fn new(repo: Arc<R>, bus: EventBus) -> Self {
        Self { repo, bus }
    }
}

impl<R: Repository<Order>> CommandHandler<AddItem> for AddItemHandler<R> {
    type Output = ();
    type Error = AppError;

    async fn handle(&self, cmd: AddItem) -> Result<Self::Output, Self::Error> {
        async move {
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
        .instrument(info_span!(
            "command.handle",
            command = "AddItem",
            order_id = %cmd.order_id,
            quantity = cmd.quantity,
            unit_price_reais = cmd.unit_price_reais,
        ))
        .await
    }
}

// ── ConfirmOrder ────────────────────────────────────────────────────────────

pub struct ConfirmOrderHandler<R: Repository<Order>> {
    repo: Arc<R>,
    bus: EventBus,
}

impl<R: Repository<Order>> ConfirmOrderHandler<R> {
    pub fn new(repo: Arc<R>, bus: EventBus) -> Self {
        Self { repo, bus }
    }
}

impl<R: Repository<Order>> CommandHandler<ConfirmOrder> for ConfirmOrderHandler<R> {
    type Output = ();
    type Error = AppError;

    async fn handle(&self, cmd: ConfirmOrder) -> Result<Self::Output, Self::Error> {
        async move {
            let mut order = load(&*self.repo, OrderId::from_uuid(cmd.order_id)).await?;
            order.confirm()?;
            save_and_publish(&*self.repo, &self.bus, &mut order)
                .await
                .map_err(AppError::infra)?;
            Ok(())
        }
        .instrument(info_span!(
            "command.handle",
            command = "ConfirmOrder",
            order_id = %cmd.order_id,
        ))
        .await
    }
}

// Ensures the command DTOs really implement Command (type-level check).
const _: fn() = || {
    fn assert_command<C: Command>() {}
    assert_command::<CreateOrder>();
    assert_command::<AddItem>();
    assert_command::<ConfirmOrder>();
};
