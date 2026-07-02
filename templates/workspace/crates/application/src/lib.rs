use domain::Order;
use pharos::prelude::*;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct PlaceOrder {
    pub customer_name: String,
}

impl Command for PlaceOrder {}

#[derive(thiserror::Error, Debug)]
pub enum PlaceOrderError {
    #[error("repository failed: {0}")]
    Repository(String),
}

pub struct PlaceOrderHandler<R> {
    repo: R,
}

impl<R> PlaceOrderHandler<R> {
    pub fn new(repo: R) -> Self {
        Self { repo }
    }
}

impl<R> CommandHandler<PlaceOrder> for PlaceOrderHandler<R>
where
    R: Repository<Order>,
{
    type Output = Order;
    type Error = PlaceOrderError;

    async fn handle(&self, command: PlaceOrder) -> Result<Self::Output, Self::Error> {
        let mut order = Order::place(command.customer_name);
        self.repo
            .save(&mut order)
            .await
            .map_err(|error| PlaceOrderError::Repository(error.to_string()))?;
        Ok(order)
    }
}
