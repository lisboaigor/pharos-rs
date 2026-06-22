use pharos_app::QueryHandler;
use pharos_core::Repository;
use pharos_macros::Query;
use serde::Deserialize;
use std::sync::Arc;
use uuid::Uuid;

use crate::application::error::AppError;
use crate::domain::order::Order;
use crate::domain::value_objects::OrderId;

/// Read query: returns the order total in cents.
///
/// `Deserialize` lets the web example parse it straight from the URL query
/// string (`/orders/total?order_id=...`).
#[derive(Query, Deserialize)]
#[query(result = Option<u64>)]
pub struct GetOrderTotal {
    #[trace(display)]
    pub order_id: Uuid,
}

pub struct GetOrderTotalHandler<R: Repository<Order>> {
    repo: Arc<R>,
}

impl<R: Repository<Order>> GetOrderTotalHandler<R> {
    pub fn new(repo: Arc<R>) -> Self {
        Self { repo }
    }
}

impl<R: Repository<Order>> QueryHandler<GetOrderTotal> for GetOrderTotalHandler<R> {
    type Error = AppError;

    async fn handle(&self, q: GetOrderTotal) -> Result<Option<u64>, Self::Error> {
        let id = OrderId::from_uuid(q.order_id);
        match self.repo.find_by_id(&id).await.map_err(AppError::infra)? {
            Some(order) => Ok(Some(order.total()?.cents())),
            None => Ok(None),
        }
    }
}
