use pharos_app::{Query, QueryHandler};
use pharos_core::Repository;
use std::sync::Arc;
use tracing::{Instrument, info_span};
use uuid::Uuid;

use crate::application::error::AppError;
use crate::domain::order::Order;
use crate::domain::value_objects::OrderId;

/// Read query: returns the order total in cents.
pub struct GetOrderTotal {
    pub order_id: Uuid,
}
impl Query for GetOrderTotal {
    type Result = Option<u64>;
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
        async move {
            let id = OrderId::from_uuid(q.order_id);
            match self.repo.find_by_id(&id).await.map_err(AppError::infra)? {
                Some(order) => Ok(Some(order.total()?.cents())),
                None => Ok(None),
            }
        }
        .instrument(info_span!(
            "query.handle",
            query = "GetOrderTotal",
            order_id = %q.order_id,
        ))
        .await
    }
}
