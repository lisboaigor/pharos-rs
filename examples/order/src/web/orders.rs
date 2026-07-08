use axum::Json;
use axum::extract::Query as QueryParams;
use axum::http::StatusCode;
use pharos_axum::{CommandHandlerState, QueryHandlerState};
use uuid::Uuid;

use crate::application::commands::{AddItem, ConfirmOrder, CreateOrder};
use crate::application::queries::GetOrderTotal;

use super::error::ApiError;
use super::state::{Commands, Totals};

/// `POST /orders` — creates an order and returns its id.
pub async fn create_order(
    handler: CommandHandlerState<CreateOrder, Commands>,
    Json(command): Json<CreateOrder>,
) -> Result<Json<Uuid>, ApiError> {
    let id = handler.dispatch(command).await?;
    Ok(Json(id))
}

/// `POST /orders/items` — adds a line item to an existing order.
pub async fn add_item(
    handler: CommandHandlerState<AddItem, Commands>,
    Json(command): Json<AddItem>,
) -> Result<StatusCode, ApiError> {
    handler.dispatch(command).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `POST /orders/confirm` — confirms an order.
pub async fn confirm_order(
    handler: CommandHandlerState<ConfirmOrder, Commands>,
    Json(command): Json<ConfirmOrder>,
) -> Result<StatusCode, ApiError> {
    handler.dispatch(command).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `GET /orders/total?order_id=...` — returns the order total in cents, or
/// `null` when the order does not exist.
pub async fn order_total(
    handler: QueryHandlerState<GetOrderTotal, Totals>,
    QueryParams(query): QueryParams<GetOrderTotal>,
) -> Result<Json<Option<u64>>, ApiError> {
    let total = handler.dispatch(query).await?;
    Ok(Json(total))
}
