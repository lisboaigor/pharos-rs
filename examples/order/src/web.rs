//! HTTP front end for the order example, built on **axum** + **tower**.
//!
//! This module shows how the very same command/query handlers exercised by
//! `main.rs` and the integration tests are exposed over HTTP, without leaking
//! any web concern into the application or domain layers:
//!
//! - [`pharos_axum`] provides the [`CommandHandlerState`] / [`QueryHandlerState`]
//!   extractors that pull the typed handler out of axum router state.
//! - [`pharos_app::dispatch`] / [`pharos_app::query_dispatch`] stay the single
//!   instrumentation seam, so every HTTP request is traced exactly like every
//!   in-process call — handlers carry no tracing and no HTTP code.
//! - A tower [`ServiceBuilder`] stack composes cross-cutting concerns (timeout,
//!   global concurrency limit, error mapping) around *all* routes at once.
//!
//! ```text
//! HTTP request
//!   └─ tower stack (HandleError → Timeout → ConcurrencyLimit)
//!        └─ axum route + extractors (Json / Query + handler from state)
//!             └─ pharos dispatch (command.handle / query.handle span)
//!                  └─ OrderHandlers / GetOrderTotalHandler (pure logic)
//! ```

use std::sync::Arc;
use std::time::Duration;

use axum::error_handling::HandleErrorLayer;
use axum::extract::{FromRef, Query as QueryParams};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use pharos_axum::{CommandHandlerState, QueryHandlerState};
use pharos_core::DomainError;
use pharos_infra::InMemoryRepository;
use tower::limit::GlobalConcurrencyLimitLayer;
use tower::timeout::TimeoutLayer;
use tower::{BoxError, ServiceBuilder};
use uuid::Uuid;

use crate::application::commands::{AddItem, ConfirmOrder, CreateOrder};
use crate::application::error::AppError;
use crate::application::handlers::OrderHandlers;
use crate::application::queries::{GetOrderTotal, GetOrderTotalHandler};
use crate::domain::order::Order;

/// Concrete repository used by the example HTTP server.
pub type Repo = InMemoryRepository<Order>;
/// Command handlers — one struct serves every order command.
pub type Commands = OrderHandlers<Repo>;
/// Read-side handler for order totals.
pub type Totals = GetOrderTotalHandler<Repo>;

/// Shared application state injected into every route.
///
/// Each handler is behind an [`Arc`] so the state is cheap to clone per request.
/// The [`FromRef`] impls let the [`pharos_axum`] extractors pick the right
/// handler out of the state by type.
#[derive(Clone)]
pub struct AppState {
    commands: Arc<Commands>,
    totals: Arc<Totals>,
}

impl AppState {
    /// Builds the state from already-shared handlers.
    pub fn new(commands: Arc<Commands>, totals: Arc<Totals>) -> Self {
        Self { commands, totals }
    }
}

impl FromRef<AppState> for Arc<Commands> {
    fn from_ref(state: &AppState) -> Self {
        Arc::clone(&state.commands)
    }
}

impl FromRef<AppState> for Arc<Totals> {
    fn from_ref(state: &AppState) -> Self {
        Arc::clone(&state.totals)
    }
}

/// Web-layer error wrapper that maps an [`AppError`] to a meaningful HTTP status
/// instead of a blanket `500`.
///
/// Keeping the mapping here — at the boundary — leaves the application error type
/// free of any HTTP knowledge.
pub struct ApiError(AppError);

impl From<AppError> for ApiError {
    fn from(error: AppError) -> Self {
        Self(error)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match &self.0 {
            AppError::Domain(DomainError::NotFound(_)) => StatusCode::NOT_FOUND,
            AppError::Domain(DomainError::Validation(_)) => StatusCode::BAD_REQUEST,
            AppError::Domain(DomainError::BusinessRule(_) | DomainError::Conflict(_)) => {
                StatusCode::CONFLICT
            }
            AppError::Infra(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (status, self.0.to_string()).into_response()
    }
}

/// `POST /orders` — creates an order and returns its id.
async fn create_order(
    handler: CommandHandlerState<CreateOrder, Commands>,
    Json(command): Json<CreateOrder>,
) -> Result<Json<Uuid>, ApiError> {
    let id = handler.dispatch(command).await?;
    Ok(Json(id))
}

/// `POST /orders/items` — adds a line item to an existing order.
async fn add_item(
    handler: CommandHandlerState<AddItem, Commands>,
    Json(command): Json<AddItem>,
) -> Result<StatusCode, ApiError> {
    handler.dispatch(command).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `POST /orders/confirm` — confirms an order.
async fn confirm_order(
    handler: CommandHandlerState<ConfirmOrder, Commands>,
    Json(command): Json<ConfirmOrder>,
) -> Result<StatusCode, ApiError> {
    handler.dispatch(command).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `GET /orders/total?order_id=...` — returns the order total in cents, or
/// `null` when the order does not exist.
async fn order_total(
    handler: QueryHandlerState<GetOrderTotal, Totals>,
    QueryParams(query): QueryParams<GetOrderTotal>,
) -> Result<Json<Option<u64>>, ApiError> {
    let total = handler.dispatch(query).await?;
    Ok(Json(total))
}

/// Builds the axum [`Router`] with the tower middleware stack applied.
///
/// Exposed so integration tests can drive it with `tower::ServiceExt::oneshot`
/// without binding a TCP socket.
pub fn router(state: AppState) -> Router {
    // `ServiceBuilder` applies layers top-to-bottom (outermost first):
    //   * `HandleErrorLayer` turns the `BoxError` produced deeper in the stack
    //     into an HTTP response. This is what lets non-`Infallible` tower layers
    //     (like `TimeoutLayer`) sit inside an axum router.
    //   * `TimeoutLayer` aborts any request exceeding the budget.
    //   * `GlobalConcurrencyLimitLayer` caps in-flight requests across every
    //     clone of the service, applying backpressure before handlers run.
    let middleware = ServiceBuilder::new()
        .layer(HandleErrorLayer::new(handle_middleware_error))
        .layer(TimeoutLayer::new(Duration::from_secs(5)))
        .layer(GlobalConcurrencyLimitLayer::new(128));

    Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/orders", post(create_order))
        .route("/orders/items", post(add_item))
        .route("/orders/confirm", post(confirm_order))
        .route("/orders/total", get(order_total))
        .layer(middleware)
        .with_state(state)
}

/// Converts errors surfaced by the tower middleware into HTTP responses.
async fn handle_middleware_error(error: BoxError) -> Response {
    if error.is::<tower::timeout::error::Elapsed>() {
        (StatusCode::REQUEST_TIMEOUT, "request timed out").into_response()
    } else {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("unhandled internal error: {error}"),
        )
            .into_response()
    }
}

/// Convenience constructor wiring an in-memory stack into [`AppState`].
///
/// Shared by the runnable binary and the integration tests so both exercise the
/// exact same composition.
pub fn in_memory_state(bus: pharos_app::EventBus) -> AppState {
    let repo = Arc::new(InMemoryRepository::<Order>::new());
    let commands = Arc::new(OrderHandlers::new(repo.clone(), bus));
    let totals = Arc::new(GetOrderTotalHandler::new(repo));

    AppState::new(commands, totals)
}
