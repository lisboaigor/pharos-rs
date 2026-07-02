//! HTTP front end for the order example, built on **axum** + **tower**.
//!
//! This module shows how command/query handlers are exposed over HTTP without
//! leaking any web concern into the application or domain layers:
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

mod error;
mod orders;
mod state;

pub use state::AppState;

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::error_handling::HandleErrorLayer;
use axum::routing::{get, post};
use pharos_infra::InMemoryRepository;
use tower::ServiceBuilder;
use tower::limit::GlobalConcurrencyLimitLayer;
use tower::timeout::TimeoutLayer;

use crate::application::handlers::OrderHandlers;
use crate::application::queries::GetOrderTotalHandler;
use crate::domain::order::Order;

use error::handle_middleware_error;
use orders::{add_item, confirm_order, create_order, order_total};

/// Builds the axum [`Router`] with the tower middleware stack applied.
///
/// Exposed so integration tests can drive it with `tower::ServiceExt::oneshot`
/// without binding a TCP socket.
///
/// `ServiceBuilder` applies layers top-to-bottom (outermost first):
///   * `HandleErrorLayer` turns the `BoxError` produced deeper in the stack
///     into an HTTP response. This is what lets non-`Infallible` tower layers
///     (like `TimeoutLayer`) sit inside an axum router.
///   * `TimeoutLayer` aborts any request exceeding the budget.
///   * `GlobalConcurrencyLimitLayer` caps in-flight requests across every
///     clone of the service, applying backpressure before handlers run.
pub fn router(state: AppState) -> Router {
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
