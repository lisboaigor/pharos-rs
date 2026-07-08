use std::sync::Arc;

use application::{PlaceOrder, PlaceOrderHandler};
use axum::{Json, Router, routing::post};
use pharos::axum::{CommandHandlerState, HandlerError, run_command};
use pharos::infra::InMemoryRepository;

#[derive(Clone)]
struct AppState {
    place_order: Arc<PlaceOrderHandler<InMemoryRepository<domain::Order>>>,
}

impl axum::extract::FromRef<AppState>
    for Arc<PlaceOrderHandler<InMemoryRepository<domain::Order>>>
{
    fn from_ref(state: &AppState) -> Self {
        Arc::clone(&state.place_order)
    }
}

async fn place_order(
    handler: CommandHandlerState<PlaceOrder, PlaceOrderHandler<InMemoryRepository<domain::Order>>>,
    payload: Json<PlaceOrder>,
) -> Result<Json<domain::Order>, HandlerError> {
    run_command(handler, payload).await
}

#[tokio::main]
async fn main() {
    let repo = InMemoryRepository::new();
    let app = Router::new()
        .route("/orders", post(place_order))
        .with_state(AppState {
            place_order: Arc::new(PlaceOrderHandler::new(repo)),
        });

    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000")
        .await
        .expect("bind listener");
    axum::serve(listener, app).await.expect("serve axum app");
}
