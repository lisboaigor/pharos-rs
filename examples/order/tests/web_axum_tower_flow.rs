//! End-to-end HTTP flow for the order example over axum, plus the tower seam.
//!
//! The router is driven in-process with `tower::ServiceExt::oneshot` — no socket
//! is bound — so the test is fast and deterministic while still exercising the
//! real extractors, the tower middleware stack, and the `dispatch` seam.

use std::sync::Arc;
use std::time::Duration;

use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use order::application::commands::CreateOrder;
use order::application::handlers::OrderHandlers;
use order::domain::events::OrderEvent;
use order::domain::order::Order;
use order::application::event_handlers::{NotifyCustomer, UpdateInventory};
use order::web::{in_memory_state, router};
use pharos_app::{CommandHandlerService, EventBus};
use pharos_infra::InMemoryRepository;
use tower::limit::ConcurrencyLimitLayer;
use tower::timeout::TimeoutLayer;
use tower::{ServiceBuilder, ServiceExt};
use uuid::Uuid;

/// Builds a router backed by a fresh in-memory stack with event handlers wired.
fn test_app() -> axum::Router {
    let bus = EventBus::new();
    bus.register::<OrderEvent, _>(NotifyCustomer);
    bus.register::<OrderEvent, _>(UpdateInventory);
    router(in_memory_state(bus))
}

async fn body_string(response: axum::response::Response) -> String {
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    String::from_utf8(bytes.to_vec()).unwrap()
}

fn post_json(uri: &str, json: String) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(json))
        .unwrap()
}

#[tokio::test]
async fn full_order_lifecycle_over_http() {
    let app = test_app();

    // Create.
    let customer_id = Uuid::now_v7();
    let response = app
        .clone()
        .oneshot(post_json(
            "/orders",
            format!(r#"{{"customer_id":"{customer_id}"}}"#),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let order_id: Uuid = serde_json::from_str(&body_string(response).await).unwrap();

    // Add two items.
    for (description, quantity, price) in [("Keyboard", 2, 350.0), ("Mousepad", 1, 80.0)] {
        let response = app
            .clone()
            .oneshot(post_json(
                "/orders/items",
                format!(
                    r#"{{"order_id":"{order_id}","description":"{description}","quantity":{quantity},"unit_price_reais":{price}}}"#
                ),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }

    // Confirm.
    let response = app
        .clone()
        .oneshot(post_json(
            "/orders/confirm",
            format!(r#"{{"order_id":"{order_id}"}}"#),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    // Read the total: 2 * 350.00 + 1 * 80.00 = 780.00 = 78_000 cents.
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/orders/total?order_id={order_id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let total: Option<u64> = serde_json::from_str(&body_string(response).await).unwrap();
    assert_eq!(total, Some(78_000));
}

#[tokio::test]
async fn confirming_unknown_order_maps_to_404() {
    let app = test_app();
    let response = app
        .oneshot(post_json(
            "/orders/confirm",
            format!(r#"{{"order_id":"{}"}}"#, Uuid::now_v7()),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn invalid_quantity_maps_to_400() {
    let app = test_app();

    // First create a valid order to add the item to.
    let customer_id = Uuid::now_v7();
    let response = app
        .clone()
        .oneshot(post_json(
            "/orders",
            format!(r#"{{"customer_id":"{customer_id}"}}"#),
        ))
        .await
        .unwrap();
    let order_id: Uuid = serde_json::from_str(&body_string(response).await).unwrap();

    // Quantity 0 violates a domain rule → 400 Bad Request, not a 500.
    let response = app
        .oneshot(post_json(
            "/orders/items",
            format!(
                r#"{{"order_id":"{order_id}","description":"Bad","quantity":0,"unit_price_reais":10.0}}"#
            ),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

/// Demonstrates the `tower` feature of `pharos-app`: a command handler exposed as
/// a `tower::Service` with timeout + concurrency-limit middleware composed around
/// it — no HTTP involved, the same handler the web routes use.
#[tokio::test]
async fn command_handler_as_a_tower_service() {
    let repo = Arc::new(InMemoryRepository::<Order>::new());
    let handlers = OrderHandlers::new(repo, EventBus::new());

    let service = ServiceBuilder::new()
        .layer(TimeoutLayer::new(Duration::from_secs(5)))
        .layer(ConcurrencyLimitLayer::new(8))
        .service(CommandHandlerService::<CreateOrder, _>::new(handlers));

    let id = service
        .oneshot(CreateOrder {
            customer_id: Uuid::now_v7(),
        })
        .await
        .expect("command service should succeed within the timeout");

    assert_ne!(id, Uuid::nil());
}
