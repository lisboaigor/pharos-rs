//! Input validation flow tests for the `AddItem` command using garde.
//!
//! These tests verify that `CommandHandlerState::dispatch` runs garde validation
//! before the handler and maps failures to `AppError::Validation` (HTTP 422),
//! while valid commands reach the handler and domain errors still map to their
//! own status codes.

use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use order::application::commands::{AddItem, ConfirmOrder, CreateOrder};
use order::web::{in_memory_state, router};
use pharos_app::{Command, EventBus};
use tower::ServiceExt;
use uuid::Uuid;

// A self-contained command with no garde annotations, defined here in the test
// module so the assertion doesn't depend on how order example commands evolve.
#[derive(pharos_macros::Command)]
struct NoValidationCommand {
    #[allow(dead_code)]
    value: u32,
}

fn test_app() -> axum::Router {
    router(in_memory_state(EventBus::new()))
}

async fn body_string(
    response: axum::response::Response,
) -> Result<String, Box<dyn std::error::Error>> {
    let bytes = to_bytes(response.into_body(), usize::MAX).await?;
    Ok(String::from_utf8(bytes.to_vec())?)
}

fn post_json(uri: &str, json: String) -> Request<Body> {
    let Ok(req) = Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(json))
    else {
        panic!("invalid URI or header in test fixture");
    };
    req
}

async fn create_order(app: axum::Router) -> Result<Uuid, Box<dyn std::error::Error>> {
    let response = app
        .oneshot(post_json(
            "/orders",
            format!(r#"{{"customer_id":"{}"}}"#, Uuid::now_v7()),
        ))
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    Ok(serde_json::from_str(&body_string(response).await?)?)
}

/// The macro must NOT generate a `validate_input` override when there are no
/// `#[garde(...)]` rules — the default no-op must be used instead.
///
/// `NoValidationCommand` is defined locally in this test module so the assertion
/// is self-contained and doesn't rely on how the order example's commands evolve.
#[test]
fn command_without_garde_annotations_has_noop_validate_input() {
    let cmd = NoValidationCommand { value: 42 };
    assert!(cmd.validate_input().is_ok());
}

/// Also verify that the two order commands without garde are no-ops, as a
/// regression guard against accidentally adding garde annotations to them.
#[test]
fn order_commands_without_garde_have_noop_validate_input() {
    assert!(
        CreateOrder {
            customer_id: Uuid::now_v7()
        }
        .validate_input()
        .is_ok()
    );
    assert!(
        ConfirmOrder {
            order_id: Uuid::now_v7()
        }
        .validate_input()
        .is_ok()
    );
}

/// A valid `AddItem` passes `validate_input` before reaching the handler.
#[test]
fn valid_add_item_passes_validate_input() {
    let cmd = AddItem {
        order_id: Uuid::now_v7(),
        description: "Keyboard".to_string(),
        quantity: 2,
        unit_price_reais: 350.0,
    };
    assert!(cmd.validate_input().is_ok());
}

/// Each violated field produces an entry in the garde report.
#[test]
fn invalid_add_item_fails_validate_input_with_report() {
    let cmd = AddItem {
        order_id: Uuid::now_v7(),
        description: String::new(), // violates length(min = 1)
        quantity: 0,                // violates range(min = 1)
        unit_price_reais: 0.0,      // violates range(min = 0.01)
    };
    let Err(error) = cmd.validate_input() else {
        panic!(
            "AddItem with blank description, zero quantity, and zero price must fail validation"
        );
    };
    assert!(
        !error.violations().is_empty(),
        "expected validation errors but no violation was recorded"
    );
    // One violation per offending field, with the field path preserved.
    for field in ["description", "quantity", "unit_price_reais"] {
        assert!(
            error.violations().iter().any(|v| v.path.contains(field)),
            "expected a violation for `{field}`"
        );
    }
}

/// Guard: a command with no garde rules passes through without validation errors.
#[tokio::test]
async fn command_without_garde_rules_dispatches_normally() -> Result<(), Box<dyn std::error::Error>>
{
    let app = test_app();
    let response = app
        .oneshot(post_json(
            "/orders",
            format!(r#"{{"customer_id":"{}"}}"#, Uuid::now_v7()),
        ))
        .await?;
    // CreateOrder has no garde annotations — dispatch must not reject it.
    assert_eq!(response.status(), StatusCode::OK);
    Ok(())
}

/// quantity = 0 violates #[garde(range(min = 1))] → 422 before the handler runs.
#[tokio::test]
async fn zero_quantity_is_rejected_before_handler() -> Result<(), Box<dyn std::error::Error>> {
    let app = test_app();
    let order_id = create_order(app.clone()).await?;

    let response = app
        .oneshot(post_json(
            "/orders/items",
            format!(
                r#"{{"order_id":"{order_id}","description":"Widget","quantity":0,"unit_price_reais":10.0}}"#
            ),
        ))
        .await?;

    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    Ok(())
}

/// Empty description violates #[garde(length(min = 1, ...))] → 422.
#[tokio::test]
async fn empty_description_is_rejected_before_handler() -> Result<(), Box<dyn std::error::Error>> {
    let app = test_app();
    let order_id = create_order(app.clone()).await?;

    let response = app
        .oneshot(post_json(
            "/orders/items",
            format!(
                r#"{{"order_id":"{order_id}","description":"","quantity":1,"unit_price_reais":10.0}}"#
            ),
        ))
        .await?;

    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    Ok(())
}

/// price = 0.0 violates #[garde(range(min = 0.01))] → 422.
#[tokio::test]
async fn zero_price_is_rejected_before_handler() -> Result<(), Box<dyn std::error::Error>> {
    let app = test_app();
    let order_id = create_order(app.clone()).await?;

    let response = app
        .oneshot(post_json(
            "/orders/items",
            format!(
                r#"{{"order_id":"{order_id}","description":"Widget","quantity":1,"unit_price_reais":0.0}}"#
            ),
        ))
        .await?;

    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    Ok(())
}

/// All fields valid → handler runs → 204 No Content.
#[tokio::test]
async fn valid_add_item_reaches_handler() -> Result<(), Box<dyn std::error::Error>> {
    let app = test_app();
    let order_id = create_order(app.clone()).await?;

    let response = app
        .oneshot(post_json(
            "/orders/items",
            format!(
                r#"{{"order_id":"{order_id}","description":"Widget","quantity":3,"unit_price_reais":49.99}}"#
            ),
        ))
        .await?;

    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    Ok(())
}

/// Multiple violations in one command → still 422 (all errors collected by garde).
#[tokio::test]
async fn multiple_violations_all_return_422() -> Result<(), Box<dyn std::error::Error>> {
    let app = test_app();
    let order_id = create_order(app.clone()).await?;

    let response = app
        .oneshot(post_json(
            "/orders/items",
            // quantity = 0 AND price = 0.0 AND description = ""
            format!(
                r#"{{"order_id":"{order_id}","description":"","quantity":0,"unit_price_reais":0.0}}"#
            ),
        ))
        .await?;

    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    Ok(())
}
