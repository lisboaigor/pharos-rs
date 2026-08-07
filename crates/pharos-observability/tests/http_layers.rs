//! The layer order `instrument` applies, pinned by its observable consequence.
//!
//! Both edges here fail silently, which is why they are worth a test rather
//! than a comment: move the metrics middleware outside the trace layer and the
//! exemplar simply comes out empty, joining metrics to traces no more.

use std::sync::Arc;

use axum::{Router, body::Body, http::Request, routing::get};
use opentelemetry_sdk::trace::SdkTracerProvider;
use pharos_observability::http::{http_metrics, instrument};
use tower::ServiceExt;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

/// Installs a tracer for the duration of the test, so spans carry a real trace
/// id. No exporter: the ids are what matter here, not where they end up.
fn with_tracer() -> (SdkTracerProvider, tracing::subscriber::DefaultGuard) {
    let provider = SdkTracerProvider::builder().build();
    let layer = tracing_opentelemetry::layer().with_tracer(
        opentelemetry::trace::TracerProvider::tracer(&provider, "test"),
    );
    let guard = tracing_subscriber::registry().with(layer).set_default();
    (provider, guard)
}

async fn call(router: Router, path: &str) -> TestResult {
    let request = Request::builder().uri(path).body(Body::empty())?;
    router.oneshot(request).await?;
    Ok(())
}

/// The point of the whole arrangement: a latency observation carries the trace
/// id of the request that produced it, so a spike in a dashboard is one click
/// from the span tree behind it.
#[tokio::test]
async fn a_recorded_observation_carries_the_requests_trace_id() -> TestResult {
    let (_provider, _guard) = with_tracer();

    let metrics = http_metrics();
    let router = instrument(
        Router::new().route("/orders/{id}", get(|| async { "ok" })),
        Arc::clone(&metrics),
    );

    call(router, "/orders/42").await?;

    let rendered = metrics.encode()?;
    let exemplar = rendered
        .lines()
        .find(|line| line.contains("trace_id="))
        .ok_or_else(|| format!("no exemplar was recorded:\n{rendered}"))?;

    // 32 lowercase hex characters — a real id, not the all-zero placeholder a
    // span outside any trace would yield.
    let id = exemplar
        .split("trace_id=\"")
        .nth(1)
        .and_then(|rest| rest.split('"').next())
        .unwrap_or_default();
    assert_eq!(id.len(), 32, "malformed trace id in exemplar: {exemplar}");
    assert!(
        id.chars().any(|c| c != '0'),
        "exemplar carried an empty trace id, so the metrics middleware ran \
         outside the request span: {exemplar}"
    );
    Ok(())
}

/// The `endpoint` label has to be the matched route. Labelling by URI would mint
/// a series per id and degrade the metrics store gradually rather than loudly.
#[tokio::test]
async fn the_endpoint_label_is_the_matched_route_not_the_uri() -> TestResult {
    let (_provider, _guard) = with_tracer();

    let metrics = http_metrics();
    let router = instrument(
        Router::new().route("/orders/{id}", get(|| async { "ok" })),
        Arc::clone(&metrics),
    );

    call(router.clone(), "/orders/1").await?;
    call(router, "/orders/2").await?;

    let rendered = metrics.encode()?;
    assert!(
        rendered.contains(r#"endpoint="/orders/{id}""#),
        "expected the matched route as label:\n{rendered}"
    );
    assert!(
        !rendered.contains(r#"endpoint="/orders/1""#),
        "the raw URI became a label, so cardinality grows with traffic:\n{rendered}"
    );
    Ok(())
}
