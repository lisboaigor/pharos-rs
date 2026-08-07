//! HTTP wiring: the request span, the metrics middleware, and the one layer
//! order in which both actually work.
//!
//! Two of this framework's sharpest edges are here, and both fail without a
//! word:
//!
//! - the metrics middleware must sit **inside** the trace layer, or the request
//!   span has already closed when an observation is recorded and the exemplar
//!   comes out empty — metrics and traces stop being joinable;
//! - the layer stamping `traceparent` must sit **outside** it, or the header
//!   does not exist yet when the span is built and every request mints a fresh
//!   trace, losing the link to whoever called.
//!
//! [`instrument`] applies that order so an application never chooses it.

use std::sync::Arc;

use axum::{Extension, Router, extract::Request, middleware::from_fn};
use opentelemetry::trace::{TraceContextExt, TraceId};
use pharos_axum::{TRACEPARENT_HEADER, metrics::HttpMetrics};
use tower_http::trace::{DefaultOnRequest, DefaultOnResponse, TraceLayer};
use tracing::{Level, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// Builds the request span and attaches it to the caller's trace.
///
/// The shape comes from [`pharos_axum::request_span`]; what this adds is the
/// SDK: the inbound `traceparent` becomes the span's parent, and the trace id
/// the SDK settled on is recorded onto the span so a log line and the exported
/// trace can be joined on the same value.
pub fn request_span<B>(request: &Request<B>) -> Span {
    let span = pharos_axum::request_span(request);

    let parent = opentelemetry::global::get_text_map_propagator(|propagator| {
        propagator.extract(&opentelemetry_http::HeaderExtractor(request.headers()))
    });
    // Fails only when no OpenTelemetry layer is installed, which is the ordinary
    // case for local development without a collector. Warning per request would
    // be pure noise, and the absence of traces speaks for itself.
    let _ = span.set_parent(parent);

    // The SDK owns the id: with no inbound `traceparent` it has just minted one,
    // and that is the value the tracing backend will store. Recording anything
    // else here would make the jump from a log line to its trace find nothing.
    let trace_id = span.context().span().span_context().trace_id();
    if trace_id != TraceId::INVALID {
        span.in_scope(|| pharos_axum::record_trace_id(trace_id));
    }
    span
}

/// Metrics whose exemplars carry the current trace id.
///
/// `pharos-axum` knows nothing about OpenTelemetry, so it asks for the id
/// through a closure; this supplies one backed by the SDK.
pub fn http_metrics() -> Arc<HttpMetrics> {
    Arc::new(HttpMetrics::new().with_exemplars(|| {
        let id = Span::current().context().span().span_context().trace_id();
        (id != TraceId::INVALID).then(|| id.to_string())
    }))
}

/// Wraps `router` in the observability layers, in the only order that works.
///
/// Reading outward from the routes:
///
/// 1. `track` — records the observation. Must be applied through `Router::layer`
///    rather than an outer `ServiceBuilder`, because `MatchedPath` only exists
///    in the extensions after routing, and that is where the bounded `endpoint`
///    label comes from.
/// 2. `Extension(metrics)` — what `track` reads.
/// 3. `TraceLayer` — opens the request span. Everything above runs inside it.
/// 4. the `traceparent` layer — must be outermost of these, so the header is
///    present when step 3 reads it.
pub fn instrument(router: Router, metrics: Arc<HttpMetrics>) -> Router {
    router
        .layer(from_fn(pharos_axum::metrics::track))
        .layer(Extension(metrics))
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(request_span::<axum::body::Body>)
                .on_request(DefaultOnRequest::new().level(Level::INFO))
                .on_response(DefaultOnResponse::new().level(Level::INFO)),
        )
}

/// Renders the metrics for a scrape, with the content type that makes a scraper
/// parse exemplars instead of discarding them.
pub fn render_metrics(metrics: &HttpMetrics) -> Result<(&'static str, String), std::fmt::Error> {
    Ok((pharos_axum::metrics::CONTENT_TYPE, metrics.encode()?))
}

/// Header an inbound trace arrives on, re-exported for wiring the layer that
/// mints one when absent.
pub const INBOUND_TRACE_HEADER: &str = TRACEPARENT_HEADER;
