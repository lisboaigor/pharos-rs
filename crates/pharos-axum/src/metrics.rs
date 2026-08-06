//! RED metrics for HTTP handlers, with exemplars linking a slow observation to
//! the trace that produced it.
//!
//! The point of the exemplar is the jump: a latency percentile tells you
//! *that* something was slow, and the exemplar carries the trace id of one
//! actual request in that bucket, so a click goes from the spike to the span
//! tree that caused it. Without it, metrics and traces stay two separate
//! investigations.
//!
//! # Cardinality
//!
//! The `endpoint` label is the *matched route* (`/orders/{id}`), never the raw
//! URI. Labelling by URI would mint a new time series per id and take the
//! metrics store down with it — the failure mode is gradual and expensive, so
//! it is handled here rather than left to each application.
//!
//! # Exemplars require OpenMetrics
//!
//! [`encode`] emits the OpenMetrics text format, which is the only exposition
//! that carries exemplars, and [`CONTENT_TYPE`] is what tells Prometheus to
//! parse it as such. The scraper must also run with
//! `--enable-feature=exemplar-storage`, or it parses them and throws them away.
//!
//! # Trace ids
//!
//! This crate does not depend on an OpenTelemetry SDK (see
//! [`observability`](crate::observability)), so it cannot know the current
//! trace id. The application supplies one through
//! [`HttpMetrics::with_exemplars`]; without it the metrics still work, just
//! without the jump.

use std::sync::Arc;

use axum::{
    Extension,
    extract::{MatchedPath, Request},
    middleware::Next,
    response::Response,
};
use prometheus_client::{
    encoding::{EncodeLabelSet, text::encode},
    metrics::{
        counter::Counter, exemplar::HistogramWithExemplars, family::Family, gauge::Gauge,
        histogram::exponential_buckets,
    },
    registry::Registry,
};

/// Content type that makes Prometheus parse exemplars instead of ignoring them.
pub const CONTENT_TYPE: &str = "application/openmetrics-text; version=1.0.0; charset=utf-8";

/// Requests slower than this are the ones worth having an exemplar for; the
/// bucket layout starts here and doubles.
const FIRST_BUCKET_SECONDS: f64 = 0.005;
const BUCKET_FACTOR: f64 = 2.0;
const BUCKET_COUNT: u16 = 12;

/// Labels shared by the request counter and the duration histogram.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct HttpLabels {
    /// HTTP method.
    pub method: String,
    /// The *matched route*, e.g. `/orders/{id}` — never the raw URI.
    pub endpoint: String,
    /// Response status code.
    pub status: u16,
}

/// The single label an exemplar carries: which trace to open.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct TraceExemplar {
    /// 32 hex characters, matching what the tracing backend stored.
    pub trace_id: String,
}

/// Supplies the current trace id when an observation is recorded.
type TraceIdSource = Arc<dyn Fn() -> Option<String> + Send + Sync>;

/// RED metrics plus the registry that exposes them.
pub struct HttpMetrics {
    requests: Family<HttpLabels, Counter>,
    duration: Family<HttpLabels, HistogramWithExemplars<TraceExemplar>>,
    in_flight: Gauge,
    registry: Registry,
    trace: Option<TraceIdSource>,
}

impl HttpMetrics {
    /// Registers the metrics under `http_server_*`.
    pub fn new() -> Self {
        let requests = Family::<HttpLabels, Counter>::default();
        let duration =
            Family::<HttpLabels, HistogramWithExemplars<TraceExemplar>>::new_with_constructor(
                || {
                    HistogramWithExemplars::new(exponential_buckets(
                        FIRST_BUCKET_SECONDS,
                        BUCKET_FACTOR,
                        BUCKET_COUNT,
                    ))
                },
            );
        let in_flight = Gauge::default();

        let mut registry = Registry::default();
        registry.register(
            "http_server_requests",
            "Requests handled, by method, route and status",
            requests.clone(),
        );
        registry.register(
            "http_server_request_duration_seconds",
            "Request duration in seconds",
            duration.clone(),
        );
        registry.register(
            "http_server_requests_in_flight",
            "Requests currently being handled",
            in_flight.clone(),
        );

        Self {
            requests,
            duration,
            in_flight,
            registry,
            trace: None,
        }
    }

    /// Attaches the source of trace ids used for exemplars.
    ///
    /// Called with the request's span still current, so an application whose
    /// SDK owns the trace id can read it from there:
    ///
    /// ```ignore
    /// HttpMetrics::new().with_exemplars(|| {
    ///     let id = Span::current().context().span().span_context().trace_id();
    ///     (id != TraceId::INVALID).then(|| id.to_string())
    /// })
    /// ```
    pub fn with_exemplars<F>(mut self, source: F) -> Self
    where
        F: Fn() -> Option<String> + Send + Sync + 'static,
    {
        self.trace = Some(Arc::new(source));
        self
    }

    /// Renders the metrics in OpenMetrics text format.
    ///
    /// Serve it with [`CONTENT_TYPE`].
    pub fn encode(&self) -> Result<String, std::fmt::Error> {
        let mut buffer = String::new();
        encode(&mut buffer, &self.registry)?;
        Ok(buffer)
    }

    /// The registry, for an application that registers metrics of its own.
    pub fn registry_mut(&mut self) -> &mut Registry {
        &mut self.registry
    }

    fn observe(&self, labels: HttpLabels, seconds: f64) {
        self.requests.get_or_create(&labels).inc();
        let exemplar = self
            .trace
            .as_ref()
            .and_then(|source| source())
            .map(|trace_id| TraceExemplar { trace_id });
        self.duration
            .get_or_create(&labels)
            .observe(seconds, exemplar, None);
    }
}

impl Default for HttpMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Middleware recording one observation per request.
///
/// Wire it with the metrics in an extension, so the layer carries no knowledge
/// of the application's state type:
///
/// ```ignore
/// router
///     .layer(axum::middleware::from_fn(pharos_axum::metrics::track))
///     .layer(Extension(metrics.clone()))
/// ```
pub async fn track(
    Extension(metrics): Extension<Arc<HttpMetrics>>,
    request: Request,
    next: Next,
) -> Response {
    // The matched route keeps the label set bounded; a request that matched no
    // route is bucketed under a constant rather than by its URI, which is
    // exactly where a scanner would otherwise mint thousands of series.
    let endpoint = request
        .extensions()
        .get::<MatchedPath>()
        .map(|p| p.as_str().to_owned())
        .unwrap_or_else(|| "<no route>".to_owned());
    let method = request.method().to_string();

    metrics.in_flight.inc();
    let started = std::time::Instant::now();
    let response = next.run(request).await;
    let seconds = started.elapsed().as_secs_f64();
    metrics.in_flight.dec();

    metrics.observe(
        HttpLabels {
            method,
            endpoint,
            status: response.status().as_u16(),
        },
        seconds,
    );

    response
}

#[cfg(test)]
mod tests {
    use super::*;

    fn labels() -> HttpLabels {
        HttpLabels {
            method: "POST".into(),
            endpoint: "/orders/{id}".into(),
            status: 200,
        }
    }

    #[test]
    fn encodes_in_openmetrics_with_an_exemplar() -> Result<(), Box<dyn std::error::Error>> {
        let metrics =
            HttpMetrics::new().with_exemplars(|| Some("4bf92f3577b34da6a3ce929d0e0e4736".into()));
        metrics.observe(labels(), 0.42);

        let output = metrics.encode()?;
        assert!(
            output.contains("http_server_request_duration_seconds"),
            "duration metric missing:\n{output}"
        );
        // The exemplar is the whole reason this crate replaced a simpler
        // exporter: without it the jump from a latency spike to its trace does
        // not exist.
        assert!(
            output.contains("trace_id=\"4bf92f3577b34da6a3ce929d0e0e4736\""),
            "exemplar with trace_id missing:\n{output}"
        );
        Ok(())
    }

    #[test]
    fn works_without_a_trace_source() -> Result<(), Box<dyn std::error::Error>> {
        let metrics = HttpMetrics::new();
        metrics.observe(labels(), 0.1);

        let output = metrics.encode()?;
        assert!(output.contains("http_server_requests_total"));
        assert!(
            !output.contains("trace_id"),
            "no trace source means no exemplar:\n{output}"
        );
        Ok(())
    }

    /// The label set must stay bounded by route, not by URI.
    #[test]
    fn separates_series_by_route_and_status() -> Result<(), Box<dyn std::error::Error>> {
        let metrics = HttpMetrics::new();
        metrics.observe(labels(), 0.1);
        metrics.observe(
            HttpLabels {
                status: 500,
                ..labels()
            },
            0.2,
        );

        let output = metrics.encode()?;
        assert!(output.contains("status=\"200\""));
        assert!(output.contains("status=\"500\""));
        Ok(())
    }
}
