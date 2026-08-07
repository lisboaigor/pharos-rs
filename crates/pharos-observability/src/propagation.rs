//! Carrying trace context across an asynchronous boundary.
//!
//! An outbox exists precisely so effects survive the request that caused them:
//! the message is persisted, the request returns, and a relay dispatches it
//! later, on its own task. That is also what breaks the trace — the relay has
//! long lost the request's span, so every effect it triggers opens a trace of
//! its own, and the operation that caused them cannot be reconstructed.
//!
//! Message headers are the channel: they already travel with the message and
//! already survive the commit. [`inject`] stamps the current context at
//! *enqueue* time, while the request's span is still current — never in the
//! relay, which no longer has it — and [`extract`] restores it on the way out.
//!
//! ```no_run
//! use std::collections::BTreeMap;
//!
//! // Enqueueing, inside the request:
//! let mut headers = BTreeMap::new();
//! pharos_observability::propagation::inject(&mut headers);
//!
//! // Dispatching, on the relay's task:
//! let parent = pharos_observability::propagation::extract(&headers);
//! ```
//!
//! Then make the dispatch span a child of `parent` (`set_parent` from
//! `tracing_opentelemetry::OpenTelemetrySpanExt`) and every handler beneath it
//! joins the originating trace.

use std::collections::BTreeMap;

use opentelemetry::{
    Context, global,
    propagation::{Extractor, Injector},
};
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// Message headers as an OpenTelemetry carrier.
///
/// `opentelemetry_http::HeaderExtractor` only speaks `http::HeaderMap`; a
/// message carries an ordered string map instead, so it needs its own pair.
struct Headers<'a>(&'a mut BTreeMap<String, String>);

impl Injector for Headers<'_> {
    fn set(&mut self, key: &str, value: String) {
        self.0.insert(key.to_owned(), value);
    }
}

struct ReadHeaders<'a>(&'a BTreeMap<String, String>);

impl Extractor for ReadHeaders<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(String::as_str)
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(String::as_str).collect()
    }
}

/// Writes the current trace context into `headers`.
///
/// Call it while the originating span is current — at enqueue time, not at
/// dispatch time.
pub fn inject(headers: &mut BTreeMap<String, String>) {
    let context = tracing::Span::current().context();
    global::get_text_map_propagator(|propagator| {
        propagator.inject_context(&context, &mut Headers(headers));
    });
}

/// Reads back the context written by [`inject`].
///
/// A message without trace headers yields an empty context, and the consumer
/// simply starts a new trace — which is what every message enqueued before this
/// was wired does.
pub fn extract(headers: &BTreeMap<String, String>) -> Context {
    global::get_text_map_propagator(|propagator| propagator.extract(&ReadHeaders(headers)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::trace::TraceContextExt;

    #[test]
    fn a_map_without_trace_headers_extracts_an_empty_context() {
        let headers = BTreeMap::from([("tenant_id".to_owned(), "acme".to_owned())]);
        assert!(!extract(&headers).span().span_context().is_valid());
    }

    /// The round trip is what keeps an effect attached to its cause.
    #[test]
    fn a_written_context_survives_the_round_trip() {
        opentelemetry::global::set_text_map_propagator(
            opentelemetry_sdk::propagation::TraceContextPropagator::new(),
        );

        let mut headers = BTreeMap::new();
        headers.insert(
            "traceparent".to_owned(),
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_owned(),
        );

        let context = extract(&headers);
        assert_eq!(
            context.span().span_context().trace_id().to_string(),
            "4bf92f3577b34da6a3ce929d0e0e4736"
        );
    }
}
