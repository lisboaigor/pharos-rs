//! Request-scoped correlation for HTTP entry points, on W3C Trace Context.
//!
//! [`dispatch`] already wraps each command and query in its own span, and the
//! in-process [`EventBus`] runs handlers inline within the caller's span. So a
//! field recorded on the *request* span is inherited by everything the request
//! sets off — every command, every query, and every cross-context event handler
//! reacting to what they published. Correlating a whole operation therefore
//! costs one field at the edge rather than an id threaded through every
//! signature.
//!
//! [`dispatch`]: pharos_app::dispatch
//! [`EventBus`]: pharos_app::EventBus
//!
//! # Why `traceparent` and not `x-request-id`
//!
//! RFC 6648 deprecates the `X-` prefix for new header names, and an opaque
//! request id has no defined shape to carry across a service boundary. W3C
//! Trace Context is the interoperable form, so a trace started here continues
//! into whatever the application calls next, and any OpenTelemetry-aware
//! collector understands it without translation.
//!
//! This module deliberately does **not** depend on an OpenTelemetry SDK: it
//! parses and mints the header and puts the ids on the span. An application
//! that also exports spans to a tracing backend layers the SDK on top; one that
//! only wants correlated logs pays nothing for it.
//!
//! # Wiring
//!
//! The layer order is the part that silently misbehaves when wrong: the header
//! must be on the request *before* the span is built, so the layer that sets it
//! has to sit outside `TraceLayer`. Inverted, [`request_span`] reads a header
//! that isn't there yet and mints a fresh trace for a request that already had
//! one — losing the link to the caller, with no error.
//!
//! ```ignore
//! Router::new()
//!     // ...routes and inner layers...
//!     .layer(PropagateRequestIdLayer::new(HeaderName::from_static(TRACEPARENT_HEADER)))
//!     .layer(TraceLayer::new_for_http().make_span_with(request_span))
//!     .layer(SetRequestIdLayer::new(
//!         HeaderName::from_static(TRACEPARENT_HEADER),
//!         MakeTraceparent,
//!     ))
//! ```
//!
//! Identity is left to the application: this crate has no notion of who a user
//! is, so it declares the fields and exposes [`record_tenant`] / [`record_user`]
//! for an auth middleware — which runs inside the span — to fill in.

use std::fmt::Display;

use axum::http::{HeaderValue, Request};
use tracing::{Span, field, info_span};

/// The W3C Trace Context header carrying the trace and parent span ids.
pub const TRACEPARENT_HEADER: &str = "traceparent";

/// The only `traceparent` version this module emits or accepts.
///
/// The spec requires unknown *higher* versions to be parsed leniently rather
/// than rejected, which [`Traceparent::parse`] does by reading the first four
/// fields and ignoring any trailing ones.
const VERSION: &str = "00";

/// `trace-flags` marking the trace as sampled.
const FLAG_SAMPLED: &str = "01";

const TRACE_ID_LEN: usize = 32;
const SPAN_ID_LEN: usize = 16;

/// A parsed `traceparent`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Traceparent {
    trace_id: String,
    span_id: String,
    sampled: bool,
}

impl Traceparent {
    /// Parses a header value, returning `None` when it does not conform.
    ///
    /// An all-zero trace or span id is invalid per the spec and is rejected —
    /// accepting it would group unrelated requests under one useless trace.
    pub fn parse(value: &str) -> Option<Self> {
        let mut parts = value.split('-');
        let version = parts.next()?;
        let trace_id = parts.next()?;
        let span_id = parts.next()?;
        let flags = parts.next()?;

        // Version `ff` is forbidden; anything else is accepted, so a future
        // version with extra fields still correlates instead of being dropped.
        if version.len() != 2 || version == "ff" || !is_lower_hex(version) {
            return None;
        }
        if trace_id.len() != TRACE_ID_LEN
            || !is_lower_hex(trace_id)
            || trace_id.bytes().all(|b| b == b'0')
        {
            return None;
        }
        if span_id.len() != SPAN_ID_LEN
            || !is_lower_hex(span_id)
            || span_id.bytes().all(|b| b == b'0')
        {
            return None;
        }
        if flags.len() != 2 || !is_lower_hex(flags) {
            return None;
        }

        Some(Self {
            trace_id: trace_id.to_owned(),
            span_id: span_id.to_owned(),
            // Only the least significant bit is defined (sampled).
            sampled: u8::from_str_radix(flags, 16).ok()? & 1 == 1,
        })
    }

    /// The 32-hex-character trace id shared by every span of one operation.
    pub fn trace_id(&self) -> &str {
        &self.trace_id
    }

    /// The 16-hex-character id of the span that produced this header.
    pub fn span_id(&self) -> &str {
        &self.span_id
    }

    /// Whether the caller marked the trace as sampled.
    pub fn sampled(&self) -> bool {
        self.sampled
    }

    /// Renders the header value.
    pub fn to_header_value(&self) -> String {
        let flags = if self.sampled { FLAG_SAMPLED } else { "00" };
        format!("{VERSION}-{}-{}-{flags}", self.trace_id, self.span_id)
    }
}

fn is_lower_hex(s: &str) -> bool {
    s.bytes()
        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// Renders a request URI for logging, with the query string replaced by a
/// marker rather than included verbatim. See [`request_span`] for why.
fn loggable_uri(uri: &axum::http::Uri) -> String {
    match uri.query() {
        Some(_) => format!("{}?<redacted>", uri.path()),
        None => uri.path().to_owned(),
    }
}

/// Builds the span every request runs under.
///
/// Shaped for `tower_http`'s `make_span_with`. `tenant` and `user` start empty
/// and are filled by [`record_tenant`] / [`record_user`]; declaring them here is
/// what makes them recordable later, since a span's field set is fixed at
/// creation.
///
/// The span is created at `INFO` on purpose: `tower_http`'s default builds it at
/// `DEBUG`, which a typical `RUST_LOG=info` discards — taking the request
/// context off every event inside it.
///
/// `uri` records the path only — the query string is dropped, replaced with
/// a marker that one was present. A query string is not a private channel
/// (it lands in access logs, proxy logs, and `Referer` headers on any link
/// out of the page), but writing it into an INFO-level span turns *this
/// crate's own logs* into one more place a credential leaks — including the
/// realtime handshake token `pharos_realtime::ConnectionAuthenticator`
/// deliberately warns against putting there in the first place. There is no
/// reliable way to redact selectively (a token can arrive under any
/// parameter name), so the whole query string is dropped rather than
/// guessed at.
pub fn request_span<B>(request: &Request<B>) -> Span {
    let traceparent = request
        .headers()
        .get(TRACEPARENT_HEADER)
        .and_then(|value| value.to_str().ok())
        .and_then(Traceparent::parse);

    let span = info_span!(
        "request",
        method = %request.method(),
        uri = %loggable_uri(request.uri()),
        trace_id = field::Empty,
        parent_span_id = field::Empty,
        tenant = field::Empty,
        user = field::Empty,
    );

    // A malformed inbound header is treated as absent rather than fatal: a
    // broken proxy should cost the link to the caller, not the request.
    //
    // Left empty when there is no inbound trace, because who mints the id then
    // depends on the application: one exporting to a tracing backend has its
    // SDK generate it and calls [`record_trace_id`] with the authoritative
    // value, which supersedes anything guessed here.
    if let Some(tp) = &traceparent {
        span.record("trace_id", tp.trace_id());
        span.record("parent_span_id", tp.span_id());
    }
    span
}

/// Records the authoritative trace id on the current request span.
///
/// For applications whose tracing SDK owns id generation: call it once the span
/// has its trace context, so the logs carry the same id the tracing backend
/// stored and the two can be joined.
pub fn record_trace_id(trace_id: impl Display) {
    Span::current().record("trace_id", field::display(trace_id));
}

/// Records the tenant on the current request span.
///
/// Call from whatever resolves the tenant — subdomain, header, or token.
pub fn record_tenant(tenant: impl Display) {
    Span::current().record("tenant", field::display(tenant));
}

/// Records the authenticated principal on the current request span.
///
/// Call from the auth middleware, which runs inside the request span. Pass a
/// stable identifier (username, subject); a credential would land in the logs.
pub fn record_user(user: impl Display) {
    Span::current().record("user", field::display(user));
}

/// Builds a `traceparent` header value from raw ids.
///
/// Exposed so an application that owns id generation (an OpenTelemetry SDK,
/// typically) can mint the header without reimplementing the format.
pub fn traceparent_value(trace_id: &str, span_id: &str, sampled: bool) -> Option<HeaderValue> {
    let tp = Traceparent {
        trace_id: trace_id.to_owned(),
        span_id: span_id.to_owned(),
        sampled,
    };
    // Round-trips through the parser so a malformed id cannot be emitted.
    Traceparent::parse(&tp.to_header_value())?;
    HeaderValue::from_str(&tp.to_header_value()).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    type Caso = Result<(), Box<dyn std::error::Error>>;

    #[test]
    fn parses_a_conformant_header() -> Caso {
        let tp = Traceparent::parse(VALID).ok_or("header is conformant")?;
        assert_eq!(tp.trace_id(), "4bf92f3577b34da6a3ce929d0e0e4736");
        assert_eq!(tp.span_id(), "00f067aa0ba902b7");
        assert!(tp.sampled());
        assert_eq!(tp.to_header_value(), VALID);
        Ok(())
    }

    #[test]
    fn rejects_malformed_headers() {
        for bad in [
            "",
            "not-a-traceparent",
            // all-zero ids are explicitly invalid per the spec
            "00-00000000000000000000000000000000-00f067aa0ba902b7-01",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",
            // wrong lengths
            "00-4bf92f35-00f067aa0ba902b7-01",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa-01",
            // uppercase hex is not allowed
            "00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01",
            // version ff is forbidden
            "ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        ] {
            assert!(
                Traceparent::parse(bad).is_none(),
                "should have rejected {bad:?}"
            );
        }
    }

    /// A later spec version may append fields; dropping those traces would
    /// break correlation across a mixed fleet.
    #[test]
    fn accepts_a_future_version_with_extra_fields() -> Caso {
        let tp =
            Traceparent::parse("01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-something")
                .ok_or("a higher version must parse leniently")?;
        assert_eq!(tp.trace_id(), "4bf92f3577b34da6a3ce929d0e0e4736");
        Ok(())
    }

    #[test]
    fn unsampled_flag_round_trips() -> Caso {
        let header = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00";
        let tp = Traceparent::parse(header).ok_or("header is conformant")?;
        assert!(!tp.sampled());
        assert_eq!(tp.to_header_value(), header);
        Ok(())
    }

    #[test]
    fn traceparent_value_refuses_malformed_ids() {
        assert!(traceparent_value("short", "00f067aa0ba902b7", true).is_none());
        assert!(
            traceparent_value("4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7", true)
                .is_some()
        );
    }

    /// Emits inside the span and reads the captured line: a span's fields
    /// cannot be inspected directly, and without a subscriber it is inert.
    fn span_line(request: &Request<()>, inside: impl FnOnce()) -> String {
        let subscriber = pharos_testing::TestSubscriber::new();
        let _guard = subscriber.install();
        request_span(request).in_scope(|| {
            inside();
            tracing::info!("handled");
        });
        subscriber
            .lines()
            .into_iter()
            .find(|line| line.contains("request"))
            .unwrap_or_default()
    }

    #[test]
    fn inbound_trace_id_carries_onto_the_span() -> Caso {
        let request = Request::builder()
            .uri("/orders")
            .header(TRACEPARENT_HEADER, VALID)
            .body(())?;

        let line = span_line(&request, || {});
        assert!(
            line.contains("4bf92f3577b34da6a3ce929d0e0e4736"),
            "the caller's trace id should continue here, got:\n{line}"
        );
        Ok(())
    }

    #[test]
    fn tenant_and_user_are_recorded_after_the_fact() -> Caso {
        let request = Request::builder().uri("/orders").body(())?;

        // Mirrors an auth middleware running inside the span.
        let line = span_line(&request, || {
            record_tenant("acme");
            record_user("carlos");
        });
        assert!(
            line.contains("acme") && line.contains("carlos"),
            "fields declared Empty should be fillable later, got:\n{line}"
        );
        Ok(())
    }

    #[test]
    fn a_query_string_never_reaches_the_span() -> Caso {
        let request = Request::builder()
            .uri("/ws/game:1?token=super-secret-session-token")
            .body(())?;

        let line = span_line(&request, || {});
        assert!(
            !line.contains("super-secret-session-token"),
            "a token in the query string must never reach the logs, got:\n{line}"
        );
        assert!(
            line.contains("/ws/game:1"),
            "the path must still be logged, got:\n{line}"
        );
        Ok(())
    }

    #[test]
    fn a_uri_with_no_query_string_is_unaffected() -> Caso {
        let uri: axum::http::Uri = "/orders".parse()?;
        assert_eq!(loggable_uri(&uri), "/orders");
        Ok(())
    }

    #[test]
    fn a_malformed_inbound_header_does_not_break_the_request() -> Caso {
        let request = Request::builder()
            .uri("/orders")
            .header(TRACEPARENT_HEADER, "garbage")
            .body(())?;

        let line = span_line(&request, || {});
        assert!(
            line.contains("/orders"),
            "the span must still be usable, got:\n{line}"
        );
        Ok(())
    }
}
