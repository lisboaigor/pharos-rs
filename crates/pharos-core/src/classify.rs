//! Cross-layer error classification.
//!
//! Every layer of a Pharos application — domain, application, transport, and
//! the app's own infrastructure adapters — has its own error type, and each
//! one necessarily knows things the layer above it must not: a repository
//! error carries a `sqlx::Error`'s constraint name, an outbox error carries a
//! broker's connection string. If that detail reaches an HTTP response body
//! (or a gRPC status, or a saga's compensating event), the transport layer
//! has leaked infrastructure it was never supposed to know exists.
//!
//! [`ClassifiedError`] is the contract that stops this at the boundary
//! between any two layers, structurally rather than by convention: a layer's
//! error type implements it once, and everything above only ever calls
//! [`ClassifiedError::kind`] and [`ClassifiedError::public_message`] — never
//! matches on the concrete enum, never calls `.to_string()` on the raw
//! error, never touches `source()` for anything except structured logging.
//! [`ErrorKind`] is the small, closed vocabulary that crosses those
//! boundaries in place of a concrete type: a transport crate maps it to a
//! status code without ever depending on `DomainError`, `StoreError`, or an
//! application's own error enum.
use std::error::Error;

/// Layer-agnostic classification of a failure.
///
/// This is deliberately small and closed to the shapes every layer needs to
/// react to a failure *generically* — decide whether to retry, what HTTP
/// status or gRPC code to answer with, whether to log at `warn` or `error`.
/// It carries no detail of its own; detail lives in
/// [`ClassifiedError::public_message`] (safe to show) or in the error's
/// `Display`/`source()` chain (safe to log, never to show).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ErrorKind {
    /// The requested resource does not exist.
    NotFound,
    /// The caller's input failed validation.
    Validation,
    /// The request conflicts with the resource's current state (optimistic
    /// concurrency, a business-rule conflict, a duplicate).
    Conflict,
    /// The caller is not authenticated.
    Unauthorized,
    /// The caller is authenticated but lacks permission.
    Forbidden,
    /// The caller exceeded a rate limit.
    RateLimited,
    /// A dependency is temporarily unavailable; retrying later may succeed.
    Unavailable,
    /// An unexpected failure with no safe detail to share. This is the
    /// classification every "catch-all" / adapter-error variant must map
    /// to — see [`ClassifiedError::public_message`]'s contract for what
    /// that implies about the message.
    Internal,
}

/// An error that knows how to describe itself to the layer above without
/// exposing how it happened underneath.
///
/// Implement this on every error type that crosses a layer boundary —
/// domain → application, application → transport, an app's own
/// infrastructure adapters → its application layer. The two methods are
/// this crate's entire contract for that crossing:
///
/// - [`kind`](Self::kind) is what the layer above uses to decide *how* to
///   react (retry? 404? 409?) without matching on your concrete variants.
///   Map every variant that isn't one of the specific [`ErrorKind`]s to
///   [`ErrorKind::Internal`] — never invent a new crossing-point by handing
///   the caller your enum.
/// - [`public_message`](Self::public_message) is the only text the layer
///   above is allowed to show anyone outside the process. For a variant
///   classified as [`ErrorKind::Internal`] (or any other kind wrapping a
///   cause you don't control the wording of — a driver error, a broker
///   timeout), this **must** be a fixed, generic string — never
///   `self.to_string()`, never a wrapped cause's `to_string()`. The
///   identifying detail belongs in a `tracing::error!` call's structured
///   fields (via this error's `Display`/`source()` chain), not in the
///   return value. Variants whose message is authored by *this* layer in
///   its own vocabulary (a domain's business-rule message, a field
///   violation naming the caller's own input) are safe to return verbatim
///   — that text never described another layer's internals to begin with.
///
/// A blanket impl propagates this through [`crate::AggregateRoot`]-adjacent
/// wrapper types generic over a handler's own error (see
/// `pharos_app::DispatchError<E>`): implement [`ClassifiedError`] once on
/// your application's top-level error type and every seam that wraps it
/// classifies itself for free.
pub trait ClassifiedError: Error + Send + Sync + 'static {
    /// How the layer above should react to this failure.
    fn kind(&self) -> ErrorKind;

    /// The text safe to return to whatever is on the other side of the next
    /// layer boundary — an HTTP client, a gRPC caller, a saga's failure
    /// event. See the trait docs for the rule this must follow for
    /// [`ErrorKind::Internal`] (and any other kind wrapping an
    /// uncontrolled cause).
    fn public_message(&self) -> String;
}
