//! Axum integration for Pharos handlers.
//!
//! This crate keeps HTTP concerns outside `pharos-app` while making it easy to
//! expose command and query handlers through Axum routes.
//!
//! See [`observability`] for the request span that correlates everything one
//! HTTP call sets off, [`metrics`] for RED metrics whose exemplars link a
//! latency observation back to that trace, and [`profile`] for a router
//! builder that makes "this binary never writes" a compile-time property.

pub mod metrics;
pub mod observability;
pub mod profile;

pub use observability::{
    TRACEPARENT_HEADER, Traceparent, record_tenant, record_trace_id, record_user, request_span,
    traceparent_value,
};
pub use profile::{ProcessProfile, ReadOnly, ReadWrite};

use std::fmt::{Display, Formatter};
use std::marker::PhantomData;
use std::sync::Arc;

use axum::extract::{FromRef, FromRequestParts, Query as QueryPayload};
use axum::http::{StatusCode, request::Parts};
use axum::response::{IntoResponse, Response};
use axum::{Json, extract::State};
use pharos_app::{Command, CommandHandler, DispatchError, Query, QueryHandler, ValidationError};
use pharos_core::{ClassifiedError, DomainError, ErrorKind};
use serde::Serialize;
use serde::de::DeserializeOwned;

/// Maps [`ErrorKind`] to the conventional HTTP status for it.
///
/// This is the *only* place an HTTP status is chosen from a failure's
/// classification: every [`HandlerError`] constructor that takes a
/// [`ClassifiedError`] goes through this function, so a domain error, an
/// application error, and an application's own error type all get the same
/// status for the same kind, without pharos-axum ever matching on their
/// concrete variants.
pub fn status_for(kind: ErrorKind) -> StatusCode {
    match kind {
        ErrorKind::NotFound => StatusCode::NOT_FOUND,
        ErrorKind::Validation => StatusCode::UNPROCESSABLE_ENTITY,
        ErrorKind::Conflict => StatusCode::CONFLICT,
        ErrorKind::Unauthorized => StatusCode::UNAUTHORIZED,
        ErrorKind::Forbidden => StatusCode::FORBIDDEN,
        ErrorKind::RateLimited => StatusCode::TOO_MANY_REQUESTS,
        ErrorKind::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        // `ErrorKind` is non_exhaustive; an unknown future kind is treated
        // the same as `Internal` — never assumed safe to answer with
        // anything more specific than "something failed on our end".
        ErrorKind::Internal => StatusCode::INTERNAL_SERVER_ERROR,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

/// Error returned when an HTTP request cannot be handled successfully.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandlerError {
    status: StatusCode,
    message: String,
}

impl HandlerError {
    /// Creates an error with an explicit status code.
    pub fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }

    /// Maps an application handler failure to `500 Internal Server Error`.
    ///
    /// The error detail is logged, never returned: handler errors routinely
    /// wrap database or broker failures, and echoing those to the client
    /// would leak internal details (table names, hosts, constraints). The
    /// response body stays generic.
    pub fn internal(error: impl Display) -> Self {
        tracing::error!(error = %error, "handler failed");
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, "internal server error")
    }

    /// Maps an input-validation failure to `422 Unprocessable Entity`.
    ///
    /// Unlike [`internal`](Self::internal), the detail is safe to return: it
    /// describes the client's own input. A thin wrapper over
    /// [`from_classified`](Self::from_classified).
    pub fn validation(error: &ValidationError) -> Self {
        Self::from_classified(error)
    }

    /// Rejects an attempt to invoke an internal-only command over HTTP.
    ///
    /// A command marked `#[command(internal)]` (a saga-issued payout, refund,
    /// or `StartGame`) has no public HTTP identity. If one is wired to a route
    /// by mistake, [`run_command`] returns this instead of executing it: the
    /// response is `404 Not Found` — to any caller the endpoint simply does not
    /// exist — while the wiring bug is logged at error level so it surfaces in
    /// operations. This is the framework backstop behind the primary rule of
    /// never registering the route in the first place.
    pub fn internal_command(name: &str) -> Self {
        tracing::error!(
            command = name,
            "refused to run an internal-only command over HTTP; its route must be removed"
        );
        Self::new(
            StatusCode::NOT_FOUND,
            "command is internal-only and cannot be invoked over HTTP",
        )
    }

    /// Maps any [`ClassifiedError`] to its conventional HTTP status and
    /// public message — [`status_for`] picks the status from
    /// [`ClassifiedError::kind`], and the response body is exactly
    /// [`ClassifiedError::public_message`], never this error's `Display` or
    /// `source()` chain.
    ///
    /// This is the framework's *one* HTTP boundary rule: pharos-axum never
    /// matches on a domain, application, or application-specific error
    /// enum to decide what to answer — it only ever calls the two methods
    /// this trait exposes. Implement [`ClassifiedError`] once on your
    /// application's top-level error type (see
    /// [`DispatchError`]'s blanket impl, which classifies for free once
    /// your handler's own error type does) and every route that returns it
    /// gets a correct, non-leaking response through this one function:
    ///
    /// ```ignore
    /// handler.dispatch(command).await.map_err(|e| HandlerError::from_classified(&e))?;
    /// ```
    ///
    /// A response built this way logs the full error (via
    /// `tracing::error!`, using this error's `Display`/`source()` chain)
    /// whenever the mapped status is a 5xx — the same rule
    /// [`internal`](Self::internal) already followed, now applied
    /// uniformly regardless of which layer's error type is in hand.
    pub fn from_classified(error: &impl ClassifiedError) -> Self {
        let status = status_for(error.kind());
        if status.is_server_error() {
            tracing::error!(error = %error, "handler failed");
        }
        Self::new(status, error.public_message())
    }

    /// Maps a [`DomainError`] to the conventional HTTP status.
    ///
    /// `NotFound` → `404`, `Conflict`/`BusinessRule` → `409`, `Validation` →
    /// `422`. The messages are domain-authored (business language, not
    /// infrastructure detail), so they are safe to return to the client. Use
    /// this at the route boundary so every application maps domain failures
    /// the same way:
    ///
    /// ```ignore
    /// handler.dispatch(command).await.map_err(|e| match e {
    ///     DispatchError::Validation(e) => HandlerError::validation(&e),
    ///     DispatchError::Handler(AppError::Domain(e)) => HandlerError::from_domain(&e),
    ///     DispatchError::Handler(e) => HandlerError::internal(e),
    /// })?;
    /// ```
    ///
    /// A thin wrapper over [`from_classified`](Self::from_classified) kept
    /// for the common single-error-type case; prefer `from_classified`
    /// directly once your application's own error type also implements
    /// [`ClassifiedError`].
    pub fn from_domain(error: &DomainError) -> Self {
        Self::from_classified(error)
    }

    /// Returns the status code.
    pub fn status(&self) -> StatusCode {
        self.status
    }

    /// Returns the response message.
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl Display for HandlerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for HandlerError {}

impl IntoResponse for HandlerError {
    fn into_response(self) -> Response {
        (self.status, self.message).into_response()
    }
}

/// Axum extractor for a concrete `CommandHandler` from router state.
pub struct CommandHandlerState<C, H> {
    handler: Arc<H>,
    _marker: PhantomData<fn(C)>,
}

impl<C, H> Clone for CommandHandlerState<C, H> {
    fn clone(&self) -> Self {
        Self {
            handler: Arc::clone(&self.handler),
            _marker: PhantomData,
        }
    }
}

impl<C, H> CommandHandlerState<C, H> {
    /// Wraps a shared handler reference.
    pub fn from_arc(handler: Arc<H>) -> Self {
        Self {
            handler,
            _marker: PhantomData,
        }
    }

    /// Returns the shared handler.
    pub fn handler(&self) -> &Arc<H> {
        &self.handler
    }
}

impl<C, H> CommandHandlerState<C, H>
where
    C: Command,
    H: CommandHandler<C>,
{
    /// Dispatches the command through the framework's validation and
    /// instrumentation seam.
    ///
    /// [`Command::validate_input`] runs inside [`pharos_app::dispatch`] itself,
    /// so validation behaves identically no matter which port the command
    /// entered through. Match on [`DispatchError`] to map validation failures
    /// and handler failures to different HTTP responses.
    ///
    /// Refuses to run a command marked `#[command(internal)]`
    /// ([`Command::INTERNAL_ONLY`]), returning [`DispatchError::Validation`]
    /// instead — the handler never executes. This is the same guard
    /// [`run_command`] applies (there it maps to `404`; here, since this
    /// method's error type carries no HTTP status of its own, it surfaces as
    /// the `422` a caller's `DispatchError::Validation` arm already handles).
    /// Route handlers built directly on `CommandHandlerState::dispatch`
    /// (rather than through [`run_command`]) previously bypassed
    /// `INTERNAL_ONLY` entirely; this closes that gap at the shared seam
    /// instead of relying on every caller to route through `run_command`.
    pub async fn dispatch(&self, command: C) -> Result<H::Output, DispatchError<H::Error>> {
        if C::INTERNAL_ONLY {
            tracing::error!(
                command = C::NAME,
                "refused to run an internal-only command through CommandHandlerState::dispatch"
            );
            return Err(DispatchError::Validation(ValidationError::violation(
                "",
                "command is internal-only and cannot be invoked over this seam",
            )));
        }
        pharos_app::dispatch(&*self.handler, command).await
    }
}

impl<S, C, H> FromRequestParts<S> for CommandHandlerState<C, H>
where
    S: Send + Sync,
    Arc<H>: FromRef<S>,
    C: Command,
    H: CommandHandler<C>,
{
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(_parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        Ok(Self::from_arc(Arc::<H>::from_ref(state)))
    }
}

/// Axum extractor for a concrete `QueryHandler` from router state.
pub struct QueryHandlerState<Q, H> {
    handler: Arc<H>,
    _marker: PhantomData<fn(Q)>,
}

impl<Q, H> Clone for QueryHandlerState<Q, H> {
    fn clone(&self) -> Self {
        Self {
            handler: Arc::clone(&self.handler),
            _marker: PhantomData,
        }
    }
}

impl<Q, H> QueryHandlerState<Q, H> {
    /// Wraps a shared handler reference.
    pub fn from_arc(handler: Arc<H>) -> Self {
        Self {
            handler,
            _marker: PhantomData,
        }
    }

    /// Returns the shared handler.
    pub fn handler(&self) -> &Arc<H> {
        &self.handler
    }
}

impl<Q, H> QueryHandlerState<Q, H>
where
    Q: Query,
    H: QueryHandler<Q>,
{
    /// Dispatches a query through the framework's instrumentation seam.
    ///
    /// Read-side counterpart to [`CommandHandlerState::dispatch`].
    pub async fn dispatch(&self, query: Q) -> Result<Q::Result, H::Error> {
        pharos_app::query_dispatch(&*self.handler, query).await
    }
}

impl<S, Q, H> FromRequestParts<S> for QueryHandlerState<Q, H>
where
    S: Send + Sync,
    Arc<H>: FromRef<S>,
    Q: Query,
    H: QueryHandler<Q>,
{
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(_parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        Ok(Self::from_arc(Arc::<H>::from_ref(state)))
    }
}

/// Executes a command handler using JSON request/response bodies.
///
/// Dispatching runs [`Command::validate_input`] before the handler — the seam
/// itself enforces it, no HTTP-specific step required. Validation failures map
/// to `422 Unprocessable Entity` with the per-field violations; handler
/// failures map to a generic `500` (the detail is logged, not returned).
///
/// A command marked `#[command(internal)]` ([`Command::INTERNAL_ONLY`]) is
/// refused here before the handler runs, mapping to `404 Not Found` — see
/// [`HandlerError::internal_command`]. `run_command_from_state` delegates to
/// this function, so the guard covers both HTTP entry points.
pub async fn run_command<C, H>(
    handler: CommandHandlerState<C, H>,
    Json(command): Json<C>,
) -> Result<Json<H::Output>, HandlerError>
where
    C: Command + DeserializeOwned,
    H: CommandHandler<C>,
    H::Output: Serialize,
{
    if C::INTERNAL_ONLY {
        return Err(HandlerError::internal_command(C::NAME));
    }
    pharos_app::dispatch(&*handler.handler, command)
        .await
        .map(Json)
        .map_err(|error| match error {
            DispatchError::Validation(error) => HandlerError::validation(&error),
            DispatchError::Handler(error) => HandlerError::internal(error),
        })
}

/// Executes a query handler using URL query parameters and a JSON response.
pub async fn run_query<Q, H>(
    handler: QueryHandlerState<Q, H>,
    QueryPayload(query): QueryPayload<Q>,
) -> Result<Json<Q::Result>, HandlerError>
where
    Q: Query + DeserializeOwned,
    H: QueryHandler<Q>,
    Q::Result: Serialize,
{
    pharos_app::query_dispatch(&*handler.handler, query)
        .await
        .map(Json)
        .map_err(HandlerError::internal)
}

/// Convenience helper for explicit Axum state extraction when you do not want a
/// dedicated wrapper extractor in the route signature.
///
/// Applies the same validation and error mapping as [`run_command`].
pub async fn run_command_from_state<S, C, H>(
    State(handler): State<Arc<H>>,
    Json(command): Json<C>,
) -> Result<Json<H::Output>, HandlerError>
where
    S: Send + Sync,
    C: Command + DeserializeOwned,
    H: CommandHandler<C>,
    H::Output: Serialize,
{
    run_command(
        CommandHandlerState::<C, H>::from_arc(handler),
        Json(command),
    )
    .await
}

/// Convenience helper for explicit Axum state extraction when you do not want a
/// dedicated wrapper extractor in the route signature.
pub async fn run_query_from_state<S, Q, H>(
    State(handler): State<Arc<H>>,
    QueryPayload(query): QueryPayload<Q>,
) -> Result<Json<Q::Result>, HandlerError>
where
    S: Send + Sync,
    Q: Query + DeserializeOwned,
    H: QueryHandler<Q>,
    Q::Result: Serialize,
{
    run_query(
        QueryHandlerState::<Q, H>::from_arc(handler),
        QueryPayload(query),
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::Router;
    use axum::body::{Body, to_bytes};
    use axum::routing::{get, post};
    use http::{Method, Request};
    use serde::{Deserialize, Serialize};
    use tower::ServiceExt;

    #[derive(Clone)]
    struct AppState {
        greet: Arc<GreetHandler>,
        lookup: Arc<LookupHandler>,
    }

    impl FromRef<AppState> for Arc<GreetHandler> {
        fn from_ref(state: &AppState) -> Self {
            Arc::clone(&state.greet)
        }
    }

    impl FromRef<AppState> for Arc<LookupHandler> {
        fn from_ref(state: &AppState) -> Self {
            Arc::clone(&state.lookup)
        }
    }

    #[derive(Debug, Deserialize)]
    struct Greet {
        name: String,
    }

    impl Command for Greet {
        const NAME: &'static str = "Greet";
    }

    struct GreetHandler;

    impl CommandHandler<Greet> for GreetHandler {
        type Output = Greeting;
        type Error = std::convert::Infallible;

        async fn handle(&self, command: Greet) -> Result<Self::Output, Self::Error> {
            Ok(Greeting {
                message: format!("hello {}", command.name),
            })
        }
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
    struct Greeting {
        message: String,
    }

    #[derive(Debug, Deserialize)]
    struct Double {
        value: u32,
    }

    impl Query for Double {
        type Result = Doubled;
        const NAME: &'static str = "Double";
    }

    struct LookupHandler;

    impl QueryHandler<Double> for LookupHandler {
        type Error = std::convert::Infallible;

        async fn handle(&self, query: Double) -> Result<Doubled, Self::Error> {
            Ok(Doubled {
                value: query.value * 2,
            })
        }
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
    struct Doubled {
        value: u32,
    }

    async fn greet_route(
        handler: CommandHandlerState<Greet, GreetHandler>,
        payload: Json<Greet>,
    ) -> Result<Json<Greeting>, HandlerError> {
        run_command(handler, payload).await
    }

    async fn query_route(
        handler: QueryHandlerState<Double, LookupHandler>,
        params: QueryPayload<Double>,
    ) -> Result<Json<Doubled>, HandlerError> {
        run_query(handler, params).await
    }

    #[tokio::test]
    async fn command_extractor_invokes_handler_from_state() -> Result<(), Box<dyn std::error::Error>>
    {
        let app = Router::new()
            .route("/commands/greet", post(greet_route))
            .route("/queries/double", get(query_route))
            .with_state(AppState {
                greet: Arc::new(GreetHandler),
                lookup: Arc::new(LookupHandler),
            });

        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/commands/greet")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"name":"pharos"}"#))?,
            )
            .await?;

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        let payload: Greeting = serde_json::from_slice(&body)?;
        assert_eq!(payload.message, "hello pharos");
        Ok(())
    }

    #[tokio::test]
    async fn query_extractor_invokes_handler_from_state() -> Result<(), Box<dyn std::error::Error>>
    {
        let app = Router::new()
            .route("/queries/double", get(query_route))
            .with_state(AppState {
                greet: Arc::new(GreetHandler),
                lookup: Arc::new(LookupHandler),
            });

        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/queries/double?value=21")
                    .body(Body::empty())?,
            )
            .await?;

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        let payload: Doubled = serde_json::from_slice(&body)?;
        assert_eq!(payload.value, 42);
        Ok(())
    }

    #[tokio::test]
    async fn run_command_refuses_internal_only_commands() -> Result<(), Box<dyn std::error::Error>>
    {
        use std::sync::atomic::{AtomicBool, Ordering};

        #[derive(Debug, Deserialize)]
        struct ReleasePayout {
            #[allow(dead_code)]
            amount: u64,
        }

        impl Command for ReleasePayout {
            const NAME: &'static str = "ReleasePayout";
            // The whole point under test: an internal-only command.
            const INTERNAL_ONLY: bool = true;
        }

        struct PayoutHandler {
            ran: Arc<AtomicBool>,
        }

        impl CommandHandler<ReleasePayout> for PayoutHandler {
            type Output = Greeting;
            type Error = std::convert::Infallible;

            async fn handle(&self, _command: ReleasePayout) -> Result<Self::Output, Self::Error> {
                self.ran.store(true, Ordering::SeqCst);
                Ok(Greeting {
                    message: "paid".into(),
                })
            }
        }

        async fn payout_route(
            handler: CommandHandlerState<ReleasePayout, PayoutHandler>,
            payload: Json<ReleasePayout>,
        ) -> Result<Json<Greeting>, HandlerError> {
            run_command(handler, payload).await
        }

        let ran = Arc::new(AtomicBool::new(false));
        let handler = Arc::new(PayoutHandler {
            ran: Arc::clone(&ran),
        });
        // State is the bare `Arc<H>`; axum's blanket `FromRef<T> for T` lets the
        // `CommandHandlerState` extractor pull it straight out.
        let app = Router::new()
            .route("/commands/payout", post(payout_route))
            .with_state(handler);

        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/commands/payout")
                    .header("content-type", "application/json")
                    // A well-formed body, so only the internal-only guard can
                    // be responsible for the rejection.
                    .body(Body::from(r#"{"amount":100}"#))?,
            )
            .await?;

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert!(
            !ran.load(Ordering::SeqCst),
            "the internal-only command handler must never run over HTTP"
        );
        Ok(())
    }

    #[tokio::test]
    async fn command_handler_state_dispatch_also_refuses_internal_only_commands() {
        use std::sync::atomic::{AtomicBool, Ordering};

        #[derive(Debug, Deserialize)]
        struct ReleasePayout;

        impl Command for ReleasePayout {
            const NAME: &'static str = "ReleasePayout";
            const INTERNAL_ONLY: bool = true;
        }

        struct PayoutHandler {
            ran: Arc<AtomicBool>,
        }

        impl CommandHandler<ReleasePayout> for PayoutHandler {
            type Output = Greeting;
            type Error = std::convert::Infallible;

            async fn handle(&self, _command: ReleasePayout) -> Result<Self::Output, Self::Error> {
                self.ran.store(true, Ordering::SeqCst);
                Ok(Greeting {
                    message: "paid".into(),
                })
            }
        }

        let ran = Arc::new(AtomicBool::new(false));
        let handler = Arc::new(PayoutHandler {
            ran: Arc::clone(&ran),
        });

        // Exercises `CommandHandlerState::dispatch` directly — the seam the
        // framework's own examples call with `handler.dispatch(command).await?`,
        // bypassing `run_command` entirely. Before this guard, an internal-only
        // command wired to a route this way ran unprotected.
        let state = CommandHandlerState::<ReleasePayout, PayoutHandler>::from_arc(handler);
        let result = state.dispatch(ReleasePayout).await;

        assert!(matches!(result, Err(DispatchError::Validation(_))));
        assert!(
            !ran.load(Ordering::SeqCst),
            "the internal-only command handler must never run through this seam either"
        );
    }

    #[derive(Debug)]
    struct SensitiveAdapterError;

    impl Display for SensitiveAdapterError {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "connection to postgres://prod-db.internal:5432 refused")
        }
    }

    impl std::error::Error for SensitiveAdapterError {}

    impl ClassifiedError for SensitiveAdapterError {
        fn kind(&self) -> ErrorKind {
            ErrorKind::Internal
        }

        fn public_message(&self) -> String {
            "internal error".to_string()
        }
    }

    #[test]
    fn status_for_maps_every_kind_to_its_conventional_status() {
        assert_eq!(status_for(ErrorKind::NotFound), StatusCode::NOT_FOUND);
        assert_eq!(
            status_for(ErrorKind::Validation),
            StatusCode::UNPROCESSABLE_ENTITY
        );
        assert_eq!(status_for(ErrorKind::Conflict), StatusCode::CONFLICT);
        assert_eq!(
            status_for(ErrorKind::Unauthorized),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(status_for(ErrorKind::Forbidden), StatusCode::FORBIDDEN);
        assert_eq!(
            status_for(ErrorKind::RateLimited),
            StatusCode::TOO_MANY_REQUESTS
        );
        assert_eq!(
            status_for(ErrorKind::Unavailable),
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            status_for(ErrorKind::Internal),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }

    #[test]
    fn from_classified_never_puts_the_adapter_error_text_in_the_response() {
        let error = HandlerError::from_classified(&SensitiveAdapterError);
        assert_eq!(error.status(), StatusCode::INTERNAL_SERVER_ERROR);
        // The whole contract: whatever `Display` says about this error (a
        // connection string, here) must never reach the client, even though
        // it's exactly what gets logged via `tracing::error!` a line above.
        assert!(!error.message().contains("postgres://"));
        assert!(!error.message().contains("5432"));
        assert_eq!(error.message(), "internal error");
    }

    #[test]
    fn from_classified_returns_domain_authored_text_for_non_internal_kinds() {
        let domain_error = DomainError::Conflict("order already shipped".into());
        let error = HandlerError::from_classified(&domain_error);
        assert_eq!(error.status(), StatusCode::CONFLICT);
        assert!(error.message().contains("order already shipped"));
    }
}
