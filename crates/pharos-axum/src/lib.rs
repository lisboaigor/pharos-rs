//! Axum integration for Pharos handlers.
//!
//! This crate keeps HTTP concerns outside `pharos-app` while making it easy to
//! expose command and query handlers through Axum routes.

use std::fmt::{Display, Formatter};
use std::marker::PhantomData;
use std::sync::Arc;

use axum::extract::{FromRef, FromRequestParts, Query as QueryPayload};
use axum::http::{StatusCode, request::Parts};
use axum::response::{IntoResponse, Response};
use axum::{Json, extract::State};
use pharos_app::{Command, CommandHandler, Query, QueryHandler};
use serde::Serialize;
use serde::de::DeserializeOwned;

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
    pub fn internal(error: impl Display) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
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
    /// Dispatches a command through the framework's instrumentation seam.
    ///
    /// Equivalent to [`pharos_app::dispatch`] over the extracted handler, but
    /// keeps the route handler free of `.handler().as_ref()` plumbing. The
    /// handler's own error type is returned, so callers map it to whatever
    /// HTTP response they want.
    pub async fn dispatch(&self, command: C) -> Result<H::Output, H::Error> {
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
pub async fn run_command<C, H>(
    handler: CommandHandlerState<C, H>,
    Json(command): Json<C>,
) -> Result<Json<H::Output>, HandlerError>
where
    C: Command + DeserializeOwned,
    H: CommandHandler<C>,
    H::Output: Serialize,
{
    handler
        .handler
        .handle(command)
        .await
        .map(Json)
        .map_err(HandlerError::internal)
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
    handler
        .handler
        .handle(query)
        .await
        .map(Json)
        .map_err(HandlerError::internal)
}

/// Convenience helper for explicit Axum state extraction when you do not want a
/// dedicated wrapper extractor in the route signature.
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
    CommandHandlerState::<C, H>::from_arc(handler)
        .handler
        .handle(command)
        .await
        .map(Json)
        .map_err(HandlerError::internal)
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
    QueryHandlerState::<Q, H>::from_arc(handler)
        .handler
        .handle(query)
        .await
        .map(Json)
        .map_err(HandlerError::internal)
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
    async fn command_extractor_invokes_handler_from_state() {
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
                    .body(Body::from(r#"{"name":"pharos"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: Greeting = serde_json::from_slice(&body).unwrap();
        assert_eq!(payload.message, "hello pharos");
    }

    #[tokio::test]
    async fn query_extractor_invokes_handler_from_state() {
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
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: Doubled = serde_json::from_slice(&body).unwrap();
        assert_eq!(payload.value, 42);
    }
}
