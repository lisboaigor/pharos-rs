//! Adapters exposing command and query handlers as [`tower::Service`]s.
//!
//! Wrapping a handler in [`CommandHandlerService`] or [`QueryHandlerService`]
//! lets it slot into a Tower stack, so middleware such as timeouts, rate
//! limiting, concurrency limits, and retries compose around your application
//! handlers without changing them.
//!
//! Available with the `tower` feature.

use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use tower::Service;

use crate::command::{Command, CommandHandler};
use crate::query::{Query, QueryHandler};

type BoxFuture<T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send>>;

/// A [`CommandHandler`] exposed as a [`tower::Service`] over command `C`.
///
/// The service is cheap to clone (the handler is shared behind an [`Arc`]) and
/// is always ready, so it can be layered with arbitrary Tower middleware.
pub struct CommandHandlerService<C, H> {
    handler: Arc<H>,
    _marker: PhantomData<fn(C)>,
}

impl<C, H> CommandHandlerService<C, H> {
    /// Wraps a handler value.
    pub fn new(handler: H) -> Self {
        Self::from_arc(Arc::new(handler))
    }

    /// Wraps an already shared handler.
    pub fn from_arc(handler: Arc<H>) -> Self {
        Self {
            handler,
            _marker: PhantomData,
        }
    }
}

impl<C, H> Clone for CommandHandlerService<C, H> {
    fn clone(&self) -> Self {
        Self {
            handler: Arc::clone(&self.handler),
            _marker: PhantomData,
        }
    }
}

impl<C, H> std::fmt::Debug for CommandHandlerService<C, H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommandHandlerService")
            .finish_non_exhaustive()
    }
}

impl<C, H> Service<C> for CommandHandlerService<C, H>
where
    C: Command,
    H: CommandHandler<C>,
{
    type Response = H::Output;
    type Error = H::Error;
    type Future = BoxFuture<H::Output, H::Error>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, command: C) -> Self::Future {
        let handler = Arc::clone(&self.handler);
        Box::pin(async move { handler.handle(command).await })
    }
}

/// A [`QueryHandler`] exposed as a [`tower::Service`] over query `Q`.
///
/// The service is cheap to clone (the handler is shared behind an [`Arc`]) and
/// is always ready, so it can be layered with arbitrary Tower middleware.
pub struct QueryHandlerService<Q, H> {
    handler: Arc<H>,
    _marker: PhantomData<fn(Q)>,
}

impl<Q, H> QueryHandlerService<Q, H> {
    /// Wraps a handler value.
    pub fn new(handler: H) -> Self {
        Self::from_arc(Arc::new(handler))
    }

    /// Wraps an already shared handler.
    pub fn from_arc(handler: Arc<H>) -> Self {
        Self {
            handler,
            _marker: PhantomData,
        }
    }
}

impl<Q, H> Clone for QueryHandlerService<Q, H> {
    fn clone(&self) -> Self {
        Self {
            handler: Arc::clone(&self.handler),
            _marker: PhantomData,
        }
    }
}

impl<Q, H> std::fmt::Debug for QueryHandlerService<Q, H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryHandlerService")
            .finish_non_exhaustive()
    }
}

impl<Q, H> Service<Q> for QueryHandlerService<Q, H>
where
    Q: Query,
    H: QueryHandler<Q>,
{
    type Response = Q::Result;
    type Error = H::Error;
    type Future = BoxFuture<Q::Result, H::Error>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, query: Q) -> Self::Future {
        let handler = Arc::clone(&self.handler);
        Box::pin(async move { handler.handle(query).await })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tower::ServiceExt;

    struct Greet(String);
    impl Command for Greet {}

    struct GreetHandler;

    impl CommandHandler<Greet> for GreetHandler {
        type Output = String;
        type Error = std::convert::Infallible;

        async fn handle(&self, command: Greet) -> Result<Self::Output, Self::Error> {
            Ok(format!("hello {}", command.0))
        }
    }

    struct Lookup(u32);
    impl Query for Lookup {
        type Result = u32;
    }

    struct LookupHandler;

    impl QueryHandler<Lookup> for LookupHandler {
        type Error = std::convert::Infallible;

        async fn handle(&self, query: Lookup) -> Result<u32, Self::Error> {
            Ok(query.0 * 2)
        }
    }

    #[tokio::test]
    async fn command_service_invokes_handler() {
        let service = CommandHandlerService::new(GreetHandler);
        let output = service.oneshot(Greet("pharos".into())).await.unwrap();
        assert_eq!(output, "hello pharos");
    }

    #[tokio::test]
    async fn query_service_invokes_handler() {
        let service = QueryHandlerService::new(LookupHandler);
        let output = service.oneshot(Lookup(21)).await.unwrap();
        assert_eq!(output, 42);
    }
}
