use std::error::Error;
use std::future::Future;

use pharos_core::DomainEvent;

use crate::cascade::CascadedCommand;

/// Handles a concrete domain event.
pub trait EventHandler<E: DomainEvent>: Send + Sync + 'static {
    /// Concrete error type returned by the handler.
    type Error: Error + Send + Sync + 'static;

    /// Reacts to a published event.
    fn handle(&self, event: &E) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// Reacts to a domain event by *returning* the follow-up commands it wants
/// run, instead of calling their handlers inline.
///
/// [`EventBus::publish`](crate::event_bus::EventBus::publish) runs the
/// returned commands, in order, right after this handler; a failure among
/// them follows the same [`PublishErrorPolicy`](crate::event_bus::PublishErrorPolicy)
/// as a regular handler failure — no more silently discarding or
/// inconsistently logging the outcome of a cascaded command. Commands
/// already dispatched before a failing one are **not** undone: a cascade is
/// a best-effort chain, not a saga.
///
/// Build each returned command with [`cascade`](crate::cascade::cascade).
pub trait CascadingEventHandler<E: DomainEvent>: Send + Sync + 'static {
    /// Concrete error type returned by the handler itself (not by a cascaded
    /// command — those surface as [`CascadeError`](crate::cascade::CascadeError)).
    type Error: Error + Send + Sync + 'static;

    /// Reacts to a published event by building the commands it triggers.
    fn handle(
        &self,
        event: &E,
    ) -> impl Future<Output = Result<Vec<Box<dyn CascadedCommand>>, Self::Error>> + Send;
}
