use std::error::Error;
use std::future::Future;

use pharos_core::DomainEvent;

/// Handles a concrete domain event.
pub trait EventHandler<E: DomainEvent>: Send + Sync + 'static {
    /// Concrete error type returned by the handler.
    type Error: Error + Send + Sync + 'static;

    /// Reacts to a published event.
    fn handle(&self, event: &E) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
