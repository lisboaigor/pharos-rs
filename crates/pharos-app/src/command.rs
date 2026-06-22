use std::error::Error;
use std::future::Future;

/// Marker trait for application commands.
pub trait Command: Send + Sync + 'static {}

/// Handles a command and returns either an output value or an error.
pub trait CommandHandler<C: Command>: Send + Sync + 'static {
    /// Successful command result type.
    type Output: Send + Sync + 'static;
    /// Concrete error type returned by the handler.
    type Error: Error + Send + Sync + 'static;

    /// Executes the command.
    fn handle(&self, command: C) -> impl Future<Output = Result<Self::Output, Self::Error>> + Send;
}
