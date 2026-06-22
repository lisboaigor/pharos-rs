use std::error::Error;
use std::future::Future;

use tracing::{Instrument, Span, info_span};

/// Marker trait for application commands.
pub trait Command: Send + Sync + 'static {
    /// Stable label for this command in traces and metrics.
    ///
    /// It is independent of any wire or serialization name, so spans stay
    /// meaningful even if the type is renamed. [`dispatch`] uses it to populate
    /// the `command.handle` span, which is why it cannot drift from the type
    /// the way a hand-written string literal in each handler could.
    const NAME: &'static str;

    /// Builds the tracing span under which this command is handled.
    ///
    /// The default span carries only `command = Self::NAME`. Override it to
    /// record command-specific fields (ids, quantities, …): they describe the
    /// command's own data, so they live on the DTO and keep handlers free of
    /// any tracing concern.
    ///
    /// ```ignore
    /// fn trace_span(&self) -> tracing::Span {
    ///     tracing::info_span!(
    ///         "command.handle",
    ///         command = Self::NAME,
    ///         order_id = %self.order_id,
    ///     )
    /// }
    /// ```
    fn trace_span(&self) -> Span {
        info_span!("command.handle", command = Self::NAME)
    }
}

/// Handles a command and returns either an output value or an error.
pub trait CommandHandler<C: Command>: Send + Sync + 'static {
    /// Successful command result type.
    type Output: Send + Sync + 'static;
    /// Concrete error type returned by the handler.
    type Error: Error + Send + Sync + 'static;

    /// Executes the command.
    ///
    /// Implementations should contain business logic only; tracing is applied
    /// by [`dispatch`]. Prefer dispatching over calling this directly.
    fn handle(&self, command: C) -> impl Future<Output = Result<Self::Output, Self::Error>> + Send;
}

/// Dispatches a command to its handler inside the command's tracing span.
///
/// This is the framework's instrumentation seam. Handlers stay pure business
/// logic while the `command.handle` span — and its fields, via
/// [`Command::trace_span`] — is applied here, consistently and impossible to
/// forget. Prefer this over calling [`CommandHandler::handle`] directly so that
/// every command is observed the same way.
pub async fn dispatch<C, H>(handler: &H, command: C) -> Result<H::Output, H::Error>
where
    C: Command,
    H: CommandHandler<C>,
{
    let span = command.trace_span();
    handler.handle(command).instrument(span).await
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Increment(u32);
    impl Command for Increment {
        const NAME: &'static str = "Increment";
    }

    struct IncrementHandler;
    impl CommandHandler<Increment> for IncrementHandler {
        type Output = u32;
        type Error = std::convert::Infallible;

        async fn handle(&self, cmd: Increment) -> Result<u32, Self::Error> {
            Ok(cmd.0 + 1)
        }
    }

    #[tokio::test]
    async fn dispatch_runs_handler_and_returns_output() {
        let out = dispatch(&IncrementHandler, Increment(41)).await.unwrap();
        assert_eq!(out, 42);
    }

    #[test]
    fn name_is_exposed_as_a_stable_label() {
        assert_eq!(Increment::NAME, "Increment");
    }

    // The span name ("command.handle") and its field contents are asserted
    // end-to-end in the `order` example's instrumentation test, where a real
    // capturing subscriber is installed.
}
