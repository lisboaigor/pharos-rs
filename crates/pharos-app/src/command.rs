use std::error::Error;
use std::future::Future;

use tracing::{Instrument, Span, info_span};

/// One field-level input violation.
///
/// `path` addresses the offending field (e.g. `quantity` or `items[2].price`)
/// and `message` is a human-readable description safe to echo back to the
/// caller, since it describes the caller's own input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldViolation {
    /// Path to the field that failed validation.
    pub path: String,
    /// Human-readable description of the violation.
    pub message: String,
}

impl std::fmt::Display for FieldViolation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.path.is_empty() {
            write!(f, "{}", self.message)
        } else {
            write!(f, "{}: {}", self.path, self.message)
        }
    }
}

/// Input-validation failure raised before a command handler runs.
///
/// This is the application-layer contract for validation errors: ports and
/// adapters (HTTP, tower, tests) depend on this neutral type, never on the
/// validation library that produced it. `#[derive(Command)]` converts a
/// `garde::Report` into this type inside the generated code, so `pharos-app`
/// itself has no dependency on any validator — swap the validation library and
/// every port stays stable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidationError {
    violations: Vec<FieldViolation>,
}

impl ValidationError {
    /// Creates a validation error from a list of field violations.
    pub fn new(violations: Vec<FieldViolation>) -> Self {
        Self { violations }
    }

    /// Creates a validation error with a single violation.
    pub fn violation(path: impl Into<String>, message: impl Into<String>) -> Self {
        Self::new(vec![FieldViolation {
            path: path.into(),
            message: message.into(),
        }])
    }

    /// Returns the collected per-field violations.
    pub fn violations(&self) -> &[FieldViolation] {
        &self.violations
    }

    /// Returns `true` when no violation was recorded.
    pub fn is_empty(&self) -> bool {
        self.violations.is_empty()
    }
}

impl std::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "invalid input")?;
        for (i, violation) in self.violations.iter().enumerate() {
            let sep = if i == 0 { ": " } else { "; " };
            write!(f, "{sep}{violation}")?;
        }
        Ok(())
    }
}

impl Error for ValidationError {}

/// Marker trait for application commands.
pub trait Command: Send + Sync + 'static {
    /// Stable label for this command in traces and metrics.
    ///
    /// It is independent of any wire or serialization name, so spans stay
    /// meaningful even if the type is renamed. [`dispatch`] uses it to populate
    /// the `command.handle` span, which is why it cannot drift from the type
    /// the way a hand-written string literal in each handler could.
    const NAME: &'static str;

    /// Whether this command may only be dispatched internally — by a saga, a
    /// worker, or an event handler — and must never be reachable over HTTP.
    ///
    /// Defaults to `false`. `#[derive(Command)]` sets it to `true` for a
    /// command annotated `#[command(internal)]`. The HTTP entry points in
    /// `pharos-axum` (`run_command` / `run_command_from_state`) refuse to
    /// execute a command whose `INTERNAL_ONLY` is `true`, so a privileged
    /// command (a payout, a refund, a saga-only `StartGame`) that is
    /// accidentally wired to a route is rejected by the framework instead of
    /// silently exposed. It is defense in depth layered on top of simply not
    /// registering the route: the in-process dispatch paths a saga uses
    /// ([`dispatch`], [`CommandHandler::handle`]) are unaffected — those are
    /// exactly how an internal command is *meant* to run.
    const INTERNAL_ONLY: bool = false;

    /// Builds the tracing span under which this command is handled.
    ///
    /// This bare trait default carries only `command = Self::NAME`.
    /// [`#[derive(Command)]`] overrides it with a span recording **every**
    /// field, so an error in the logs can be traced back to the arguments that
    /// caused it; see the derive's docs for opting a field out with
    /// `#[trace(skip)]`.
    ///
    /// Implement it by hand only when the fields to record are not the struct's
    /// own (a computed total, the length of a collection):
    ///
    /// ```ignore
    /// fn trace_span(&self) -> tracing::Span {
    ///     tracing::info_span!(
    ///         "command.handle",
    ///         command = Self::NAME,
    ///         order_id = %self.order_id,
    ///         itens = self.itens.len(),
    ///     )
    /// }
    /// ```
    fn trace_span(&self) -> Span {
        info_span!("command.handle", command = Self::NAME)
    }

    /// Validates the command's input fields before the handler runs.
    ///
    /// The default is a no-op. [`#[derive(Command)]`] generates an override
    /// automatically when it detects `#[garde(...)]` annotations on the struct's
    /// fields. Adding `#[derive(garde::Validate)]` on the same struct is still
    /// required — it is what makes the generated code compile. The generated
    /// override converts the `garde::Report` into the neutral
    /// [`ValidationError`] inline, so this crate never depends on garde.
    ///
    /// ```ignore
    /// // validation is opt-in per field; no extra attribute needed beyond garde's own
    /// #[derive(Command, Deserialize, Validate)]
    /// pub struct AddItem {
    ///     #[garde(skip)]
    ///     pub order_id: Uuid,
    ///     #[garde(length(min = 1, max = 255))]
    ///     pub description: String,
    ///     #[garde(range(min = 1))]
    ///     pub quantity: u32,
    /// }
    ///
    /// // commands without garde annotations need neither Validate nor #[garde(skip)]
    /// #[derive(Command, Deserialize)]
    /// pub struct ConfirmOrder {
    ///     pub order_id: Uuid,
    /// }
    /// ```
    fn validate_input(&self) -> Result<(), ValidationError> {
        Ok(())
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

/// Error returned by [`dispatch`]: either the input failed validation before
/// the handler ran, or the handler itself failed.
///
/// Validation is part of the dispatch seam — not of any particular transport —
/// so a command entering through HTTP, a worker, or a broker consumer is
/// validated identically. Callers that need to distinguish the two failure
/// modes (e.g. to map validation to `422` and handler failures to `500`) match
/// on the variant.
#[derive(Debug, thiserror::Error)]
pub enum DispatchError<E: Error> {
    /// The command's input failed [`Command::validate_input`]; the handler
    /// never ran.
    #[error(transparent)]
    Validation(ValidationError),
    /// The handler ran and failed.
    #[error(transparent)]
    Handler(E),
}

/// Dispatches a command to its handler inside the command's tracing span.
///
/// This is the framework's instrumentation *and validation* seam:
/// [`Command::validate_input`] runs first (commands without validation rules
/// use the no-op default), then the handler executes under the `command.handle`
/// span built by [`Command::trace_span`]. Handlers stay pure business logic,
/// and every entry port — HTTP, worker, broker consumer — validates and
/// observes commands identically. Prefer this over calling
/// [`CommandHandler::handle`] directly.
pub async fn dispatch<C, H>(handler: &H, command: C) -> Result<H::Output, DispatchError<H::Error>>
where
    C: Command,
    H: CommandHandler<C>,
{
    command
        .validate_input()
        .map_err(DispatchError::Validation)?;
    let span = command.trace_span();
    handler
        .handle(command)
        .instrument(span)
        .await
        .map_err(DispatchError::Handler)
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
    async fn dispatch_runs_handler_and_returns_output() -> Result<(), Box<dyn Error>> {
        let out = dispatch(&IncrementHandler, Increment(41)).await?;
        assert_eq!(out, 42);
        Ok(())
    }

    struct Rejecting(u32);

    impl Command for Rejecting {
        const NAME: &'static str = "Rejecting";

        fn validate_input(&self) -> Result<(), ValidationError> {
            if self.0 == 0 {
                return Err(ValidationError::violation("0", "value must be positive"));
            }
            Ok(())
        }
    }

    struct RejectingHandler;

    impl CommandHandler<Rejecting> for RejectingHandler {
        type Output = u32;
        type Error = std::convert::Infallible;

        async fn handle(&self, cmd: Rejecting) -> Result<u32, Self::Error> {
            Ok(cmd.0)
        }
    }

    #[tokio::test]
    async fn dispatch_rejects_invalid_input_before_the_handler_runs() -> Result<(), Box<dyn Error>>
    {
        let result = dispatch(&RejectingHandler, Rejecting(0)).await;
        let Err(DispatchError::Validation(error)) = result else {
            panic!("expected a validation error, got {result:?}");
        };
        assert_eq!(error.violations().len(), 1);
        assert_eq!(error.violations()[0].path, "0");

        // Valid input reaches the handler normally.
        let out = dispatch(&RejectingHandler, Rejecting(7)).await?;
        assert_eq!(out, 7);
        Ok(())
    }

    #[test]
    fn name_is_exposed_as_a_stable_label() {
        assert_eq!(Increment::NAME, "Increment");
    }

    #[test]
    fn validate_input_is_a_noop_by_default() {
        // Commands that don't override validate_input always return Ok(()).
        assert!(Increment(0).validate_input().is_ok());
    }

    #[test]
    fn commands_are_http_reachable_by_default() {
        // Only commands that explicitly opt into `INTERNAL_ONLY` are refused
        // by the HTTP entry points; the default keeps every command routable.
        const { assert!(!Increment::INTERNAL_ONLY) };
    }

    struct Payout;

    impl Command for Payout {
        const NAME: &'static str = "Payout";
        const INTERNAL_ONLY: bool = true;
    }

    #[test]
    fn a_command_can_be_marked_internal_only() {
        const { assert!(Payout::INTERNAL_ONLY) };
    }

    // The span name ("command.handle") and its field contents are asserted
    // end-to-end in the `order` example's instrumentation test, where a real
    // capturing subscriber is installed.
}
