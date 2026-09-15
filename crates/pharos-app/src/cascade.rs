//! Cascaded commands: the type-erased envelope an event handler uses to
//! *return* follow-up commands instead of calling another handler inline.
//!
//! [`Command`] is not `dyn`-safe (it carries the associated `const NAME`), so
//! a [`CascadingEventHandler`](crate::event_handler::CascadingEventHandler)
//! cannot return `Vec<Box<dyn Command>>` directly. [`cascade`] closes that gap
//! by pairing a concrete command with the handler that will run it, behind
//! the object-safe [`CascadedCommand`] trait — the same shape `EventBus` uses
//! internally to erase [`EventHandler`](crate::event_handler::EventHandler)s.

use std::error::Error;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::command::{Command, CommandHandler};

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// A command already bound to the handler that will run it, its output
/// discarded — a cascaded command is fire-and-forget from the emitting
/// event's perspective.
///
/// Build one with [`cascade`]; this trait's only implementor is private.
pub trait CascadedCommand: Send + 'static {
    /// Runs the command through its handler.
    fn dispatch(self: Box<Self>) -> BoxFuture<'static, Result<(), CascadeError>>;
}

/// A cascaded command failed. Carries the failing command's
/// [`Command::NAME`] for logs and metrics without requiring a generic error
/// type on the caller.
#[derive(Debug, thiserror::Error)]
#[error("cascaded command '{command_name}' failed: {source}")]
pub struct CascadeError {
    /// [`Command::NAME`] of the command that failed.
    pub command_name: &'static str,
    /// The handler's original error.
    #[source]
    pub source: Box<dyn Error + Send + Sync>,
}

impl CascadeError {
    fn new<C: Command>(source: impl Error + Send + Sync + 'static) -> Self {
        Self {
            command_name: C::NAME,
            source: Box::new(source),
        }
    }
}

struct Cascaded<C, H> {
    command: C,
    handler: Arc<H>,
}

impl<C, H> CascadedCommand for Cascaded<C, H>
where
    C: Command,
    H: CommandHandler<C>,
{
    fn dispatch(self: Box<Self>) -> BoxFuture<'static, Result<(), CascadeError>> {
        Box::pin(async move {
            self.handler
                .handle(self.command)
                .await
                .map(|_output| ())
                .map_err(CascadeError::new::<C>)
        })
    }
}

/// Builds a cascaded command: `command` paired with the `handler` that will
/// run it, ready to be returned from a
/// [`CascadingEventHandler`](crate::event_handler::CascadingEventHandler).
pub fn cascade<C, H>(command: C, handler: Arc<H>) -> Box<dyn CascadedCommand>
where
    C: Command,
    H: CommandHandler<C>,
{
    Box::new(Cascaded { command, handler })
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};

    use super::*;

    struct Increment(u32);

    impl Command for Increment {
        const NAME: &'static str = "Increment";
    }

    struct IncrementHandler(Arc<AtomicU32>);

    impl CommandHandler<Increment> for IncrementHandler {
        type Output = u32;
        type Error = std::convert::Infallible;

        async fn handle(&self, cmd: Increment) -> Result<u32, Self::Error> {
            let total = self.0.fetch_add(cmd.0, Ordering::SeqCst) + cmd.0;
            Ok(total)
        }
    }

    struct AlwaysFails;

    #[derive(Debug, thiserror::Error)]
    #[error("boom")]
    struct Boom;

    impl Command for AlwaysFails {
        const NAME: &'static str = "AlwaysFails";
    }

    struct FailingHandler;

    impl CommandHandler<AlwaysFails> for FailingHandler {
        type Output = ();
        type Error = Boom;

        async fn handle(&self, _cmd: AlwaysFails) -> Result<(), Self::Error> {
            Err(Boom)
        }
    }

    #[tokio::test]
    async fn dispatch_runs_the_command_through_its_bound_handler() {
        let total = Arc::new(AtomicU32::new(0));
        let handler = Arc::new(IncrementHandler(Arc::clone(&total)));

        let boxed = cascade(Increment(5), handler);
        boxed.dispatch().await.unwrap_or_else(|e| panic!("{e}"));

        assert_eq!(total.load(Ordering::SeqCst), 5);
    }

    #[tokio::test]
    async fn dispatch_surfaces_the_handler_error_with_the_command_name() {
        let boxed = cascade(AlwaysFails, Arc::new(FailingHandler));

        let Err(err) = boxed.dispatch().await else {
            panic!("expected the cascaded command to fail");
        };
        assert_eq!(err.command_name, "AlwaysFails");
        assert!(err.source.downcast_ref::<Boom>().is_some());
    }
}
