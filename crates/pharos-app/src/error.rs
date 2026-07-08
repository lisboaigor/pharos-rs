use pharos_core::DomainError;
use thiserror::Error;

use crate::event_bus::EventBusError;
use pharos_messaging::outbox::OutboxError;

/// Concrete error returned by the application-layer orchestration helpers.
///
/// A concrete enum lets callers match on the failure mode (for example, retry
/// on [`ConcurrencyConflict`](ApplicationError::ConcurrencyConflict)) instead of
/// inspecting an opaque boxed error.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ApplicationError {
    /// A domain invariant or business rule was violated.
    #[error(transparent)]
    Domain(#[from] DomainError),

    /// The aggregate repository failed to persist or load state.
    ///
    /// The original adapter error is kept as the typed `source`, so callers
    /// can walk the chain (e.g. down to a `sqlx::Error`) instead of matching
    /// on strings.
    #[error("repository failure: {0}")]
    Repository(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),

    /// An optimistic-concurrency conflict was detected while saving.
    #[error("optimistic concurrency conflict: expected version {expected}, found {actual:?}")]
    ConcurrencyConflict {
        /// Version the in-memory aggregate was loaded at.
        expected: u64,
        /// Version currently stored, when known.
        actual: Option<u64>,
    },

    /// An in-process event handler failed.
    #[error(transparent)]
    EventBus(#[from] EventBusError),

    /// Writing to the outbox failed.
    #[error(transparent)]
    Outbox(#[from] OutboxError),
}
