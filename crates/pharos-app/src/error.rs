use pharos_core::DomainError;
use thiserror::Error;

use crate::event_bus::EventBusError;
use crate::outbox::OutboxError;

/// Concrete error returned by the application-layer orchestration helpers.
///
/// A concrete enum lets callers match on the failure mode (for example, retry
/// on [`ConcurrencyConflict`](ApplicationError::ConcurrencyConflict)) instead of
/// inspecting an opaque boxed error.
#[derive(Debug, Error)]
pub enum ApplicationError {
    /// A domain invariant or business rule was violated.
    #[error(transparent)]
    Domain(#[from] DomainError),

    /// The aggregate repository failed to persist or load state.
    #[error("repository failure: {0}")]
    Repository(String),

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
