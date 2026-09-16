use pharos_core::{ClassifiedError, DomainError, ErrorKind};
use thiserror::Error;

use crate::event_bus::EventBusError;
#[cfg(feature = "messaging")]
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
    #[cfg(feature = "messaging")]
    #[error(transparent)]
    Outbox(#[from] OutboxError),
}

impl ClassifiedError for ApplicationError {
    fn kind(&self) -> ErrorKind {
        match self {
            // Delegates instead of hardcoding: the domain is the one that
            // knows whether this particular rejection is a not-found, a
            // validation failure, or a conflict.
            ApplicationError::Domain(e) => e.kind(),
            ApplicationError::ConcurrencyConflict { .. } => ErrorKind::Conflict,
            ApplicationError::Repository(_) => ErrorKind::Internal,
            ApplicationError::EventBus(_) => ErrorKind::Internal,
            #[cfg(feature = "messaging")]
            ApplicationError::Outbox(_) => ErrorKind::Internal,
        }
    }

    fn public_message(&self) -> String {
        match self {
            ApplicationError::Domain(e) => e.public_message(),
            ApplicationError::ConcurrencyConflict { .. } => self.to_string(),
            // `Repository`, `EventBus`, and `Outbox` each wrap a
            // lower-layer adapter's own error (storage, an in-process
            // handler panic path, a broker) — none of that is this layer's
            // to describe to a caller. The detail stays reachable through
            // `source()`/`Display` for `tracing::error!`, never through
            // this method.
            _ => "internal application error".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use pharos_core::DomainError;

    use super::*;

    #[derive(Debug, thiserror::Error)]
    #[error("duplicate key value violates constraint orders_pkey")]
    struct SensitiveAdapterError;

    #[test]
    fn domain_variant_delegates_kind_and_message_to_the_wrapped_domain_error() {
        let error = ApplicationError::Domain(DomainError::Conflict("already shipped".into()));
        assert_eq!(error.kind(), ErrorKind::Conflict);
        assert!(error.public_message().contains("already shipped"));
    }

    #[test]
    fn concurrency_conflict_classifies_with_its_own_message() {
        let error = ApplicationError::ConcurrencyConflict {
            expected: 1,
            actual: Some(2),
        };
        assert_eq!(error.kind(), ErrorKind::Conflict);
        assert!(error.public_message().contains("expected version 1"));
    }

    #[test]
    fn repository_and_event_bus_failures_never_leak_the_wrapped_adapter_error() {
        let repo_error = ApplicationError::Repository(Box::new(SensitiveAdapterError));
        assert_eq!(repo_error.kind(), ErrorKind::Internal);
        assert!(!repo_error.public_message().contains("orders_pkey"));
        assert_eq!(repo_error.public_message(), "internal application error");
    }
}
