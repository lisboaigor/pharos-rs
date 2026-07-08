use thiserror::Error;

/// Common error type for domain validation and business rule failures.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum DomainError {
    #[error("not found: {0}")]
    NotFound(String),

    #[error("validation: {0}")]
    Validation(String),

    #[error("business rule violation: {0}")]
    BusinessRule(String),

    #[error("conflict: {0}")]
    Conflict(String),
}

/// Convenience alias for domain operations.
pub type DomainResult<T> = Result<T, DomainError>;
