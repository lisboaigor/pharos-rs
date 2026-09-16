use thiserror::Error;

use crate::classify::{ClassifiedError, ErrorKind};

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

impl ClassifiedError for DomainError {
    fn kind(&self) -> ErrorKind {
        match self {
            DomainError::NotFound(_) => ErrorKind::NotFound,
            DomainError::Validation(_) => ErrorKind::Validation,
            DomainError::BusinessRule(_) | DomainError::Conflict(_) => ErrorKind::Conflict,
        }
    }

    fn public_message(&self) -> String {
        // Every `DomainError` message is authored by the domain itself, in
        // business language, about the request that was made — never about
        // how storage or a broker underneath it failed. That's exactly the
        // detail this trait exists to let through, so it's safe verbatim.
        self.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_variant_classifies_to_its_conventional_kind() {
        assert_eq!(
            DomainError::NotFound("x".into()).kind(),
            ErrorKind::NotFound
        );
        assert_eq!(
            DomainError::Validation("x".into()).kind(),
            ErrorKind::Validation
        );
        assert_eq!(
            DomainError::BusinessRule("x".into()).kind(),
            ErrorKind::Conflict
        );
        assert_eq!(
            DomainError::Conflict("x".into()).kind(),
            ErrorKind::Conflict
        );
    }

    #[test]
    fn public_message_matches_display_for_domain_authored_text() {
        let error = DomainError::BusinessRule("credit limit exceeded".into());
        assert_eq!(error.public_message(), error.to_string());
        assert!(error.public_message().contains("credit limit exceeded"));
    }
}
