use pharos_core::DomainError;
use thiserror::Error;

/// Application-layer error that combines domain and infrastructure failures
/// into a single concrete type implementing `std::error::Error`.
#[derive(Debug, Error)]
pub enum AppError {
    #[error(transparent)]
    Domain(#[from] DomainError),

    #[error("infrastructure: {0}")]
    Infra(String),
}

impl AppError {
    pub fn infra(e: impl std::fmt::Display) -> Self {
        AppError::Infra(e.to_string())
    }
}
