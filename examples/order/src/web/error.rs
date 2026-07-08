use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use pharos_core::DomainError;
use tower::BoxError;

use crate::application::error::AppError;

/// Web-layer error wrapper that maps an [`AppError`] to a meaningful HTTP status
/// instead of a blanket `500`.
///
/// Keeping the mapping here — at the boundary — leaves the application error type
/// free of any HTTP knowledge.
pub struct ApiError(AppError);

impl From<AppError> for ApiError {
    fn from(error: AppError) -> Self {
        Self(error)
    }
}

impl From<pharos_app::DispatchError<AppError>> for ApiError {
    fn from(error: pharos_app::DispatchError<AppError>) -> Self {
        match error {
            // Validation failed before the handler ran; the AppError variant
            // maps it to 422 below.
            pharos_app::DispatchError::Validation(error) => Self(AppError::Validation(error)),
            pharos_app::DispatchError::Handler(error) => Self(error),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match &self.0 {
            AppError::Domain(DomainError::NotFound(_)) => StatusCode::NOT_FOUND,
            AppError::Domain(DomainError::Validation(_)) => StatusCode::BAD_REQUEST,
            AppError::Domain(DomainError::BusinessRule(_) | DomainError::Conflict(_)) => {
                StatusCode::CONFLICT
            }
            // `DomainError` is non_exhaustive; unknown future variants are
            // still domain-authored rejections of the request.
            AppError::Domain(_) => StatusCode::UNPROCESSABLE_ENTITY,
            AppError::Validation(_) => StatusCode::UNPROCESSABLE_ENTITY,
            AppError::Infra(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (status, self.0.to_string()).into_response()
    }
}

/// Converts errors surfaced by the tower middleware into HTTP responses.
pub async fn handle_middleware_error(error: BoxError) -> Response {
    if error.is::<tower::timeout::error::Elapsed>() {
        (StatusCode::REQUEST_TIMEOUT, "request timed out").into_response()
    } else {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("unhandled internal error: {error}"),
        )
            .into_response()
    }
}
