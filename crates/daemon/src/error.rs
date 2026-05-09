use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::json;

/// Errors returned by the HTTP API layer.
#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    /// The requested task does not exist.
    #[error("task not found")]
    TaskNotFound,
    /// An unexpected internal error occurred.
    #[error("internal error: {0}")]
    Internal(String),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match &self {
            ApiError::TaskNotFound => (StatusCode::NOT_FOUND, self.to_string()),
            ApiError::Internal(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg.clone()),
        };
        (status, axum::Json(json!({ "error": message }))).into_response()
    }
}
