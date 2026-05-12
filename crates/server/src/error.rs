use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::json;

/// HTTP API 层返回的错误。
#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    /// 请求的任务不存在。
    #[error("task not found")]
    TaskNotFound,
    /// 发生了意外的内部错误。
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
