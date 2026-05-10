use crate::error::ApiError;
use crate::state::{AppState, TaskState, TaskStatus};
use axum::http::StatusCode;
use axum::{
    Json,
    extract::{Path, State},
    response::{IntoResponse, Sse},
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::BroadcastStream;

/// Request body for `POST /sync`.
#[derive(Deserialize)]
pub struct SyncRequest {
    /// The model identifier to synchronize (e.g. `Qwen/Qwen-7B-Chat`).
    pub model_id: String,
}

/// Response body for `POST /sync`.
#[derive(Serialize)]
pub struct SyncResponse {
    /// The unique task ID assigned to this synchronization request.
    pub task_id: String,
}

/// Response body for `GET /tasks/{task_id}`.
#[derive(Serialize)]
pub struct TaskResponse {
    /// Unique identifier of the task.
    pub task_id: String,
    /// Current status: `pending`, `running`, `success`, or `failed`.
    pub status: String,
    /// The model being synchronized.
    pub model_id: String,
    /// Total number of files in the repository.
    pub total_files: usize,
    /// Number of files processed so far.
    pub completed_files: usize,
    /// Cumulative bytes downloaded.
    pub downloaded_bytes: u64,
    /// Total expected bytes (may be zero until known).
    pub total_bytes: u64,
    /// Number of files that were already present and valid.
    pub cached_files: usize,
    /// Error message if the task failed.
    pub error: Option<String>,
    /// ISO-8601 timestamp when the task was created.
    pub created_at: String,
    /// ISO-8601 timestamp of the most recent update.
    pub updated_at: String,
}

impl From<TaskState> for TaskResponse {
    fn from(t: TaskState) -> Self {
        Self {
            task_id: t.task_id,
            status: format!("{:?}", t.status).to_lowercase(),
            model_id: t.model_id,
            total_files: t.total_files,
            completed_files: t.completed_files,
            downloaded_bytes: t.downloaded_bytes,
            total_bytes: t.total_bytes,
            cached_files: t.cached_files,
            error: t.error,
            created_at: t.created_at.to_rfc3339(),
            updated_at: t.updated_at.to_rfc3339(),
        }
    }
}

/// Submit a new model synchronization task.
///
/// Returns `202 Accepted` with the task ID. If a task for the same model is
/// already pending or running, the existing task ID is returned instead.
pub async fn post_sync(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SyncRequest>,
) -> impl IntoResponse {
    for entry in state.tasks.iter() {
        let task = entry.value();
        if task.model_id == req.model_id
            && (task.status == TaskStatus::Pending || task.status == TaskStatus::Running)
        {
            return (
                StatusCode::ACCEPTED,
                Json(SyncResponse {
                    task_id: task.task_id.clone(),
                }),
            );
        }
    }

    let task_id = crate::tasks::spawn_sync_task(req.model_id, state).await;
    (StatusCode::ACCEPTED, Json(SyncResponse { task_id }))
}

/// Query the current state of a synchronization task.
pub async fn get_task(
    State(state): State<Arc<AppState>>,
    Path(task_id): Path<String>,
) -> Result<Json<TaskResponse>, ApiError> {
    match state.tasks.get(&task_id) {
        Some(task) => Ok(Json(TaskResponse::from(task.clone()))),
        None => Err(ApiError::TaskNotFound),
    }
}

/// Subscribe to Server-Sent Events for a specific task.
///
/// The stream emits JSON-encoded [`TaskResponse`] objects whenever the task
/// state changes.
pub async fn get_task_events(
    State(state): State<Arc<AppState>>,
    Path(task_id): Path<String>,
) -> Sse<
    impl tokio_stream::Stream<Item = Result<axum::response::sse::Event, broadcast::error::RecvError>>,
> {
    let rx = state.broadcast.subscribe();
    let filtered = BroadcastStream::new(rx).filter_map(move |result| match result {
        Ok(task) if task.task_id == task_id => {
            let event = axum::response::sse::Event::default()
                .json_data(TaskResponse::from(task))
                .unwrap();
            Some(Ok::<_, broadcast::error::RecvError>(event))
        }
        _ => None,
    });
    Sse::new(filtered)
}

/// Kubernetes liveness probe.
///
/// Returns `200 OK` as long as the HTTP server is running.
pub async fn health() -> impl IntoResponse {
    Json(serde_json::json!({"status": "ok"}))
}

/// Kubernetes readiness probe.
///
/// Returns `200 OK` if both `cache_dir` and `target_dir` exist and are writable,
/// otherwise `503 SERVICE_UNAVAILABLE`.
pub async fn ready(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let cache_ok = is_dir_writable(&state.config.cache_dir).await;
    let target_ok = is_dir_writable(&state.config.target_dir).await;
    if cache_ok && target_ok {
        (StatusCode::OK, Json(serde_json::json!({"status": "ok"})))
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"status": "not ready"})),
        )
    }
}

/// Verify that a directory exists and is writable by creating and removing a temporary file.
async fn is_dir_writable(path: &std::path::Path) -> bool {
    match tokio::fs::metadata(path).await {
        Ok(meta) if meta.is_dir() => {
            let test_file = path.join(".ready_test");
            tokio::fs::write(&test_file, b"").await.is_ok()
                && tokio::fs::remove_file(&test_file).await.is_ok()
        }
        _ => false,
    }
}
