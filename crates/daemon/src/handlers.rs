use crate::state::{AppState, TaskState, TaskStatus};
use axum::{
    extract::{Path, State},
    response::{IntoResponse, Sse},
    Json,
};
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;

#[derive(Deserialize)]
pub struct SyncRequest {
    pub model_id: String,
}

#[derive(Serialize)]
pub struct SyncResponse {
    pub task_id: String,
}

#[derive(Serialize)]
pub struct TaskResponse {
    pub task_id: String,
    pub status: String,
    pub model_id: String,
    pub total_files: usize,
    pub completed_files: usize,
    pub downloaded_bytes: u64,
    pub total_bytes: u64,
    pub cached_files: usize,
    pub error: Option<String>,
    pub created_at: String,
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

pub async fn post_sync(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SyncRequest>,
) -> impl IntoResponse {
    for entry in state.tasks.iter() {
        let task = entry.value();
        if task.model_id == req.model_id && (task.status == TaskStatus::Pending || task.status == TaskStatus::Running) {
            return (StatusCode::ACCEPTED, Json(SyncResponse { task_id: task.task_id.clone() }));
        }
    }

    let task_id = crate::tasks::spawn_sync_task(req.model_id, state).await;
    (StatusCode::ACCEPTED, Json(SyncResponse { task_id }))
}

pub async fn get_task(
    State(state): State<Arc<AppState>>,
    Path(task_id): Path<String>,
) -> impl IntoResponse {
    match state.tasks.get(&task_id) {
        Some(task) => (StatusCode::OK, Json(TaskResponse::from(task.clone()))).into_response(),
        None => (StatusCode::NOT_FOUND, "task not found").into_response(),
    }
}

pub async fn get_task_events(
    State(state): State<Arc<AppState>>,
    Path(task_id): Path<String>,
) -> Sse<impl tokio_stream::Stream<Item = Result<axum::response::sse::Event, broadcast::error::RecvError>>> {
    let rx = state.broadcast.subscribe();
    let filtered = BroadcastStream::new(rx)
        .filter_map(move |result| {
            match result {
                Ok(task) if task.task_id == task_id => {
                    let event = axum::response::sse::Event::default()
                        .json_data(TaskResponse::from(task))
                        .unwrap();
                    Some(Ok::<_, broadcast::error::RecvError>(event))
                }
                _ => None,
            }
        });
    Sse::new(filtered)
}

pub async fn health() -> impl IntoResponse {
    Json(serde_json::json!({"status": "ok"}))
}

pub async fn ready(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let cache_ok = tokio::fs::metadata(&state.config.cache_dir).await.is_ok();
    let target_ok = tokio::fs::metadata(&state.config.target_dir).await.is_ok();
    if cache_ok && target_ok {
        (StatusCode::OK, Json(serde_json::json!({"status": "ok"})))
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({"status": "not ready"})))
    }
}
