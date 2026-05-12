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

/// `POST /sync` 的请求体。
#[derive(Deserialize)]
pub struct SyncRequest {
    /// 要同步的模型标识符（例如 `Qwen/Qwen-7B-Chat`）。
    pub model_id: String,
}

/// `POST /sync` 的响应体。
#[derive(Serialize)]
pub struct SyncResponse {
    /// 分配给本次同步请求的唯一任务 ID。
    pub task_id: String,
}

/// `GET /tasks/{task_id}` 的响应体。
#[derive(Serialize)]
pub struct TaskResponse {
    /// 任务的唯一标识符。
    pub task_id: String,
    /// 当前状态：`pending`、`running`、`success` 或 `failed`。
    pub status: String,
    /// 正在同步的模型标识符。
    pub model_id: String,
    /// 仓库中的文件总数。
    pub total_files: usize,
    /// 到目前为止已处理的文件数。
    pub completed_files: usize,
    /// 累计已下载的字节数。
    pub downloaded_bytes: u64,
    /// 预期总字节数（在获知前可能为零）。
    pub total_bytes: u64,
    /// 已存在且校验通过的文件数。
    pub cached_files: usize,
    /// 任务失败时的错误信息。
    pub error: Option<String>,
    /// 任务创建时的 ISO-8601 时间戳。
    pub created_at: String,
    /// 最近更新时的 ISO-8601 时间戳。
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

/// 提交新的模型同步任务。
///
/// 返回 `202 Accepted` 及任务 ID。如果同一模型的任务已处于 pending 或 running 状态，
/// 则返回已有任务 ID。
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

/// 查询同步任务的当前状态。
pub async fn get_task(
    State(state): State<Arc<AppState>>,
    Path(task_id): Path<String>,
) -> Result<Json<TaskResponse>, ApiError> {
    match state.tasks.get(&task_id) {
        Some(task) => Ok(Json(TaskResponse::from(task.clone()))),
        None => Err(ApiError::TaskNotFound),
    }
}

/// 订阅指定任务的 Server-Sent Events。
///
/// 当任务状态发生变化时，流会发送 JSON 编码的 [`TaskResponse`] 对象。
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

/// Kubernetes 存活探针。
///
/// 只要 HTTP 服务器正在运行就返回 `200 OK`。
pub async fn health() -> impl IntoResponse {
    Json(serde_json::json!({"status": "ok"}))
}

/// Kubernetes 就绪探针。
///
/// 如果 `cache_dir` 和 `target_dir` 均存在且可写则返回 `200 OK`，
/// 否则返回 `503 SERVICE_UNAVAILABLE`。
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

/// 验证目录是否存在且可写，通过创建并删除一个临时文件来测试。
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
