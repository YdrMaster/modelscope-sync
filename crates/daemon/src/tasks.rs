use crate::state::{AppState, TaskState, TaskStatus};
use modelscope_sync_core::sync;
use std::sync::Arc;
use tokio::sync::mpsc;

/// 创建新的同步任务并在后台执行。
///
/// 立即返回 `task_id`，以便调用者轮询状态。
/// 如果同一 `model_id` 的任务已在运行，则返回已有任务 ID（去重）。
///
/// # Arguments
///
/// - `model_id`: 要同步的模型标识符。
/// - `state`: 共享应用状态。
pub async fn spawn_sync_task(model_id: String, state: Arc<AppState>) -> String {
    let task_id = uuid::Uuid::new_v4().to_string();
    let now = chrono::Utc::now();

    let task = TaskState {
        task_id: task_id.clone(),
        model_id: model_id.clone(),
        status: TaskStatus::Pending,
        total_files: 0,
        completed_files: 0,
        downloaded_bytes: 0,
        total_bytes: 0,
        cached_files: 0,
        error: None,
        created_at: now,
        updated_at: now,
    };

    state.tasks.insert(task_id.clone(), task.clone());
    let _ = state.broadcast.send(task);
    state
        .active_tasks
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    let task_id_clone = task_id.clone();
    let state_clone = state.clone();

    tokio::spawn(async move {
        run_sync(model_id, task_id_clone, state_clone).await;
    });

    task_id
}

/// 执行实际的模型同步逻辑并更新任务状态。
async fn run_sync(model_id: String, task_id: String, state: Arc<AppState>) {
    let start = std::time::Instant::now();
    let mut task = state.tasks.get(&task_id).unwrap().clone();
    task.status = TaskStatus::Running;
    task.updated_at = chrono::Utc::now();
    state.tasks.insert(task_id.clone(), task.clone());
    let _ = state.broadcast.send(task);

    let (tx, mut rx) = mpsc::channel::<(String, u64, u64)>(128);

    let progress_handle = {
        let state = state.clone();
        let task_id = task_id.clone();
        tokio::spawn(async move {
            while let Some((_path, delta, total)) = rx.recv().await {
                if let Some(mut t) = state.tasks.get_mut(&task_id) {
                    t.downloaded_bytes += delta;
                    if total > t.total_bytes {
                        t.total_bytes = total;
                    }
                    t.updated_at = chrono::Utc::now();
                    let updated = t.clone();
                    drop(t);
                    let _ = state.broadcast.send(updated);
                }
            }
        })
    };

    let result = sync::sync_model(
        &state.reqwest_client,
        &state.config.api_base,
        &model_id,
        &state.config.cache_dir,
        &state.config.target_dir,
        state.config.max_concurrent_downloads,
        tx,
    )
    .await;

    progress_handle.await.ok();

    let mut task = state.tasks.get(&task_id).unwrap().clone();
    task.updated_at = chrono::Utc::now();

    match &result {
        Ok(report) => {
            if report.failed_files > 0 {
                task.status = TaskStatus::Failed;
                task.error = Some(format!(
                    "{} of {} files failed to synchronize",
                    report.failed_files, report.total_files
                ));
            } else {
                task.status = TaskStatus::Success;
                task.error = None;
            }
            task.total_files = report.total_files;
            task.completed_files = report.cached_files + report.downloaded_files;
            task.cached_files = report.cached_files;
            metrics::counter!("modelscope_sync_files_total", "status" => "success")
                .increment((report.cached_files + report.downloaded_files) as u64);
            metrics::counter!("modelscope_sync_files_total", "status" => "failed")
                .increment(report.failed_files as u64);
        }
        Err(e) => {
            task.status = TaskStatus::Failed;
            task.error = Some(e.to_string());
        }
    }

    let status_label = format!("{:?}", task.status).to_lowercase();
    metrics::counter!("modelscope_sync_tasks_total", "status" => status_label).increment(1);

    let duration = start.elapsed().as_secs_f64();
    metrics::histogram!("modelscope_sync_task_duration_seconds").record(duration);

    state
        .active_tasks
        .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    state.tasks.insert(task_id, task.clone());
    let _ = state.broadcast.send(task);
}
