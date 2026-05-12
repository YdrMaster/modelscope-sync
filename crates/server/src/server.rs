use crate::handlers;
use crate::state::AppState;
use axum::{
    Router,
    routing::{get, post},
};
use metrics_exporter_prometheus::PrometheusHandle;
use std::sync::Arc;

/// 启动 HTTP 服务器并在收到关闭信号前保持运行。
///
/// 注册所有 REST 路由以及 Prometheus `/metrics` 端点。
/// 在 `Ctrl+C`（所有平台）或 `SIGTERM`（Unix）时优雅关闭。
///
/// # Arguments
///
/// - `state`: 共享应用状态。
/// - `port`: 监听的 TCP 端口号。
/// - `prometheus`: Prometheus 指标导出器句柄。
pub async fn run(state: Arc<AppState>, port: u16, prometheus: PrometheusHandle) {
    let bind_addr = format!("0.0.0.0:{}", port);
    let app = Router::new()
        .route("/sync", post(handlers::post_sync))
        .route("/tasks/{task_id}", get(handlers::get_task))
        .route("/tasks/{task_id}/events", get(handlers::get_task_events))
        .route("/health", get(handlers::health))
        .route("/ready", get(handlers::ready))
        .route("/metrics", get(move || async move { prometheus.render() }))
        .with_state(state.clone());

    let listener = tokio::net::TcpListener::bind(&bind_addr).await.unwrap();
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            state.shutdown.notified().await;
        })
        .await
        .unwrap();
}
