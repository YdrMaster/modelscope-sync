use crate::handlers;
use crate::state::AppState;
use axum::{
    routing::{get, post},
    Router,
};
use metrics_exporter_prometheus::PrometheusHandle;
use std::sync::Arc;

pub async fn run(state: Arc<AppState>, bind_addr: &str, prometheus: PrometheusHandle) {
    let app = Router::new()
        .route("/sync", post(handlers::post_sync))
        .route("/tasks/{task_id}", get(handlers::get_task))
        .route("/tasks/{task_id}/events", get(handlers::get_task_events))
        .route("/health", get(handlers::health))
        .route("/ready", get(handlers::ready))
        .route("/metrics", get(move || async move { prometheus.render() }))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(bind_addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
