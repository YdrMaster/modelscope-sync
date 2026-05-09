use crate::handlers;
use crate::state::AppState;
use axum::{
    routing::{get, post},
    Router,
};
use metrics_exporter_prometheus::PrometheusHandle;
use std::sync::Arc;

/// Start the HTTP server and block until a shutdown signal is received.
///
/// Registers all REST routes and the Prometheus `/metrics` endpoint.
/// Gracefully shuts down on `Ctrl+C` (all platforms) or `SIGTERM` (Unix).
///
/// # Arguments
/// * `state` — Shared application state.
/// * `bind_addr` — TCP address to listen on (e.g. `"0.0.0.0:8080"`).
/// * `prometheus` — Prometheus metrics exporter handle.
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

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    tracing::info!("shutdown signal received, starting graceful shutdown");
}
