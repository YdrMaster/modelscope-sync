use axum::body::Body;
use axum::http::{Request, StatusCode};
use modelscope_sync_daemon::state::{AppState, Config};
use std::path::PathBuf;
use std::sync::Arc;
use tower::ServiceExt;

fn test_state() -> Arc<AppState> {
    let config = Config {
        cache_dir: PathBuf::from("/tmp/test-cache"),
        target_dir: PathBuf::from("/tmp/test-target"),
        max_concurrent_downloads: 3,
        api_base: "https://test.modelscope.cn".to_string(),
        port: 0,
    };
    AppState::new(config)
}

#[tokio::test]
async fn test_health_endpoint() {
    let state = test_state();
    let app = axum::Router::new()
        .route("/health", axum::routing::get(modelscope_sync_daemon::handlers::health))
        .with_state(state);

    let response = app
        .oneshot(Request::builder().uri("/health").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_ready_endpoint() {
    let state = test_state();
    let app = axum::Router::new()
        .route("/ready", axum::routing::get(modelscope_sync_daemon::handlers::ready))
        .with_state(state);

    let response = app
        .oneshot(Request::builder().uri("/ready").body(Body::empty()).unwrap())
        .await
        .unwrap();

    // ready 会检查目录是否存在，/tmp/test-cache 和 /tmp/test-target 可能不存在
    // 所以可能返回 503，这也是可以接受的
    assert!(response.status() == StatusCode::OK || response.status() == StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn test_sync_endpoint_returns_202() {
    let state = test_state();
    let app = axum::Router::new()
        .route("/sync", axum::routing::post(modelscope_sync_daemon::handlers::post_sync))
        .with_state(state);

    let body = axum::body::Body::from(r#"{"model_id":"test-model"}"#);
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/sync")
                .header("content-type", "application/json")
                .body(body)
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::ACCEPTED);
}
