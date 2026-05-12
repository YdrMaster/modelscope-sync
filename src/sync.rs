use modelscope_sync_server::state::Config;
use tracing::{error, info};

/// 执行直接同步子命令。
pub async fn run(config: Config, model_id: String) {
    let client = reqwest::Client::builder()
        .user_agent("modelscope-sync/0.1.0")
        .build()
        .unwrap();
    let (progress_tx, mut progress_rx) = tokio::sync::mpsc::channel(128);

    tokio::spawn(async move { while progress_rx.recv().await.is_some() {} });

    let result = modelscope_sync_core::sync::sync_model(
        &client,
        "https://www.modelscope.cn",
        &model_id,
        &config.cache_dir,
        &config.target_dir,
        config.max_concurrent_downloads,
        progress_tx,
    )
    .await;

    match result {
        Ok(report) => {
            info!(
                total = report.total_files,
                cached = report.cached_files,
                downloaded = report.downloaded_files,
                failed = report.failed_files,
                "sync completed"
            );
            if report.failed_files > 0 {
                std::process::exit(1);
            }
        }
        Err(e) => {
            error!(error = %e, "sync failed");
            std::process::exit(1);
        }
    }
}
