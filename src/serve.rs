use crate::CommonArgs;
use modelscope_sync_server::state::{AppState, Config};
use tracing::info;

#[derive(clap::Args, Debug)]
pub struct ServeArgs {
    #[command(flatten)]
    common: CommonArgs,
    /// HTTP 服务监听端口。
    #[arg(long, short('p'), default_value = "8080")]
    port: u16,
}

impl ServeArgs {
    /// 启动 HTTP 服务。
    pub async fn run(self) {
        let Self { common, port } = self;
        let config = Config {
            cache_dir: common.cache_dir,
            target_dir: common.target_dir,
            max_concurrent_downloads: common.max_concurrent_downloads,
        };

        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let prometheus = recorder.handle();
        metrics::set_global_recorder(recorder).unwrap();

        let state = AppState::new(config);

        let server_handle = {
            let state = state.clone();
            tokio::spawn(async move {
                modelscope_sync_server::server::run(state, port, prometheus).await;
            })
        };

        let shutdown = {
            let state = state.clone();
            async move {
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

                info!("shutdown signal received, starting graceful shutdown");
                state.shutdown.notify_waiters()
            }
        };

        tokio::select! {
            _ = server_handle => {},
            _ = shutdown => {},
        }

        // 等待活跃的后台任务完成（最多 30 秒）。
        let timeout = std::time::Duration::from_secs(30);
        let start = std::time::Instant::now();
        while state
            .active_tasks
            .load(std::sync::atomic::Ordering::Relaxed)
            > 0
        {
            if start.elapsed() > timeout {
                tracing::warn!(
                    "shutdown timed out, {} tasks still active",
                    state
                        .active_tasks
                        .load(std::sync::atomic::Ordering::Relaxed)
                );
                break;
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await
        }

        info!("shutdown complete")
    }
}
