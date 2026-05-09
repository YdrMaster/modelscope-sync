use clap::Parser;
use modelscope_sync_daemon::state::{AppState, Config};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "modelscope-sync-daemon")]
struct Args {
    #[arg(long, default_value = "/var/cache/modelscope")]
    cache_dir: PathBuf,

    #[arg(long, default_value = "/mnt/models")]
    target_dir: PathBuf,

    #[arg(long, default_value = "3")]
    max_concurrent_downloads: usize,

    #[arg(long, default_value = "https://www.modelscope.cn")]
    api_base: String,

    #[arg(long, default_value = "8080")]
    port: u16,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    tracing_subscriber::fmt::init();
    tracing::info!("daemon starting with args: {:?}", args);

    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let prometheus = recorder.handle();
    metrics::set_global_recorder(recorder).unwrap();

    let config = Config {
        cache_dir: args.cache_dir,
        target_dir: args.target_dir,
        max_concurrent_downloads: args.max_concurrent_downloads,
        api_base: args.api_base,
        port: args.port,
    };

    let state = AppState::new(config);

    let server_handle = {
        let state = state.clone();
        tokio::spawn(async move {
            modelscope_sync_daemon::server::run(state, args.port, prometheus).await;
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

            tracing::info!("shutdown signal received, starting graceful shutdown");
            state.shutdown.notify_waiters();
        }
    };

    tokio::select! {
        _ = server_handle => {},
        _ = shutdown => {},
    }

    // Wait for active background tasks to complete (up to 30s).
    let timeout = std::time::Duration::from_secs(30);
    let start = std::time::Instant::now();
    while state.active_tasks.load(std::sync::atomic::Ordering::Relaxed) > 0 {
        if start.elapsed() > timeout {
            tracing::warn!(
                "shutdown timed out, {} tasks still active",
                state.active_tasks.load(std::sync::atomic::Ordering::Relaxed)
            );
            break;
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }

    tracing::info!("daemon shutdown complete");
}
