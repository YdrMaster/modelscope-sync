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
    modelscope_sync_daemon::server::run(state, args.port, prometheus).await;
}
