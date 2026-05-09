mod handlers;
mod server;
mod state;
mod tasks;

use clap::Parser;
use state::{AppState, Config};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "modelscope-sync-daemon")]
struct Args {
    #[arg(long, env = "CACHE_DIR", default_value = "/var/cache/modelscope")]
    cache_dir: PathBuf,

    #[arg(long, env = "TARGET_DIR", default_value = "/mnt/models")]
    target_dir: PathBuf,

    #[arg(long, env = "MAX_CONCURRENT", default_value = "3")]
    max_concurrent_downloads: usize,

    #[arg(long, env = "API_BASE", default_value = "https://www.modelscope.cn")]
    api_base: String,

    #[arg(long, env = "BIND_ADDR", default_value = "0.0.0.0:8080")]
    bind_addr: String,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    tracing_subscriber::fmt::init();
    tracing::info!("daemon starting with args: {:?}", args);

    let config = Config {
        cache_dir: args.cache_dir,
        target_dir: args.target_dir,
        max_concurrent_downloads: args.max_concurrent_downloads,
        api_base: args.api_base,
        bind_addr: args.bind_addr.clone(),
    };

    let state = AppState::new(config);
    server::run(state, &args.bind_addr).await;
}
