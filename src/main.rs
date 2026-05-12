//! ModelScope 同步应用程序的可执行入口。
//!
//! 解析命令行参数、初始化日志与指标，根据子命令启动 HTTP 服务器或直接同步模型。

#![deny(missing_docs)]

use clap::{Parser, Subcommand};
use modelscope_sync_server::state::Config;
use std::path::PathBuf;

mod serve;
mod sync;

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(env_filter).init();
    tracing::info!("starting with args: {:?}", args);

    let config = Config {
        cache_dir: args.cache_dir,
        target_dir: args.target_dir,
        max_concurrent_downloads: args.max_concurrent_downloads,
    };

    match args.command {
        Commands::Serve { port } => serve::run(config, port).await,
        Commands::Sync { model_id } => sync::run(config, model_id).await,
    }
}

/// 命令行参数。
#[derive(Parser, Debug)]
#[command(name = "modelscope-sync")]
struct Args {
    /// 本地缓存目录。
    #[arg(long, short('c'))]
    pub cache_dir: PathBuf,

    /// 模型文件的目标目录。
    #[arg(long, short('t'))]
    pub target_dir: PathBuf,

    /// 最大并发下载数。
    #[arg(long, short('j'), default_value = "3")]
    pub max_concurrent_downloads: usize,

    #[command(subcommand)]
    pub command: Commands,
}

/// 支持的子命令。
#[derive(Subcommand, Debug)]
enum Commands {
    /// 启动 HTTP 服务。
    Serve {
        /// HTTP 服务监听端口。
        #[arg(long, short('p'), default_value = "8080")]
        port: u16,
    },
    /// 直接同步指定模型并退出。
    Sync {
        /// 要同步的模型标识符（例如 `Qwen/Qwen-7B-Chat`）。
        #[arg(long, short('m'))]
        model_id: String,
    },
}
