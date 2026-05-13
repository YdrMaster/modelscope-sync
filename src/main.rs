//! ModelScope 同步应用程序的可执行入口。
//!
//! 解析命令行参数、初始化日志与指标，根据子命令启动 HTTP 服务器或直接同步模型。

#![deny(missing_docs)]

mod serve;
mod sync;

use serve::ServeArgs;
use sync::SyncArgs;

use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(env_filter).init();
    tracing::info!("starting with args: {args:?}");

    match args.command {
        Commands::Serve(args) => args.run().await,
        Commands::Sync(args) => args.run().await,
    }
}

/// 命令行参数。
#[derive(Parser, Debug)]
#[command(name = "modelscope-sync")]
struct Args {
    #[command(subcommand)]
    pub command: Commands,
}

/// 支持的子命令。
#[derive(Subcommand, Debug)]
enum Commands {
    /// 启动 HTTP 服务。
    Serve(ServeArgs),
    /// 直接同步指定模型并退出。
    Sync(SyncArgs),
}

/// 各子命令共享的公共参数。
#[derive(clap::Args, Debug)]
struct CommonArgs {
    /// 本地缓存目录。
    #[arg(long, short('c'), default_value = "cache")]
    pub cache_dir: PathBuf,

    /// 模型文件的目标目录。
    #[arg(long, short('t'), default_value = "models")]
    pub target_dir: PathBuf,

    /// 最大并发下载数。
    #[arg(long, short('j'), default_value = "3")]
    pub max_concurrent_downloads: usize,
}
