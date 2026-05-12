use dashmap::DashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use tokio::sync::{Notify, broadcast};

/// 同步任务当前状态的快照。
#[derive(Debug, Clone)]
pub struct TaskState {
    /// 任务唯一标识符（UUID v4）。
    pub task_id: String,
    /// 正在同步的模型标识符。
    pub model_id: String,
    /// 任务当前的生命周期状态。
    pub status: TaskStatus,
    /// 模型仓库中的文件总数。
    pub total_files: usize,
    /// 到目前为止已处理的文件数。
    pub completed_files: usize,
    /// 所有文件累计已下载的字节数。
    pub downloaded_bytes: u64,
    /// 所有文件预期总字节数（在获知前可能为零）。
    pub total_bytes: u64,
    /// 已存在且校验通过的文件数。
    pub cached_files: usize,
    /// 任务失败时的错误信息。
    pub error: Option<String>,
    /// 任务创建时的 UTC 时间戳。
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// 最近状态更新时的 UTC 时间戳。
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

/// 同步任务的生命周期状态。
#[derive(Debug, Clone, PartialEq)]
pub enum TaskStatus {
    /// 任务已接受但尚未开始执行。
    Pending,
    /// 文件正在检查或下载中。
    Running,
    /// 所有文件处理成功。
    Success,
    /// 一个或多个文件同步失败。
    Failed,
}

/// 守护进程配置，从 CLI 参数或环境变量填充。
#[derive(Debug, Clone)]
pub struct Config {
    /// 用作下载暂存区的本地目录。
    pub cache_dir: PathBuf,
    /// 模型文件的最终目标目录。
    pub target_dir: PathBuf,
    /// 最大并发下载文件数。
    pub max_concurrent_downloads: usize,
}

/// 所有请求处理函数和后台任务均可访问的共享应用状态。
pub struct AppState {
    /// 以 task_id 为键的所有已知任务的内存映射。
    pub tasks: DashMap<String, TaskState>,
    /// 当前守护进程实例的静态配置。
    pub config: Config,
    /// 向 SSE 订阅者推送实时任务更新的广播通道。
    pub broadcast: broadcast::Sender<TaskState>,
    /// 用于 API 调用和下载的共享 HTTP 客户端。
    pub reqwest_client: reqwest::Client,
    /// 当前活跃的同步任务数量。
    pub active_tasks: Arc<AtomicUsize>,
    /// 收到关闭信号时触发通知。
    pub shutdown: Notify,
}

impl AppState {
    /// 创建一个新的 `AppState` 并包装在 `Arc` 中，以便在任务间低成本克隆。
    pub fn new(config: Config) -> Arc<Self> {
        let (broadcast, _) = broadcast::channel(128);
        Arc::new(Self {
            tasks: DashMap::new(),
            config,
            broadcast,
            reqwest_client: reqwest::Client::builder()
                .user_agent("modelscope-sync/0.1.0")
                .build()
                .unwrap(),
            active_tasks: Arc::new(AtomicUsize::new(0)),
            shutdown: Notify::new(),
        })
    }
}
