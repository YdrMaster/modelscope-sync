use dashmap::DashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::broadcast;

#[derive(Debug, Clone)]
pub struct TaskState {
    pub task_id: String,
    pub model_id: String,
    pub status: TaskStatus,
    pub total_files: usize,
    pub completed_files: usize,
    pub downloaded_bytes: u64,
    pub total_bytes: u64,
    pub cached_files: usize,
    pub error: Option<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TaskStatus {
    Pending,
    Running,
    Success,
    Failed,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub cache_dir: PathBuf,
    pub target_dir: PathBuf,
    pub max_concurrent_downloads: usize,
    pub api_base: String,
    pub bind_addr: String,
}

pub struct AppState {
    pub tasks: DashMap<String, TaskState>,
    pub config: Config,
    pub broadcast: broadcast::Sender<TaskState>,
    pub reqwest_client: reqwest::Client,
}

impl AppState {
    pub fn new(config: Config) -> Arc<Self> {
        let (broadcast, _) = broadcast::channel(128);
        Arc::new(Self {
            tasks: DashMap::new(),
            config,
            broadcast,
            reqwest_client: reqwest::Client::new(),
        })
    }
}
