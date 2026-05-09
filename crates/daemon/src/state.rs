use dashmap::DashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{broadcast, Notify};
use std::sync::atomic::{AtomicUsize, Ordering};

/// Snapshot of a synchronization task's current state.
#[derive(Debug, Clone)]
pub struct TaskState {
    /// Unique identifier for the task (UUID v4).
    pub task_id: String,
    /// The model being synchronized.
    pub model_id: String,
    /// Current lifecycle status of the task.
    pub status: TaskStatus,
    /// Total number of files in the model repository.
    pub total_files: usize,
    /// Number of files that have been processed so far.
    pub completed_files: usize,
    /// Cumulative bytes downloaded across all files.
    pub downloaded_bytes: u64,
    /// Total expected bytes across all files (may be zero until known).
    pub total_bytes: u64,
    /// Number of files that were already present and valid.
    pub cached_files: usize,
    /// Error message if the task failed.
    pub error: Option<String>,
    /// UTC timestamp when the task was created.
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// UTC timestamp of the most recent status update.
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

/// Lifecycle status of a synchronization task.
#[derive(Debug, Clone, PartialEq)]
pub enum TaskStatus {
    /// Task has been accepted but not yet started.
    Pending,
    /// Files are currently being checked or downloaded.
    Running,
    /// All files were processed successfully.
    Success,
    /// One or more files could not be synchronized.
    Failed,
}

/// Daemon configuration, populated from CLI arguments or environment variables.
#[derive(Debug, Clone)]
pub struct Config {
    /// Local directory used as a download staging area.
    pub cache_dir: PathBuf,
    /// Final destination directory for model files.
    pub target_dir: PathBuf,
    /// Maximum number of files to download concurrently.
    pub max_concurrent_downloads: usize,
    /// Base URL of the ModelScope API.
    pub api_base: String,
    /// TCP port to listen on.
    pub port: u16,
}

/// Shared application state accessible from all request handlers and background tasks.
pub struct AppState {
    /// In-memory map of all known tasks keyed by `task_id`.
    pub tasks: DashMap<String, TaskState>,
    /// Static configuration for this daemon instance.
    pub config: Config,
    /// Broadcast channel for pushing real-time task updates to SSE subscribers.
    pub broadcast: broadcast::Sender<TaskState>,
    /// Shared HTTP client for API calls and downloads.
    pub reqwest_client: reqwest::Client,
    /// Number of currently active synchronization tasks.
    pub active_tasks: Arc<AtomicUsize>,
    /// Notified when a shutdown signal is received.
    pub shutdown: Notify,
}

impl AppState {
    /// Create a new `AppState` wrapped in an `Arc` for cheap cloning across tasks.
    pub fn new(config: Config) -> Arc<Self> {
        let (broadcast, _) = broadcast::channel(128);
        Arc::new(Self {
            tasks: DashMap::new(),
            config,
            broadcast,
            reqwest_client: reqwest::Client::new(),
            active_tasks: Arc::new(AtomicUsize::new(0)),
            shutdown: Notify::new(),
        })
    }
}
