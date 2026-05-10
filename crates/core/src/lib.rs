use serde::Deserialize;

/// Metadata for a single file in a model repository.
#[derive(Debug, Clone, Deserialize)]
pub struct FileMeta {
    /// Relative path of the file within the repository.
    pub path: String,
    /// Expected SHA-256 hash of the file content.
    pub sha256: String,
    /// File size in bytes.
    pub size: u64,
}

/// Errors that can occur in the core synchronization logic.
#[derive(Debug, thiserror::Error)]
pub enum CoreError {
    /// An HTTP request to the ModelScope API failed.
    #[error("api request failed: {0}")]
    ApiRequest(#[from] reqwest::Error),
    /// An I/O operation failed.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    /// The downloaded file's SHA-256 hash does not match the expected value.
    #[error("hash mismatch")]
    HashMismatch,
    /// The ModelScope API returned an error response.
    #[error("api response error: {message}")]
    ApiResponseError { message: String },
}

/// Convenient type alias for results in the core crate.
pub type Result<T> = std::result::Result<T, CoreError>;

/// Summary report produced after synchronizing a model repository.
#[derive(Debug, Clone)]
pub struct SyncReport {
    /// Total number of files in the repository.
    pub total_files: usize,
    /// Number of files that were already present and valid (skipped download).
    pub cached_files: usize,
    /// Number of files that had to be downloaded.
    pub downloaded_files: usize,
    /// Number of files that failed to synchronize.
    pub failed_files: usize,
}

pub mod api;
pub mod cache;
pub mod download;
pub mod hash;
pub mod sync;
