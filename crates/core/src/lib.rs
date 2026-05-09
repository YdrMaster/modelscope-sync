use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct FileMeta {
    pub path: String,
    pub sha256: String,
    pub size: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum CoreError {
    #[error("api request failed: {0}")]
    ApiRequest(#[from] reqwest::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("hash mismatch")]
    HashMismatch,
}

pub type Result<T> = std::result::Result<T, CoreError>;

pub mod api;
pub mod cache;
pub mod download;
pub mod hash;
pub mod sync;
