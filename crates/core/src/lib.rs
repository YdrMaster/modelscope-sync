//! 与 ModelScope 模型仓库同步相关的核心逻辑。
//!
//! 提供文件元数据获取、流式下载、SHA-256 校验以及完整的模型同步流程。

#![deny(missing_docs)]

mod api;
mod download;
mod hash;
/// 缓存目录扫描与整理相关的模块。
pub mod scan;
/// 模型同步主流程相关的模块。
pub mod sync;

#[macro_use]
extern crate tracing;

use serde::Deserialize;
use std::path::Path;

/// 模型仓库中单个文件的元数据。
#[derive(Debug, Clone, Deserialize)]
pub struct FileMeta {
    /// 文件在仓库内的相对路径。
    path: String,
    /// 文件内容的预期 SHA-256 哈希值。
    sha256: String,
    /// 文件大小，单位为字节。
    size: u64,
}

/// 核心同步逻辑中可能发生的错误。
#[derive(Debug, thiserror::Error)]
pub enum CoreError {
    /// 对 ModelScope API 的 HTTP 请求失败。
    #[error("api request failed: {0}")]
    ApiRequest(#[from] reqwest::Error),
    /// I/O 操作失败。
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    /// 下载文件的 SHA-256 哈希值与预期不符。
    #[error("hash mismatch")]
    HashMismatch,
    /// ModelScope API 返回了错误响应。
    #[error("api response error: {message}")]
    ApiResponseError {
        /// API 返回的错误信息。
        message: String,
    },
}

/// core crate 中常用的结果类型别名。
pub type Result<T> = std::result::Result<T, CoreError>;

/// 同步模型仓库后生成的汇总报告。
#[derive(Debug, Clone)]
pub struct SyncReport {
    /// 仓库中的文件总数。
    pub total_files: usize,
    /// 已存在且校验通过的文件数（跳过下载）。
    pub cached_files: usize,
    /// 需要下载的文件数。
    pub downloaded_files: usize,
    /// 同步失败的文件数。
    pub failed_files: usize,
}

/// 验证本地文件是否存在且其 SHA-256 哈希值与预期一致。
///
/// 若文件不存在，直接返回 `false`；若存在则计算 SHA-256 并与预期值比较。
async fn verify_file(path: &Path, expected: &str) -> Result<bool> {
    if !path.exists() {
        return Ok(false);
    }
    let file = tokio::fs::File::open(path).await?;
    let hash = hash::sha256_stream(file).await?;
    Ok(hash == expected)
}

/// 确保目标文件的父目录存在，然后将源文件原子移动到目标路径。
async fn atomic_move(src: &Path, dst: &Path) -> Result<()> {
    if let Some(parent) = dst.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    tokio::fs::rename(src, dst).await?;
    Ok(())
}

/// 本地文件验证与整理后的状态。
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum FileStatus {
    /// 文件已在目标目录中且验证通过。
    Verified,
    /// 文件从缓存目录验证通过后移动到目标目录。
    Moved,
    /// 文件验证失败后被删除。
    Deleted,
    /// 文件在本地不存在。
    Missing,
}

/// 验证并整理本地已有的模型文件。
///
/// 按以下顺序处理：
/// 1. 若目标目录中文件存在且 hash 正确，保留并清理缓存中的旧副本。
/// 2. 若缓存目录中文件存在且 hash 正确，原子移动到目标目录。
/// 3. 若 hash 错误且 `delete_corrupt` 为 `true`，删除损坏文件。
///
/// # Arguments
///
/// - `cache_path`: 文件在缓存目录中的路径。
/// - `target_path`: 文件在目标目录中的路径。
/// - `expected_hash`: 预期的 SHA-256 哈希值。
/// - `delete_corrupt`: 是否删除 hash 验证失败的文件。
///
/// # Returns
///
/// 返回 [`FileStatus`] 表示文件的处理结果。
pub(crate) async fn verify_and_organize(
    cache_path: &Path,
    target_path: &Path,
    expected_hash: &str,
    delete_corrupt: bool,
) -> Result<FileStatus> {
    let mut deleted = false;

    // 1. 检查目标目录（优先级最高）。
    if target_path.exists() {
        if verify_file(target_path, expected_hash).await? {
            if cache_path.exists() {
                let _ = tokio::fs::remove_file(cache_path).await;
            }
            return Ok(FileStatus::Verified);
        } else if delete_corrupt {
            tokio::fs::remove_file(target_path).await?;
            deleted = true;
        }
    }

    // 2. 检查缓存目录。
    if cache_path.exists() {
        if verify_file(cache_path, expected_hash).await? {
            atomic_move(cache_path, target_path).await?;
            return Ok(FileStatus::Moved);
        } else if delete_corrupt {
            tokio::fs::remove_file(cache_path).await?;
            deleted = true;
        }
    }

    if deleted {
        Ok(FileStatus::Deleted)
    } else {
        Ok(FileStatus::Missing)
    }
}
