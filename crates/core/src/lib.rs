//! 与 ModelScope 模型仓库同步相关的核心逻辑。
//!
//! 提供文件元数据获取、本地缓存管理、流式下载、SHA-256 校验以及完整的模型同步流程。

#![deny(missing_docs)]

use serde::Deserialize;

/// 模型仓库中单个文件的元数据。
#[derive(Debug, Clone, Deserialize)]
pub struct FileMeta {
    /// 文件在仓库内的相对路径。
    pub path: String,
    /// 文件内容的预期 SHA-256 哈希值。
    pub sha256: String,
    /// 文件大小，单位为字节。
    pub size: u64,
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

/// 与 ModelScope API 交互的模块。
pub mod api;
/// 本地缓存路径解析相关的模块。
pub mod cache;
/// 流式下载相关的模块。
pub mod download;
/// SHA-256 哈希计算相关的模块。
pub mod hash;
/// 模型同步主流程相关的模块。
pub mod sync;
