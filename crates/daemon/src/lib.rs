//! ModelScope 同步守护进程的 HTTP API 与后台任务管理。
//!
//! 提供 REST API、任务调度、状态管理和 Prometheus 指标导出。

#![deny(missing_docs)]

/// HTTP API 错误类型。
pub mod error;
/// 守护进程 REST API 的请求处理函数。
pub mod handlers;
/// Axum 服务器启动与优雅关闭。
pub mod server;
/// 共享应用状态、配置与任务跟踪。
pub mod state;
/// 后台任务创建与进度跟踪。
pub mod tasks;
