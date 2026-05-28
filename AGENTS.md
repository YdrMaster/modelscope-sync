# ModelScope Sync — Agent Guide

## Project Overview

ModelScope Sync 是一个用 Rust 编写的常驻模型分发服务（daemon）。
它自动将 [ModelScope](https://www.modelscope.cn) 上的模型仓库同步到本地目标目录，
支持双目录（缓存 + 目标）增量校验、并发下载和 Prometheus 可观测性。

主要特性：

- **仓库级同步**：以整个模型仓库为单位自动同步所有文件。
- **双目录设计**：缓存目录作为下载暂存区，目标目录为最终存储位置；命中缓存时直接原子移动。
- **SHA-256 增量校验**：本地文件哈希匹配时跳过下载，仅下载变更部分。
- **并发下载**：通过信号量限制最大并发文件下载数，避免带宽和磁盘过载。
- **异步任务模式**：HTTP API 提交任务后后台异步执行，支持 SSE 实时进度推送。
- **云原生可观测性**：内置 `/health`、 `/ready` 探针和 Prometheus `/metrics` 端点。
- **优雅关闭**：监听 SIGTERM（Unix）和 Ctrl+C（全平台），完成当前请求后安全退出。

## Technology Stack

- **Language**: Rust (Edition 2024)
- **Build Tool**: Cargo (Workspace, Resolver 3)
- **Async Runtime**: Tokio
- **Web Framework**: Axum 0.8
- **HTTP Client**: reqwest 0.12
- **Metrics**: metrics + metrics-exporter-prometheus
- **CLI Parsing**: clap 4 (derive 特性)
- **Hashing**: sha2 (SHA-256)
- **Concurrency Primitives**: tokio::sync::Semaphore, DashMap, tokio::sync::broadcast

## Workspace Structure

```
.
├── Cargo.toml              # Workspace 根配置，成员：crates/core、crates/daemon
├── crates/
│   ├── core/               # 核心同步逻辑库（modelscope-sync-core）
│   │   └── src/
│   │       ├── lib.rs      # 公共类型：FileMeta、CoreError、SyncReport
│   │       ├── api.rs      # ModelScope API 调用（获取文件元数据）
│   │       ├── sync.rs     # 主同步逻辑：校验、缓存命中、下载、原子移动
│   │       ├── download.rs # HTTP 流式下载与进度上报
│   │       ├── cache.rs    # 本地路径解析（base_dir/model_id/file_path）
│   │       └── hash.rs     # 异步 SHA-256 流式计算
│   └── daemon/             # HTTP 服务二进制（modelscope-sync-daemon）
│       └── src/
│           ├── main.rs     # 入口：解析 CLI、初始化 Prometheus、启动 Server、信号处理
│           ├── lib.rs      # 模块导出
│           ├── server.rs   # Axum Router 组装与 graceful shutdown
│           ├── handlers.rs # REST API 处理器（sync、task、events、health、ready、metrics）
│           ├── state.rs    # AppState、Config、TaskState、TaskStatus
│           ├── tasks.rs    # 后台任务创建、进度聚合、任务状态广播
│           └── error.rs    # ApiError 及其 IntoResponse 实现
│       └── tests/
│           └── integration_test.rs  # 集成测试（health / ready / sync 端点）
├── docs/
│   ├── design.md           # 技术选型与设计分析（中文）
│   └── superpowers/        # 计划、评审、规格文档
├── markdownlint/           # Git 子模块：DavidAnson/markdownlint
├── markdown-rules.md       # Markdown 写作规范
└── .markdownlint.json      # markdownlint 配置（MD013 禁用）
```

## Build and Run

### 构建

```bash
# 开发检查
cargo check

# 构建 Release 二进制
cargo build --release -p modelscope-sync-daemon
```

### 运行

```bash
./target/release/modelscope-sync-daemon \
  --cache-dir /var/cache/modelscope \
  --target-dir /mnt/models \
  --max-concurrent 3 \
  --bind-addr 0.0.0.0:8080
```

所有 CLI 参数均支持通过环境变量传入（大写 + 下划线形式），例如：

```bash
CACHE_DIR=/var/cache/modelscope \
TARGET_DIR=/mnt/models \
MAX_CONCURRENT=3 \
BIND_ADDR=0.0.0.0:8080 \
./target/release/modelscope-sync-daemon
```

| CLI 参数 | 环境变量 | 默认值 | 说明 |
|---------|---------|--------|------|
| `--cache-dir` | `CACHE_DIR` | `/var/cache/modelscope` | 下载缓存目录 |
| `--target-dir` | `TARGET_DIR` | `/mnt/models` | 目标存储目录 |
| `--max-concurrent` | `MAX_CONCURRENT` | `3` | 最大并发下载文件数 |
| `--api-base` | `API_BASE` | `https://www.modelscope.cn` | ModelScope API 地址 |
| `--port` | `PORT` | `8080` | HTTP 服务监听端口 |

## Testing

### 运行所有测试

```bash
cargo test --workspace
```

### 测试策略

- **单元测试**：分散在各模块的 `#[cfg(test)]` 块中，使用 `wiremock` 模拟 HTTP 服务端、`tempfile` 创建临时目录、`tokio-test` 驱动异步测试。
  - `api.rs`：测试 ModelScope API 成功/失败场景。
  - `sync.rs`：测试目标命中、缓存命中、下载后哈希失败、API 404 等场景。
  - `download.rs`：测试流式下载与进度上报。
  - `cache.rs`：测试路径解析规则。
  - `hash.rs`：测试已知内容 SHA-256 与空内容。
- **集成测试**：`crates/daemon/tests/integration_test.rs` 使用 `tower::ServiceExt::oneshot` 直接对 Axum Router 做端点测试，覆盖 `health`、`ready`、`sync` 三个端点。

## Code Style Guidelines

- **语言**：源码中的文档注释、README、docs 目录均使用**中文**。修改或新增注释时应保持一致。
- **模块组织**：每个 crate 的 `lib.rs` 导出公共类型和子模块；业务逻辑放在对应模块中；错误类型使用 `thiserror` 定义。
- **错误处理**：
  - `core` crate 使用 `CoreError`（`ApiRequest`、`Io`、`HashMismatch`），配合 `type Result<T>`。
  - `daemon` crate 使用 `ApiError`（`TaskNotFound`、`Internal`），并实现 `IntoResponse` 以统一返回 JSON 错误体。
- **异步模式**：大量使用 `tokio::spawn` + 通道（`mpsc` / `broadcast`）进行后台任务与进度推送；使用 `tokio::sync::Semaphore` 控制并发。
- **路径与文件操作**：所有文件路径操作使用 `tokio::fs` 异步 API；临时文件下载完成后通过 `fs::rename` 原子移动到目标目录。
- **指标命名**：Prometheus 指标统一使用 `snake_case`，前缀为 `modelscope_sync_*`，例如 `modelscope_sync_tasks_total`、`modelscope_sync_files_total`。

## HTTP API

| 方法 | 路径 | 说明 |
|------|------|------|
| `POST` | `/sync` | 提交同步任务，返回 `202 Accepted` + `task_id`；同一 `model_id` 若已有运行中任务则返回现有 `task_id` |
| `GET` | `/tasks/{task_id}` | 查询任务当前状态 |
| `GET` | `/tasks/{task_id}/events` | SSE 实时进度流，推送 JSON 格式的任务状态变更 |
| `GET` | `/health` | Liveness 探针，始终返回 `200 {"status":"ok"}` |
| `GET` | `/ready` | Readiness 探针，检查缓存目录和目标目录可写，否则返回 `503` |
| `GET` | `/metrics` | Prometheus 指标端点 |

## Deployment Considerations

- **目录权限**：启动前必须确保 `cache_dir` 和 `target_dir` 存在且对进程可写；`ready` 探针会实际创建临时文件验证写入权限。
- **优雅关闭**：收到 SIGTERM 或 Ctrl+C 后，daemon 会：
  1. 触发 `Notify::notify_waiters()` 通知 HTTP server 停止接受新连接；
  2. 等待最多 30 秒让活跃后台任务完成；
  3. 超时后强制退出并记录警告日志。
- **并发控制**：`max_concurrent` 仅限制**下载**阶段的并发文件数；本地哈希校验和文件移动不在信号量限制范围内。
- **去重机制**：同一 `model_id` 的同步任务在 `pending` 或 `running` 状态时再次提交会直接返回已有 `task_id`，不会创建新任务。

## Markdown Linting

项目使用 `markdownlint-cli2` 对 Markdown 文件做规范检查。

```bash
markdownlint-cli2 <markdown-file-name>
```

- 配置在 `.markdownlint.json`，当前仅禁用 `MD013`（行长度限制）。
- 详细规则定义见 `markdown-rules.md`。
- `markdownlint/` 为 Git 子模块（`git@github.com:DavidAnson/markdownlint.git`）。
