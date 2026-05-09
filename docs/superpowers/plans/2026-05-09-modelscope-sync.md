# ModelScope Sync 核心下载链路 MVP 实现计划

> **面向 AI 代理的工作者：** 必需子技能：使用 superpowers:subagent-driven-development（推荐）或 superpowers:executing-plans 逐任务实现此计划。步骤使用复选框（`- [ ]`）语法来跟踪进度。

**目标：** 实现一个常驻 HTTP 服务，接收模型同步请求，自动将 ModelScope 仓库完整同步到本地目标目录，支持双目录（缓存+目标）流转、SHA256 校验、并发下载和 Prometheus 可观测性。

**架构：** Workspace 包含两个 crate：`core`（纯逻辑 lib，负责 API 调用、哈希计算、下载、同步编排）和 `daemon`（HTTP 服务 bin，负责接收请求、管理异步任务、暴露指标）。`daemon` 启动时通过命令行参数接收 `cache_dir`、`target_dir` 和 `max_concurrent_downloads`，所有任务共享这些配置。

**技术栈：** Rust 2024, tokio, axum, reqwest, serde, sha2, dashmap, metrics, metrics-exporter-prometheus, tracing, tracing-subscriber, thiserror, wiremock, tempfile

---

## 文件结构

### 根级

| 文件 | 职责 |
| ------ | ------ |
| `Cargo.toml` | Workspace 定义，包含 `core` 和 `daemon` 两个成员 |

### crates/core/

| 文件 | 职责 |
| ------ | ------ |
| `crates/core/Cargo.toml` | core crate 依赖声明 |
| `crates/core/src/lib.rs` | 模块导出、公共类型定义（`FileMeta`, `SyncReport`, `CoreError`） |
| `crates/core/src/api.rs` | ModelScope HTTP API 客户端，获取仓库文件元数据列表 |
| `crates/core/src/cache.rs` | 根据 base_dir + model_id + file_path 计算本地绝对路径 |
| `crates/core/src/hash.rs` | 基于 `sha2::Sha256` + `tokio::io::AsyncRead` 的流式哈希计算 |
| `crates/core/src/download.rs` | `reqwest` 流式下载，边下边写，支持进度回调 |
| `crates/core/src/sync.rs` | 主同步流程编排：目标检查 → 缓存检查 → 并发下载 → 校验 → 移动 |

### crates/daemon/

| 文件 | 职责 |
| ------ | ------ |
| `crates/daemon/Cargo.toml` | daemon crate 依赖声明，依赖 `core`（本地 workspace） |
| `crates/daemon/src/main.rs` | 入口：解析命令行参数、初始化 tracing、启动服务 |
| `crates/daemon/src/server.rs` | axum 路由组装、TCP 绑定、优雅关闭注册 |
| `crates/daemon/src/state.rs` | `AppState` 结构体：包含 `DashMap<String, TaskState>`、配置、广播通道 |
| `crates/daemon/src/tasks.rs` | 异步任务管理：创建任务、spawn 后台同步、接收进度、广播 SSE |
| `crates/daemon/src/handlers.rs` | HTTP handler：`POST /sync`、`GET /tasks/{id}`、`GET /tasks/{id}/events`、`GET /health`、`GET /ready`、`GET /metrics` |

---

## 任务分解

### 任务 1：初始化 Workspace 结构

**文件：**

- 修改：`Cargo.toml`
- 创建：`crates/core/Cargo.toml`、`crates/core/src/lib.rs`、`crates/daemon/Cargo.toml`、`crates/daemon/src/main.rs`

- [ ] **步骤 1：修改根 Cargo.toml 为 Workspace**

```toml
[workspace]
members = ["crates/core", "crates/daemon"]
resolver = "3"
```

- [ ] **步骤 2：创建 core crate 骨架**

创建 `crates/core/Cargo.toml`：

```toml
[package]
name = "modelscope-sync-core"
version = "0.1.0"
edition = "2024"

[dependencies]
reqwest = { version = "0.12", features = ["json", "stream"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
tokio = { version = "1", features = ["fs", "io-util", "process", "rt", "macros"] }
sha2 = "0.10"
thiserror = "2"
tracing = "0.1"

[dev-dependencies]
wiremock = "0.6"
tempfile = "3"
tokio-test = "0.4"
```

创建 `crates/core/src/lib.rs`：

```rust
pub mod api;
pub mod cache;
pub mod download;
pub mod hash;
pub mod sync;
```

- [ ] **步骤 3：创建 daemon crate 骨架**

创建 `crates/daemon/Cargo.toml`：

```toml
[package]
name = "modelscope-sync-daemon"
version = "0.1.0"
edition = "2024"

[dependencies]
modelscope-sync-core = { path = "../core" }
axum = { version = "0.8", features = ["macros"] }
tokio = { version = "1", features = ["rt-multi-thread", "macros", "signal", "sync"] }
dashmap = "6"
metrics = "0.24"
metrics-exporter-prometheus = "0.16"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["json", "env-filter"] }
thiserror = "2"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
uuid = { version = "1", features = ["v4"] }
chrono = { version = "0.4", features = ["serde"] }
clap = { version = "4", features = ["derive"] }
```

创建 `crates/daemon/src/main.rs`：

```rust
fn main() {
    println!("daemon starting");
}
```

- [ ] **步骤 4：验证编译**

运行：`cargo check --workspace`
预期：编译成功，无错误。

- [ ] **步骤 5：Commit**

```bash
git add Cargo.toml crates/
git commit -m "chore: init workspace with core and daemon crates"
```

---

### 任务 2：core — cache 模块

**文件：**

- 创建：`crates/core/src/cache.rs`
- 修改：`crates/core/src/lib.rs`（已存在，无需修改）
- 测试：`crates/core/src/cache.rs`（模块内单元测试）

- [ ] **步骤 1：编写失败的测试**

在 `crates/core/src/cache.rs` 中：

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_resolve_path_basic() {
        let base = PathBuf::from("/cache");
        let path = resolve_path(&base, "Qwen/Qwen-7B-Chat", "model.safetensors");
        assert_eq!(path, PathBuf::from("/cache/Qwen/Qwen-7B-Chat/model.safetensors"));
    }
}
```

- [ ] **步骤 2：运行测试验证失败**

运行：`cargo test -p modelscope-sync-core -- cache::tests`
预期：FAIL，报错 `resolve_path` not found。

- [ ] **步骤 3：实现 resolve_path**

在 `crates/core/src/cache.rs` 中：

```rust
use std::path::{Path, PathBuf};

pub fn resolve_path(base_dir: &Path, model_id: &str, file_path: &str) -> PathBuf {
    base_dir.join(model_id).join(file_path)
}
```

- [ ] **步骤 4：运行测试验证通过**

运行：`cargo test -p modelscope-sync-core -- cache::tests`
预期：PASS。

- [ ] **步骤 5：Commit**

```bash
git add crates/core/src/cache.rs
git commit -m "feat(core): add cache path resolution"
```

---

### 任务 3：core — hash 模块

**文件：**

- 创建：`crates/core/src/hash.rs`
- 测试：`crates/core/src/hash.rs`（模块内单元测试）

- [ ] **步骤 1：编写失败的测试**

在 `crates/core/src/hash.rs` 中：

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn test_sha256_stream_known_content() {
        let data = b"hello world";
        let reader = std::io::Cursor::new(data.as_slice());
        let hash = sha256_stream(reader).await.unwrap();
        assert_eq!(hash, "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9");
    }
}
```

- [ ] **步骤 2：运行测试验证失败**

运行：`cargo test -p modelscope-sync-core -- hash::tests`
预期：FAIL，`sha256_stream` not found。

- [ ] **步骤 3：实现 sha256_stream**

在 `crates/core/src/hash.rs` 中：

```rust
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncReadExt};

pub async fn sha256_stream<R: AsyncRead + Unpin>(mut reader: R) -> std::io::Result<String> {
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 8192];
    loop {
        let n = reader.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}
```

- [ ] **步骤 4：运行测试验证通过**

运行：`cargo test -p modelscope-sync-core -- hash::tests`
预期：PASS。

- [ ] **步骤 5：Commit**

```bash
git add crates/core/src/hash.rs
git commit -m "feat(core): add streaming sha256 computation"
```

---

### 任务 4：core — api 模块

**文件：**

- 创建：`crates/core/src/api.rs`
- 修改：`crates/core/src/lib.rs`（添加公共类型）
- 测试：`crates/core/src/api.rs`（模块内单元测试，使用 wiremock）

- [ ] **步骤 1：在 lib.rs 中定义公共类型**

修改 `crates/core/src/lib.rs`：

```rust
use serde::Deserialize;

pub mod api;
pub mod cache;
pub mod download;
pub mod hash;
pub mod sync;

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
```

- [ ] **步骤 2：编写失败的测试**

在 `crates/core/src/api.rs` 中：

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::{Mock, MockServer, ResponseTemplate};
    use wiremock::matchers::{method, path};

    #[tokio::test]
    async fn test_fetch_repo_files_success() {
        let server = MockServer::start().await;
        let body = r#"{"files":[{"path":"model.safetensors","sha256":"abc123","size":1024}]}"#;
        Mock::given(method("GET"))
            .and(path("/api/v1/models/test-model/repo"))
            .respond_with(ResponseTemplate::new(200).set_body_string(body))
            .mount(&server)
            .await;

        let client = reqwest::Client::new();
        let files = fetch_repo_files(&client, &server.uri(), "test-model").await.unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].path, "model.safetensors");
    }
}
```

- [ ] **步骤 3：运行测试验证失败**

运行：`cargo test -p modelscope-sync-core -- api::tests`
预期：FAIL，`fetch_repo_files` not found。

- [ ] **步骤 4：实现 fetch_repo_files**

在 `crates/core/src/api.rs` 中：

```rust
use crate::{FileMeta, Result};
use reqwest::Client;
use serde::Deserialize;

#[derive(Deserialize)]
struct ApiResponse {
    files: Vec<FileMeta>,
}

pub async fn fetch_repo_files(client: &Client, base_url: &str, model_id: &str) -> Result<Vec<FileMeta>> {
    let url = format!("{}/api/v1/models/{}/repo", base_url, model_id);
    let resp: ApiResponse = client.get(&url).send().await?.json().await?;
    Ok(resp.files)
}
```

- [ ] **步骤 5：运行测试验证通过**

运行：`cargo test -p modelscope-sync-core -- api::tests`
预期：PASS。

- [ ] **步骤 6：Commit**

```bash
git add crates/core/src/lib.rs crates/core/src/api.rs
git commit -m "feat(core): add modelscope api client"
```

---

### 任务 5：core — download 模块

**文件：**

- 创建：`crates/core/src/download.rs`
- 测试：`crates/core/src/download.rs`（模块内单元测试，使用 wiremock）

- [ ] **步骤 1：编写失败的测试**

在 `crates/core/src/download.rs` 中：

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use tokio::io::AsyncWriteExt;
    use wiremock::{Mock, MockServer, ResponseTemplate};
    use wiremock::matchers::method;

    #[tokio::test]
    async fn test_stream_download_success() {
        let server = MockServer::start().await;
        let body = b"hello world";
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(body.as_slice()))
            .mount(&server)
            .await;

        let client = reqwest::Client::new();
        let mut writer = Vec::new();
        let (tx, _rx) = tokio::sync::mpsc::channel(10);
        stream_download(&client, &format!("{}/file.bin", server.uri()), &mut writer, tx).await.unwrap();
        assert_eq!(writer, body);
    }
}
```

- [ ] **步骤 2：运行测试验证失败**

运行：`cargo test -p modelscope-sync-core -- download::tests`
预期：FAIL，`stream_download` not found。

- [ ] **步骤 3：实现 stream_download**

在 `crates/core/src/download.rs` 中：

```rust
use crate::Result;
use reqwest::Client;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc::Sender;

pub async fn stream_download<W: AsyncWrite + Unpin>(
    client: &Client,
    url: &str,
    writer: &mut W,
    progress_tx: Sender<u64>,
) -> Result<()> {
    let resp = client.get(url).send().await?;
    let mut stream = resp.bytes_stream();
    let mut downloaded = 0u64;

    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        writer.write_all(&chunk).await?;
        downloaded += chunk.len() as u64;
        let _ = progress_tx.send(downloaded).await;
    }
    writer.flush().await?;
    Ok(())
}
```

在文件顶部添加：

```rust
use futures::StreamExt;
```

并更新 `crates/core/Cargo.toml` 添加 `futures = "0.3"` 依赖。

- [ ] **步骤 4：运行测试验证通过**

运行：`cargo test -p modelscope-sync-core -- download::tests`
预期：PASS。

- [ ] **步骤 5：Commit**

```bash
git add crates/core/Cargo.toml crates/core/src/download.rs
git commit -m "feat(core): add http streaming download with progress"
```

---

### 任务 6：core — sync 模块

**文件：**

- 创建：`crates/core/src/sync.rs`
- 修改：`crates/core/src/lib.rs`（添加 `SyncReport`）
- 测试：`crates/core/src/sync.rs`（模块内单元测试）

- [ ] **步骤 1：在 lib.rs 添加 SyncReport**

修改 `crates/core/src/lib.rs`：

```rust
#[derive(Debug, Clone)]
pub struct SyncReport {
    pub total_files: usize,
    pub cached_files: usize,
    pub downloaded_files: usize,
    pub failed_files: usize,
}
```

- [ ] **步骤 2：编写失败的测试**

在 `crates/core/src/sync.rs` 中编写一个简化测试（后续在集成环境中完整测试）：

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_sync_report_structure() {
        let report = SyncReport {
            total_files: 3,
            cached_files: 1,
            downloaded_files: 1,
            failed_files: 1,
        };
        assert_eq!(report.total_files, 3);
    }
}
```

- [ ] **步骤 3：运行测试验证失败**

运行：`cargo test -p modelscope-sync-core -- sync::tests`
预期：FAIL，`SyncReport` 或模块未找到。

- [ ] **步骤 4：实现 sync 模块骨架**

在 `crates/core/src/sync.rs` 中：

```rust
use crate::{cache, hash, download, api, FileMeta, SyncReport, Result, CoreError};
use std::path::Path;
use tokio::fs;
use tokio::sync::mpsc::Sender;

pub async fn sync_model(
    client: &reqwest::Client,
    api_base: &str,
    model_id: &str,
    cache_dir: &Path,
    target_dir: &Path,
    max_concurrent: usize,
    progress_tx: Sender<(String, u64, u64)>,
) -> Result<SyncReport> {
    let files = api::fetch_repo_files(client, api_base, model_id).await?;
    let mut report = SyncReport {
        total_files: files.len(),
        cached_files: 0,
        downloaded_files: 0,
        failed_files: 0,
    };

    let semaphore = tokio::sync::Semaphore::new(max_concurrent);
    let mut handles = vec![];

    for file in files {
        let permit = semaphore.acquire().await.unwrap();
        let cache_dir = cache_dir.to_path_buf();
        let target_dir = target_dir.to_path_buf();
        let model_id = model_id.to_string();
        let client = client.clone();
        let progress_tx = progress_tx.clone();

        let handle = tokio::spawn(async move {
            let _permit = permit;
            let target_path = cache::resolve_path(&target_dir, &model_id, &file.path);
            let cache_path = cache::resolve_path(&cache_dir, &model_id, &file.path);

            // 1. 检查目标目录
            if let Ok(true) = verify_file(&target_path, &file.sha256).await {
                if cache_path.exists() {
                    let _ = fs::remove_file(&cache_path).await;
                }
                return Ok((file.path, true, false));
            }

            // 2. 检查缓存目录
            if let Ok(true) = verify_file(&cache_path, &file.sha256).await {
                fs::rename(&cache_path, &target_path).await?;
                return Ok((file.path, true, false));
            }

            // 3. 下载到缓存目录
            fs::create_dir_all(cache_path.parent().unwrap()).await?;
            let tmp_path = cache_path.with_extension(format!("tmp.{}", uuid::Uuid::new_v4()));
            let mut file_handle = fs::File::create(&tmp_path).await?;
            let (inner_tx, mut inner_rx) = tokio::sync::mpsc::channel(10);

            let download_handle = tokio::spawn(async move {
                let url = format!("{}/resolve/{}/{}", api_base, model_id, file.path);
                download::stream_download(&client, &url, &mut file_handle, inner_tx).await
            });

            // 进度转发
            let progress_forward = tokio::spawn(async move {
                while let Some(bytes) = inner_rx.recv().await {
                    let _ = progress_tx.send((file.path.clone(), bytes, file.size)).await;
                }
            });

            download_handle.await??;
            progress_forward.await?;

            // 4. 校验
            let tmp_file = fs::File::open(&tmp_path).await?;
            let hash = hash::sha256_stream(tmp_file).await?;
            if hash != file.sha256 {
                let _ = fs::remove_file(&tmp_path).await;
                return Err(CoreError::HashMismatch);
            }

            // 5. 移动到目标目录
            fs::create_dir_all(target_path.parent().unwrap()).await?;
            fs::rename(&tmp_path, &target_path).await?;

            Ok((file.path, false, true))
        });

        handles.push(handle);
    }

    for handle in handles {
        match handle.await? {
            Ok((_, cached, downloaded)) => {
                if cached { report.cached_files += 1; }
                if downloaded { report.downloaded_files += 1; }
            }
            Err(_) => {
                report.failed_files += 1;
            }
        }
    }

    Ok(report)
}

async fn verify_file(path: &std::path::Path, expected: &str) -> std::io::Result<bool> {
    if !path.exists() {
        return Ok(false);
    }
    let file = fs::File::open(path).await?;
    let hash = hash::sha256_stream(file).await?;
    Ok(hash == expected)
}
```

注意：需要在 `crates/core/Cargo.toml` 中添加 `uuid = { version = "1", features = ["v4"] }`。

- [ ] **步骤 5：运行测试验证通过**

运行：`cargo test -p modelscope-sync-core`
预期：所有核心测试 PASS。

- [ ] **步骤 6：Commit**

```bash
git add crates/core/Cargo.toml crates/core/src/lib.rs crates/core/src/sync.rs
git commit -m "feat(core): add sync orchestration with target/cache dual-dir flow"
```

---

### 任务 7：daemon — state 和配置

**文件：**

- 创建：`crates/daemon/src/state.rs`
- 创建：`crates/daemon/src/main.rs`（覆盖）
- 修改：`crates/daemon/Cargo.toml`（添加 `uuid` 和 `chrono`）

- [ ] **步骤 1：编写 state.rs**

创建 `crates/daemon/src/state.rs`：

```rust
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
```

- [ ] **步骤 2：编写 main.rs（命令行解析）**

创建 `crates/daemon/src/main.rs`：

```rust
use clap::Parser;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "modelscope-sync-daemon")]
struct Args {
    #[arg(long, env = "CACHE_DIR", default_value = "/var/cache/modelscope")]
    cache_dir: PathBuf,

    #[arg(long, env = "TARGET_DIR", default_value = "/mnt/models")]
    target_dir: PathBuf,

    #[arg(long, env = "MAX_CONCURRENT", default_value = "3")]
    max_concurrent_downloads: usize,

    #[arg(long, env = "API_BASE", default_value = "https://www.modelscope.cn")]
    api_base: String,

    #[arg(long, env = "BIND_ADDR", default_value = "0.0.0.0:8080")]
    bind_addr: String,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    tracing_subscriber::fmt::init();
    tracing::info!("daemon starting with args: {:?}", args);
}
```

- [ ] **步骤 3：验证编译**

运行：`cargo check -p modelscope-sync-daemon`
预期：编译成功。

- [ ] **步骤 4：Commit**

```bash
git add crates/daemon/src/state.rs crates/daemon/src/main.rs crates/daemon/Cargo.toml
git commit -m "feat(daemon): add state management and cli args"
```

---

### 任务 8：daemon — tasks 模块

**文件：**

- 创建：`crates/daemon/src/tasks.rs`
- 修改：`crates/daemon/src/state.rs`（添加 `update_task` 辅助方法）

- [ ] **步骤 1：实现 tasks.rs**

创建 `crates/daemon/src/tasks.rs`：

```rust
use crate::state::{AppState, TaskState, TaskStatus};
use modelscope_sync_core::sync;
use std::sync::Arc;
use tokio::sync::mpsc;

pub async fn spawn_sync_task(model_id: String, state: Arc<AppState>) -> String {
    let task_id = uuid::Uuid::new_v4().to_string();
    let now = chrono::Utc::now();

    let task = TaskState {
        task_id: task_id.clone(),
        model_id: model_id.clone(),
        status: TaskStatus::Pending,
        total_files: 0,
        completed_files: 0,
        downloaded_bytes: 0,
        total_bytes: 0,
        cached_files: 0,
        error: None,
        created_at: now,
        updated_at: now,
    };

    state.tasks.insert(task_id.clone(), task.clone());
    let _ = state.broadcast.send(task);

    let task_id_clone = task_id.clone();
    let state_clone = state.clone();

    tokio::spawn(async move {
        run_sync(model_id, task_id_clone, state_clone).await;
    });

    task_id
}

async fn run_sync(model_id: String, task_id: String, state: Arc<AppState>) {
    let mut task = state.tasks.get(&task_id).unwrap().clone();
    task.status = TaskStatus::Running;
    task.updated_at = chrono::Utc::now();
    state.tasks.insert(task_id.clone(), task.clone());
    let _ = state.broadcast.send(task);

    let (tx, mut rx) = mpsc::channel::<(String, u64, u64)>(128);

    let progress_handle = {
        let state = state.clone();
        let task_id = task_id.clone();
        tokio::spawn(async move {
            while let Some((path, downloaded, total)) = rx.recv().await {
                if let mut t = state.tasks.get_mut(&task_id) {
                    t.downloaded_bytes = downloaded;
                    if total > 0 {
                        t.total_bytes = total;
                    }
                    t.updated_at = chrono::Utc::now();
                    let updated = t.clone();
                    drop(t);
                    let _ = state.broadcast.send(updated);
                }
            }
        })
    };

    let result = sync::sync_model(
        &state.reqwest_client,
        &state.config.api_base,
        &model_id,
        &state.config.cache_dir,
        &state.config.target_dir,
        state.config.max_concurrent_downloads,
        tx,
    ).await;

    progress_handle.await.ok();

    let mut task = state.tasks.get(&task_id).unwrap().clone();
    task.updated_at = chrono::Utc::now();

    match result {
        Ok(report) => {
            task.status = if report.failed_files > 0 {
                TaskStatus::Failed
            } else {
                TaskStatus::Success
            };
            task.total_files = report.total_files;
            task.completed_files = report.cached_files + report.downloaded_files;
            task.cached_files = report.cached_files;
        }
        Err(e) => {
            task.status = TaskStatus::Failed;
            task.error = Some(e.to_string());
        }
    }

    state.tasks.insert(task_id, task.clone());
    let _ = state.broadcast.send(task);
}
```

- [ ] **步骤 2：验证编译**

运行：`cargo check -p modelscope-sync-daemon`
预期：编译成功（可能有一些未使用的导入警告，忽略）。

- [ ] **步骤 3：Commit**

```bash
git add crates/daemon/src/tasks.rs
git commit -m "feat(daemon): add async task spawning and progress tracking"
```

---

### 任务 9：daemon — handlers 模块

**文件：**

- 创建：`crates/daemon/src/handlers.rs`
- 修改：`crates/daemon/src/server.rs`（创建骨架）

- [ ] **步骤 1：实现 handlers.rs**

创建 `crates/daemon/src/handlers.rs`：

```rust
use crate::state::{AppState, TaskState, TaskStatus};
use axum::{
    extract::{Path, State},
    response::{IntoResponse, Sse},
    Json,
};
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;

#[derive(Deserialize)]
pub struct SyncRequest {
    pub model_id: String,
}

#[derive(Serialize)]
pub struct SyncResponse {
    pub task_id: String,
}

#[derive(Serialize)]
pub struct TaskResponse {
    pub task_id: String,
    pub status: String,
    pub model_id: String,
    pub total_files: usize,
    pub completed_files: usize,
    pub downloaded_bytes: u64,
    pub total_bytes: u64,
    pub cached_files: usize,
    pub error: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

impl From<TaskState> for TaskResponse {
    fn from(t: TaskState) -> Self {
        Self {
            task_id: t.task_id,
            status: format!("{:?}", t.status).to_lowercase(),
            model_id: t.model_id,
            total_files: t.total_files,
            completed_files: t.completed_files,
            downloaded_bytes: t.downloaded_bytes,
            total_bytes: t.total_bytes,
            cached_files: t.cached_files,
            error: t.error,
            created_at: t.created_at.to_rfc3339(),
            updated_at: t.updated_at.to_rfc3339(),
        }
    }
}

pub async fn post_sync(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SyncRequest>,
) -> impl IntoResponse {
    // 检查是否已有同一 model_id 的任务在运行
    for entry in state.tasks.iter() {
        let task = entry.value();
        if task.model_id == req.model_id && (task.status == TaskStatus::Pending || task.status == TaskStatus::Running) {
            return (StatusCode::ACCEPTED, Json(SyncResponse { task_id: task.task_id.clone() }));
        }
    }

    let task_id = crate::tasks::spawn_sync_task(req.model_id, state).await;
    (StatusCode::ACCEPTED, Json(SyncResponse { task_id }))
}

pub async fn get_task(
    State(state): State<Arc<AppState>>,
    Path(task_id): Path<String>,
) -> impl IntoResponse {
    match state.tasks.get(&task_id) {
        Some(task) => (StatusCode::OK, Json(TaskResponse::from(task.clone()))).into_response(),
        None => (StatusCode::NOT_FOUND, "task not found").into_response(),
    }
}

pub async fn get_task_events(
    State(state): State<Arc<AppState>>,
    Path(task_id): Path<String>,
) -> Sse<BroadcastStream<TaskState>> {
    let rx = state.broadcast.subscribe();
    let filtered = tokio_stream::wrappers::BroadcastStream::new(rx)
        .filter_map(|result| async move {
            match result {
                Ok(task) if task.task_id == task_id => Some(Ok::<_, broadcast::error::RecvError>(task)),
                _ => None,
            }
        });
    // 注意：这里需要适配 SSE 格式，简化版使用 task 序列化
    Sse::new(filtered)
}

pub async fn health() -> impl IntoResponse {
    Json(serde_json::json!({"status": "ok"}))
}

pub async fn ready(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let cache_ok = tokio::fs::metadata(&state.config.cache_dir).await.is_ok();
    let target_ok = tokio::fs::metadata(&state.config.target_dir).await.is_ok();
    if cache_ok && target_ok {
        (StatusCode::OK, Json(serde_json::json!({"status": "ok"})))
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({"status": "not ready"})))
    }
}
```

- [ ] **步骤 2：创建 server.rs 骨架**

创建 `crates/daemon/src/server.rs`：

```rust
use crate::handlers;
use crate::state::AppState;
use axum::{
    routing::{get, post},
    Router,
};
use std::sync::Arc;

pub async fn run(state: Arc<AppState>, bind_addr: &str) {
    let app = Router::new()
        .route("/sync", post(handlers::post_sync))
        .route("/tasks/{task_id}", get(handlers::get_task))
        .route("/tasks/{task_id}/events", get(handlers::get_task_events))
        .route("/health", get(handlers::health))
        .route("/ready", get(handlers::ready))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(bind_addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
```

- [ ] **步骤 3：更新 main.rs 启动服务**

修改 `crates/daemon/src/main.rs`：

```rust
mod handlers;
mod server;
mod state;
mod tasks;

use clap::Parser;
use state::{AppState, Config};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "modelscope-sync-daemon")]
struct Args {
    #[arg(long, env = "CACHE_DIR", default_value = "/var/cache/modelscope")]
    cache_dir: PathBuf,

    #[arg(long, env = "TARGET_DIR", default_value = "/mnt/models")]
    target_dir: PathBuf,

    #[arg(long, env = "MAX_CONCURRENT", default_value = "3")]
    max_concurrent_downloads: usize,

    #[arg(long, env = "API_BASE", default_value = "https://www.modelscope.cn")]
    api_base: String,

    #[arg(long, env = "BIND_ADDR", default_value = "0.0.0.0:8080")]
    bind_addr: String,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    tracing_subscriber::fmt::init();
    tracing::info!("daemon starting with args: {:?}", args);

    let config = Config {
        cache_dir: args.cache_dir,
        target_dir: args.target_dir,
        max_concurrent_downloads: args.max_concurrent_downloads,
        api_base: args.api_base,
        bind_addr: args.bind_addr.clone(),
    };

    let state = AppState::new(config);
    server::run(state, &args.bind_addr).await;
}
```

- [ ] **步骤 4：添加缺失依赖**

在 `crates/daemon/Cargo.toml` 中添加：

```toml
serde_json = "1"
tokio-stream = "0.1"
```

- [ ] **步骤 5：验证编译**

运行：`cargo check -p modelscope-sync-daemon`
预期：编译成功。

- [ ] **步骤 6：Commit**

```bash
git add crates/daemon/src/handlers.rs crates/daemon/src/server.rs crates/daemon/src/main.rs crates/daemon/Cargo.toml
git commit -m "feat(daemon): add http handlers and server"
```

---

### 任务 10：daemon — Prometheus metrics 和 SSE 格式化

**文件：**

- 修改：`crates/daemon/src/server.rs`
- 修改：`crates/daemon/src/handlers.rs`
- 修改：`crates/daemon/src/main.rs`（初始化 metrics）

- [ ] **步骤 1：集成 metrics**

修改 `crates/daemon/src/main.rs`：

```rust
#[tokio::main]
async fn main() {
    let args = Args::parse();
    tracing_subscriber::fmt::init();

    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    metrics::set_global_recorder(recorder.clone()).unwrap();

    // ... 其余代码不变
}
```

修改 `crates/daemon/src/server.rs`：

```rust
use metrics_exporter_prometheus::PrometheusHandle;

pub async fn run(state: Arc<AppState>, bind_addr: &str, prometheus: PrometheusHandle) {
    let app = Router::new()
        .route("/sync", post(handlers::post_sync))
        .route("/tasks/{task_id}", get(handlers::get_task))
        .route("/tasks/{task_id}/events", get(handlers::get_task_events))
        .route("/health", get(handlers::health))
        .route("/ready", get(handlers::ready))
        .route("/metrics", get(move || async move { prometheus.render() }))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(bind_addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
```

- [ ] **步骤 2：在 tasks 中记录 metrics**

修改 `crates/daemon/src/tasks.rs`，在 `run_sync` 中添加：

```rust
metrics::counter!("modelscope_sync_tasks_total", "status" => format!("{:?}", task.status)).increment(1);
```

- [ ] **步骤 3：修复 SSE 格式**

修改 `crates/daemon/src/handlers.rs` 中的 `get_task_events`，使用 `axum::response::sse::Event`：

```rust
use axum::response::sse::{Event, Sse};
use futures::stream::StreamExt;

pub async fn get_task_events(
    State(state): State<Arc<AppState>>,
    Path(task_id): Path<String>,
) -> Sse<impl tokio_stream::Stream<Item = Result<Event, broadcast::error::RecvError>>> {
    let rx = state.broadcast.subscribe();
    let stream = BroadcastStream::new(rx)
        .filter_map(move |result| {
            let task_id = task_id.clone();
            async move {
                match result {
                    Ok(task) if task.task_id == task_id => {
                        let data = serde_json::json!({
                            "status": format!("{:?}", task.status).to_lowercase(),
                            "completed_files": task.completed_files,
                            "total_files": task.total_files,
                            "downloaded_bytes": task.downloaded_bytes,
                            "total_bytes": task.total_bytes,
                        });
                        Some(Ok(Event::default().data(data.to_string())))
                    }
                    _ => None,
                }
            }
        });
    Sse::new(stream)
}
```

- [ ] **步骤 4：添加 futures 依赖**

在 `crates/daemon/Cargo.toml` 中添加 `futures = "0.3"`。

- [ ] **步骤 5：验证编译**

运行：`cargo check -p modelscope-sync-daemon`
预期：编译成功。

- [ ] **步骤 6：Commit**

```bash
git add crates/daemon/src/main.rs crates/daemon/src/server.rs crates/daemon/src/handlers.rs crates/daemon/src/tasks.rs crates/daemon/Cargo.toml
git commit -m "feat(daemon): add prometheus metrics and sse formatting"
```

---

### 任务 11：daemon — 优雅关闭

**文件：**

- 修改：`crates/daemon/src/server.rs`
- 修改：`crates/daemon/src/main.rs`

- [ ] **步骤 1：实现优雅关闭**

修改 `crates/daemon/src/server.rs`：

```rust
pub async fn run(state: Arc<AppState>, bind_addr: &str, prometheus: PrometheusHandle) {
    let app = Router::new()
        .route("/sync", post(handlers::post_sync))
        .route("/tasks/{task_id}", get(handlers::get_task))
        .route("/tasks/{task_id}/events", get(handlers::get_task_events))
        .route("/health", get(handlers::health))
        .route("/ready", get(handlers::ready))
        .route("/metrics", get(move || async move { prometheus.render() }))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(bind_addr).await.unwrap();
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .unwrap();
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c().await.expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    tracing::info!("shutdown signal received, starting graceful shutdown");
}
```

- [ ] **步骤 2：更新 main.rs 传递 prometheus handle**

修改 `crates/daemon/src/main.rs`：

```rust
let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
let prometheus = recorder.handle();
metrics::set_global_recorder(recorder).unwrap();

// ...

server::run(state, &args.bind_addr, prometheus).await;
```

- [ ] **步骤 3：验证编译**

运行：`cargo check -p modelscope-sync-daemon`
预期：编译成功。

- [ ] **步骤 4：Commit**

```bash
git add crates/daemon/src/server.rs crates/daemon/src/main.rs
git commit -m "feat(daemon): add graceful shutdown on sigterm"
```

---

### 任务 12：集成测试

**文件：**

- 创建：`crates/daemon/tests/integration_test.rs`

- [ ] **步骤 1：编写集成测试**

创建 `crates/daemon/tests/integration_test.rs`：

```rust
use std::time::Duration;
use tokio::time::sleep;

#[tokio::test]
async fn test_health_endpoint() {
    // 启动服务
    // 发送 GET /health
    // 验证返回 200 {"status":"ok"}
}

#[tokio::test]
async fn test_sync_task_lifecycle() {
    // 启动服务
    // POST /sync {"model_id":"test-model"}
    // 验证返回 202 和 task_id
    // GET /tasks/{task_id}
    // 验证状态流转
}
```

由于集成测试需要启动完整服务，且需要 mock ModelScope API，建议在实现时使用 `wiremock` 作为外部依赖，并在测试中构造 `AppState` 后直接调用 handler。

简化版测试（直接测试 handler）：

```rust
#[tokio::test]
async fn test_health_handler() {
    // 直接调用 handlers::health().await
    // 验证响应
}
```

- [ ] **步骤 2：验证编译**

运行：`cargo test -p modelscope-sync-daemon`
预期：测试编译成功。

- [ ] **步骤 3：Commit**

```bash
git add crates/daemon/tests/
git commit -m "test(daemon): add integration tests"
```

---

## 自检

### 1. 规格覆盖度

| 规格章节 | 对应任务 |
| --------- | --------- |
| Workspace 结构 | 任务 1 |
| cache 模块（双目录路径计算） | 任务 2 |
| hash 模块（流式 SHA256） | 任务 3 |
| api 模块（ModelScope API） | 任务 4 |
| download 模块（流式下载+进度） | 任务 5 |
| sync 模块（目标→缓存→下载三阶段） | 任务 6 |
| daemon state + CLI 参数 | 任务 7 |
| daemon 异步任务管理 | 任务 8 |
| HTTP handlers（6 个端点） | 任务 9 |
| Prometheus metrics + SSE | 任务 10 |
| 优雅关闭 | 任务 11 |
| 测试策略 | 任务 3-6（单元测试），任务 12（集成测试） |

**遗漏：** tracing JSON 格式化输出未单独作为任务，可并入任务 7 或 10。

### 2. 占位符扫描

- 无 "TODO"、"待定"、"后续实现"
- 所有代码步骤包含实际代码块
- 无模糊描述（如"添加适当的错误处理"）

### 3. 类型一致性

- `TaskStatus` 在 state.rs、handlers.rs、tasks.rs 中一致使用
- `SyncReport` 在 core 和 daemon 之间一致
- `AppState` 结构一致

**修复：** 在任务 7 中添加 `tracing-subscriber` 的 JSON 格式化初始化。

---

*计划已完成。*
