# ModelScope Sync 核心下载链路 MVP 设计文档

> 日期：2026-05-09
> 范围：核心下载链路 MVP（仓库级同步）
> 状态：已确认，待实现

## 1. 概述

ModelScope Sync 是一个常驻的模型分发服务，接收外部 HTTP 请求，自动将 ModelScope 上指定模型的整个仓库同步到目标目录，支持双目录（缓存目录 + 目标目录）的 SHA256 增量校验和并发下载。本设计覆盖 MVP 阶段，聚焦核心下载链路：从接收同步请求到完成仓库内所有文件的下载、校验与目录间流转。

## 2. 目标与非目标

### 2.1 目标

- 接收 `POST /sync` 请求，以整个模型仓库为单位进行异步同步
- 自动获取仓库内所有文件的元数据（SHA256、大小）
- 目标目录已存在且 SHA256 匹配时直接跳过，并清理缓存目录冗余副本
- 缓存目录已存在且 SHA256 匹配时直接移动到目标目录，跳过下载
- 均未命中时通过 HTTP 流式下载到缓存目录，边下边算 SHA256，校验通过后移动到目标目录
- 提供任务状态查询和 SSE 实时进度推送
- 暴露 Prometheus `/metrics`、健康探针、优雅关闭

### 2.2 非目标（后续扩展）

- Dragonfly P2P 下载集成
- 配置文件支持
- 外部配置文件（目录通过请求体或环境变量指定）
- 多实例分布式协调

## 3. 架构设计

### 3.1 Workspace 结构

```plaintext
modelscope-sync/
├── Cargo.toml              # workspace 根
└── crates/
    ├── core/               # lib crate — 核心同步逻辑
    │   ├── src/
    │   │   ├── lib.rs
    │   │   ├── api.rs      # ModelScope API 客户端
    │   │   ├── cache.rs    # 本地缓存路径管理
    │   │   ├── download.rs # HTTP 流式下载
    │   │   ├── hash.rs     # SHA256 流式计算
    │   │   └── sync.rs     # 主同步流程编排
    │   └── Cargo.toml
    └── daemon/             # bin crate — 常驻 HTTP 服务
        ├── src/
        │   ├── main.rs
        │   ├── server.rs   # axum 路由与服务启动
        │   ├── state.rs    # 应用状态（任务存储）
        │   ├── handlers.rs # HTTP handler
        │   └── tasks.rs    # 异步任务管理
        └── Cargo.toml
```

### 3.2 设计原则

- `core` 是纯逻辑库，不依赖 HTTP 框架，可独立测试
- `daemon` 是薄控制层，负责接收请求、管理任务生命周期、调用 `core`
- 所有业务逻辑下沉到 `core`，`daemon` 只做协议转换和状态维护
- 缓存目录和目标目录属于服务本地配置，不通过同步协议（`POST /sync`）传递，由 `daemon` 在启动时确定并注入 `core`

## 4. API 设计

### 4.1 端点一览

| 方法 | 路径 | 说明 |
| ------ | ------ | ------ |
| `POST` | `/sync` | 提交模型同步任务 |
| `GET` | `/tasks/{task_id}` | 查询任务总体状态 |
| `GET` | `/tasks/{task_id}/events` | SSE 实时进度推送 |
| `GET` | `/health` | Liveness 探针 |
| `GET` | `/ready` | Readiness 探针 |
| `GET` | `/metrics` | Prometheus 指标 |

### 4.2 POST /sync

请求体：

```json
{
  "model_id": "Qwen/Qwen-7B-Chat"
}
```

成功响应（202 Accepted）：

```json
{
  "task_id": "550e8400-e29b-41d4-a716-446655440000"
}
```

### 4.3 GET /tasks/{task_id}

响应：

```json
{
  "task_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "running",
  "model_id": "Qwen/Qwen-7B-Chat",
  "total_files": 5,
  "completed_files": 1,
  "downloaded_bytes": 1073977791,
  "total_bytes": 8590169159,
  "cached_files": 1,
  "error": null,
  "created_at": "2026-05-09T08:00:00Z",
  "updated_at": "2026-05-09T08:01:23Z"
}
```

`status` 枚举：`pending` → `running` → `success` | `failed`

### 4.4 GET /tasks/{task_id}/events

SSE 事件流：

```plaintext
event: progress
data: {"completed_files":1,"total_files":5,"downloaded_bytes":1073977791,"total_bytes":8590169159}

event: completed
data: {"status":"success","total_files":5,"cached_files":1}
```

### 4.5 探针

- `GET /health` — Liveness，服务存活即返回 200 `{"status":"ok"}`
- `GET /ready` — Readiness，本地配置的缓存目录和目标目录均可写返回 200，否则 503

## 5. 核心数据流

```plaintext
Client            Daemon                    Core
  |                 |                        |
  | -- POST /sync ->|                        |
  |                 | -- create task ------->|
  |                 |   (status: pending)    |
  |<- task_id ----- |                        |
  |                 | -- tokio::spawn ------>|
  |                 |                        |
  |                 |                        | - call ModelScope API
  |                 |                        |   -> Vec<FileMeta> (path, sha256, size)
  |                 |                        |
  |                 |                        | - for each file:
  |                 |                        |   1. check target_dir/{model_id}/{path}
  |                 |                        |      -> if sha256 match: delete cache copy, skip
  |                 |                        |   2. check cache_dir/{model_id}/{path}
  |                 |                        |      -> if sha256 match: move to target_dir, skip
  |                 |                        |   3. else: add to download queue
  |                 |                        |
  |                 |                        | - concurrent download (max 3) to cache_dir
  |                 |<-- update progress --- |
  |<-- SSE/events-- |                        |
  |                 |                        | - stream download + stream sha256
  |                 |                        | - verify sha256 in cache_dir
  |                 |                        | - atomic move: cache_dir -> target_dir
  |                 |                        |
  |                 |<--- task completed --->|
  |                 |    (status: success)   |
```

## 6. 模块详细设计

### 6.1 Core crate

| 模块 | 公开接口 | 职责 |
| ------ | --------- | ------ |
| `api` | `fetch_repo_files(model_id) -> Result<Vec<FileMeta>>` | 调用 ModelScope HTTP API 获取文件元数据列表，参考 `models-cat` 的 API 调用方式 |
| `cache` | `resolve_path(base_dir, model_id, file_path) -> PathBuf` | 计算本地绝对路径 `base_dir/{model_id}/{file_path}`，base_dir 可为 cache_dir 或 target_dir |
| `hash` | `sha256_stream(reader) -> Result<String>` | 基于 `sha2::Sha256` 的流式哈希计算 |
| `download` | `stream_download(url, writer, progress_tx) -> Result<()>` | `reqwest` 流式下载，参考 `models-cat` 的下载 URL 构造和进度回调模式 |
| `sync` | `sync_model(model_id, cache_dir, target_dir, progress_tx) -> Result<SyncReport>` | 编排整个同步流程：获取元数据 → 目标目录检查 → 缓存目录检查 → 并发下载到缓存 → 校验 → 移动到目标目录 → 汇总报告 |

关键实现细节：

- `download` 使用自定义 `HashingWriter`（实现 `AsyncWrite`），内部同时写入缓存目录的临时文件和更新 `Sha256`，避免下载后二次读取
- 下载到缓存目录的临时文件（`.tmp.{random}`），校验通过后原子移动到目标目录的目标路径
- 校验失败时删除缓存目录的临时文件，不污染任何目录
- 目标目录命中时，如果缓存目录存在同名文件则删除，确保缓存不冗余

### 6.2 Daemon crate

| 模块 | 职责 |
| ------ | ------ |
| `state` | `Arc<AppState>` 包含 `DashMap<String, TaskState>`、本地配置（cache_dir, target_dir, max_concurrent_downloads）、`broadcast` 通道用于 SSE |
| `tasks` | `spawn_sync_task(model_id, state)`：创建任务 → spawn 后台任务 → 接收 `core` 进度回调 → 更新 `TaskState` → 广播 SSE 事件 |
| `handlers` | axum 路由处理：`POST /sync`、`GET /tasks/{id}`、`GET /tasks/{id}/events`、`GET /health`、`GET /ready`、`GET /metrics` |
| `server` | 绑定 TCP、组装路由、启动 axum 服务、注册优雅关闭处理器 |

## 7. 错误处理

使用 `thiserror` 定义分层错误类型：

- `core::CoreError` — API 调用失败、下载中断、校验失败、IO 错误等
- `daemon::ApiError` — 任务不存在（404）、任务冲突（409）、内部错误（500）、上游失败（502）

| 场景 | 行为 |
| ------ | ------ |
| ModelScope API 调用失败 | 任务标记 `failed`，返回具体错误，不清理已有文件 |
| 下载中断（网络超时/断开） | 删除临时文件 `.tmp.*`，任务标记 `failed` |
| SHA256 校验失败 | 删除下载的临时文件，任务标记 `failed` |
| 磁盘满/写入失败 | 删除临时文件，任务标记 `failed` |
| 同一 model_id 已在同步中 | 直接返回已存在的 `task_id`，HTTP 202 |

## 8. 并发控制

- `tokio::sync::Semaphore` — 全局限制同时下载的文件数量，通过本地配置指定（默认 3），服务启动时加载
- 同一 `model_id` 任务互斥 — `DashMap::entry(model_id)` 原子判断，避免同一仓库重复同步
- 单个文件下载不额外限速，依赖 `reqwest` 默认 TCP 行为

## 9. 云原生可观测性

### 9.1 Metrics（Prometheus）

使用 `metrics` + `metrics-exporter-prometheus`：

| 指标名 | 类型 | 标签 | 说明 |
| -------- | ------ | ------ | ------ |
| `modelscope_sync_tasks_total` | Counter | `status` | 任务总数 |
| `modelscope_sync_task_duration_seconds` | Histogram | — | 任务耗时分布 |
| `modelscope_sync_download_bytes_total` | Counter | — | 实际下载字节数 |
| `modelscope_sync_target_hits_total` | Counter | — | 目标目录命中次数 |
| `modelscope_sync_cache_hits_total` | Counter | — | 缓存目录命中次数（移动到目标目录） |
| `modelscope_sync_active_downloads` | Gauge | — | 当前活跃下载数 |
| `modelscope_sync_files_total` | Counter | `status` | 文件处理总数 |

### 9.2 日志

使用 `tracing` 原生宏，`tracing-subscriber` 输出 JSON 格式。

- 每个同步任务创建 `tracing::span!(task_id, model_id)`，子调用自动继承上下文
- 日志字段：`task_id`、`model_id`、`file_path`、`event`

### 9.3 优雅关闭

- 捕获 `SIGTERM`
- 停止接受新请求（axum graceful shutdown）
- 等待已有下载任务完成或超时 30s
- 退出进程

## 10. 测试策略

### 10.1 Core crate 单元测试

| 测试目标 | 方法 |
| --------- | ------ |
| `cache::resolve_path` | 参数化测试 `model_id` 和内部文件路径，验证输出路径格式 |
| `hash::sha256_stream` | 构造已知内容 `AsyncRead`，比对预计算哈希 |
| `download::stream_download` | `wiremock` mock HTTP server，测试流式下载 + 进度回调 |
| `sync::sync_model`（全目标命中） | mock API 返回多个文件，目标目录均已命中，验证跳过下载并清理缓存冗余 |
| `sync::sync_model`（混合场景） | 部分命中、部分下载，验证并发下载和最终报告 |
| `sync::sync_model`（校验失败） | mock 错误哈希，验证临时文件清理和失败状态 |
| `sync::sync_model`（仓库不存在） | mock 404，验证任务立即失败 |

### 10.2 Daemon crate 集成测试

| 测试目标 | 方法 |
| --------- | ------ |
| `POST /sync` | 启动服务，发送请求，验证 202 和有效 task_id |
| `GET /tasks/{id}` 生命周期 | 轮询验证 `pending` → `running` → `success` |
| `GET /tasks/{id}/events` | 验证 SSE 流推送 `progress` 和 `completed` |
| `GET /metrics` | 验证 Prometheus 格式和指标存在性 |
| `GET /health` / `/ready` | 验证状态码 |
| 任务互斥 | 同一 model_id 两次提交，返回同一 task_id |
| 优雅关闭 | SIGTERM 后验证任务完成再退出 |

### 10.3 测试基础设施

- `wiremock` — mock HTTP server
- `tempfile::TempDir` — 隔离缓存目录
- `tokio::test` — 异步测试运行时

## 11. 关键依赖

### 11.1 Core

- `reqwest` — HTTP 客户端
- `serde` + `serde_json` — JSON 序列化
- `tokio` — 异步运行时
- `sha2` — SHA256 哈希
- `thiserror` — 错误定义
- `tracing` — 结构化日志

### 11.2 Daemon

- `axum` — Web 框架
- `tokio` — 异步运行时
- `dashmap` — 并发 HashMap
- `metrics` + `metrics-exporter-prometheus` — 指标暴露
- `tracing` + `tracing-subscriber` — 日志输出
- `core`（本地 workspace 依赖）

### 11.3 Dev

- `wiremock` — mock HTTP server
- `tempfile` — 临时目录
- `tokio-test` — 测试工具

## 12. 后续扩展

| 扩展项 | 说明 |
| -------- | ------ |
| Dragonfly 集成 | 下载时优先调用 `dfget`，失败回退 HTTP 直连 |
| CLI 工具 | 独立 `cli` crate，通过 HTTP API 与 daemon 交互 |
| 动态目录配置 | 通过特殊控制端点（如 `POST /config`）在运行时修改 `cache_dir` 和 `target_dir` |
| 分块并发下载 | 单文件多 range 并发下载，加速大文件 |
