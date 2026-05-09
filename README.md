# ModelScope Sync

ModelScope Sync 是一个常驻的模型分发服务，自动将 ModelScope 上的模型仓库同步到本地目标目录，支持双目录（缓存 + 目标）增量校验、并发下载和 Prometheus 可观测性。

## 功能特性

- **仓库级同步**：以整个模型仓库为单位自动同步所有文件
- **双目录设计**：缓存目录作为下载暂存区，目标目录为最终存储位置，支持命中缓存时直接移动
- **SHA256 增量校验**：本地文件哈希匹配时跳过下载，仅下载变更部分
- **并发下载**：可配置的最大并发数，避免带宽和磁盘过载
- **异步任务模式**：HTTP API 提交任务后异步执行，支持 SSE 实时进度推送
- **云原生可观测性**：内置 `/health`、 `/ready` 探针和 Prometheus `/metrics` 端点
- **优雅关闭**：监听 SIGTERM 信号，完成当前 HTTP 请求后安全退出

## 快速开始

### 构建

```bash
cargo build --release -p modelscope-sync-daemon
```

### 启动服务

```bash
./target/release/modelscope-sync-daemon \
  --cache-dir /var/cache/modelscope \
  --target-dir /mnt/models \
  --max-concurrent 3 \
  --bind-addr 0.0.0.0:8080
```

或使用环境变量：

```bash
CACHE_DIR=/var/cache/modelscope \
TARGET_DIR=/mnt/models \
MAX_CONCURRENT=3 \
BIND_ADDR=0.0.0.0:8080 \
./target/release/modelscope-sync-daemon
```

## 命令行参数

| 参数 | 环境变量 | 默认值 | 说明 |
| ------ | --------- | -------- | ------ |
| `--cache-dir` | `CACHE_DIR` | `/var/cache/modelscope` | 下载缓存目录（暂存区） |
| `--target-dir` | `TARGET_DIR` | `/mnt/models` | 目标存储目录 |
| `--max-concurrent` | `MAX_CONCURRENT` | `3` | 最大并发下载文件数 |
| `--api-base` | `API_BASE` | `https://www.modelscope.cn` | ModelScope API 地址 |
| `--bind-addr` | `BIND_ADDR` | `0.0.0.0:8080` | HTTP 服务监听地址 |

## HTTP API

### 提交同步任务

```plaintext
POST /sync
```

请求体：

```json
{
  "model_id": "Qwen/Qwen-7B-Chat"
}
```

响应（202 Accepted）：

```json
{
  "task_id": "550e8400-e29b-41d4-a716-446655440000"
}
```

若同一 `model_id` 已有任务在运行，直接返回该任务的 `task_id`。

### 查询任务状态

```plaintext
GET /tasks/{task_id}
```

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

`status` 取值：`pending`、`running`、`success`、`failed`。

### SSE 实时进度

```plaintext
GET /tasks/{task_id}/events
```

建立 SSE 连接，服务端推送任务状态变更事件：

```plaintext
event: message
data: {"task_id":"...","status":"running","completed_files":1,"total_files":5}
```

### 健康探针

```plaintext
GET /health
```

Liveness 探针，服务存活即返回 200：

```json
{"status": "ok"}
```

```plaintext
GET /ready
```

Readiness 探针，缓存目录和目标目录均可访问时返回 200，否则 503：

```json
{"status": "ok"}
```

### Prometheus 指标

```plaintext
GET /metrics
```

暴露 Prometheus 格式的指标，包括：

| 指标名 | 类型 | 说明 |
| -------- | ------ | ------ |
| `modelscope_sync_tasks_total` | Counter | 已完成的任务总数（按 `status` 标签分类） |

## 文件同步策略

对每个文件按以下优先级处理：

1. **目标目录命中**：目标目录已存在该文件且 SHA256 匹配 → 跳过下载，清理缓存目录冗余副本
2. **缓存目录命中**：缓存目录已存在该文件且 SHA256 匹配 → 原子移动到目标目录
3. **下载**：均未命中 → HTTP 流式下载到缓存目录临时文件 → SHA256 校验 → 原子移动到目标目录
