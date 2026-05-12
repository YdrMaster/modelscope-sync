use crate::{CoreError, Result, SyncReport, api, download, hash};
use std::path::{Path, PathBuf};
use tokio::fs;
use tokio::sync::mpsc::Sender;
use tracing::{error, info};

/// 将完整的模型仓库从 ModelScope 同步到本地文件系统。
///
/// 对仓库中的每个文件按以下优先级处理：
///
/// 1. **目标命中** — 如果文件已存在于 `target_dir` 且 SHA-256 校验通过，
///    则跳过下载，并删除 `cache_dir` 中的旧副本。
/// 2. **缓存命中** — 如果文件已存在于 `cache_dir` 且 SHA-256 校验通过，
///    则将其原子移动到 `target_dir`。
/// 3. **下载** — 否则将文件流式下载到 `cache_dir` 的临时位置，
///    校验哈希后原子移动到 `target_dir`。
///
/// 下载通过全局 `Semaphore` 限流，最多同时下载 `max_concurrent` 个文件。
///
/// # Arguments
///
/// - `client`: 用于 API 和下载请求的 HTTP 客户端。
/// - `api_base`: ModelScope 实例的基础 URL。
/// - `model_id`: 模型标识符。
/// - `cache_dir`: 本地下载暂存目录。
/// - `target_dir`: 模型文件的最终目标目录。
/// - `max_concurrent`: 最大并发下载文件数。
/// - `progress_tx`: 用于发送每个文件进度更新
///   `(file_path, downloaded_bytes, total_bytes)` 的通道发送端。
///
/// # Returns
///
/// 返回 [`SyncReport`] 汇总同步结果。
pub async fn sync_model(
    client: &reqwest::Client,
    api_base: &str,
    model_id: &str,
    cache_dir: &Path,
    target_dir: &Path,
    max_concurrent: usize,
    progress_tx: Sender<(String, u64, u64)>,
) -> Result<SyncReport> {
    info!(model_id, "starting model sync");
    let files = api::fetch_repo_files(client, api_base, model_id).await?;
    info!(model_id, file_count = files.len(), "fetched file list");

    let mut report = SyncReport {
        total_files: files.len(),
        cached_files: 0,
        downloaded_files: 0,
        failed_files: 0,
    };

    let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(max_concurrent));
    let mut tasks = tokio::task::JoinSet::new();

    for file in &files {
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let cache_dir = cache_dir.to_path_buf();
        let target_dir = target_dir.to_path_buf();
        let model_id = model_id.to_string();
        let client = client.clone();
        let progress_tx = progress_tx.clone();
        let api_base = api_base.to_string();
        let file = file.clone();

        tasks.spawn(async move {
            let _permit = permit;
            let target_path = resolve_path(&target_dir, &model_id, &file.path);
            let cache_path = resolve_path(&cache_dir, &model_id, &file.path);

            // 1. 检查目标目录。
            if let Ok(true) = verify_file(&target_path, &file.sha256).await {
                info!(path = %file.path, "target hit, skipping download");
                if cache_path.exists() {
                    let _ = fs::remove_file(&cache_path).await;
                }
                return Ok((file.path, true, false));
            }

            // 2. 检查缓存目录。
            if let Ok(true) = verify_file(&cache_path, &file.sha256).await {
                info!(path = %file.path, "cache hit, moving to target directory");
                fs::create_dir_all(target_path.parent().unwrap()).await?;
                fs::rename(&cache_path, &target_path).await?;
                return Ok((file.path, true, false));
            }

            // 3. 下载到缓存目录。
            info!(path = %file.path, size = file.size, "starting file download");
            fs::create_dir_all(cache_path.parent().unwrap()).await?;
            let tmp_path = cache_path.with_extension(format!("tmp.{}", uuid::Uuid::new_v4()));

            let url = format!(
                "{api_base}/api/v1/models/{model_id}/repo?Revision=master&FilePath={}",
                file.path
            );
            if let Err(e) = download_to_tmp(
                &client,
                &url,
                &tmp_path,
                progress_tx.clone(),
                &file.path,
                file.size,
            )
            .await
            {
                error!(path = %file.path, error = %e, "download failed");
                let _ = fs::remove_file(&tmp_path).await;
                return Err(e);
            }

            // 4. 校验 SHA-256。
            let tmp_file = fs::File::open(&tmp_path).await?;
            let hash = hash::sha256_stream(tmp_file).await?;
            if hash != file.sha256 {
                error!(
                    path = %file.path,
                    expected = %file.sha256,
                    actual = %hash,
                    "SHA-256 verification failed, keeping temp file for debugging"
                );
                return Err(CoreError::HashMismatch);
            }

            // 5. 原子移动到目标目录。
            fs::create_dir_all(target_path.parent().unwrap()).await?;
            fs::rename(&tmp_path, &target_path).await?;
            info!(path = %file.path, "file downloaded and verified, moved to target directory");

            Ok((file.path, false, true))
        });
    }

    while let Some(res) = tasks.join_next().await {
        match res {
            Ok(Ok((_path, cached, downloaded))) => {
                if cached {
                    report.cached_files += 1;
                }
                if downloaded {
                    report.downloaded_files += 1;
                }
            }
            Ok(Err(e)) => {
                error!(error = %e, "file sync failed");
                report.failed_files += 1;
            }
            Err(e) => {
                error!(error = %e, "sync task panicked");
                report.failed_files += 1;
            }
        }
    }

    info!(
        total = report.total_files,
        cached = report.cached_files,
        downloaded = report.downloaded_files,
        failed = report.failed_files,
        "sync completed"
    );

    Ok(report)
}

/// 验证本地文件是否存在且其 SHA-256 哈希值与预期一致。
async fn verify_file(path: &std::path::Path, expected: &str) -> std::io::Result<bool> {
    if !path.exists() {
        return Ok(false);
    }
    let file = fs::File::open(path).await?;
    let hash = hash::sha256_stream(file).await?;
    Ok(hash == expected)
}

/// 将单个文件下载到临时路径，同时转发进度信息。
async fn download_to_tmp(
    client: &reqwest::Client,
    url: &str,
    tmp_path: &std::path::Path,
    progress_tx: tokio::sync::mpsc::Sender<(String, u64, u64)>,
    file_path: &str,
    file_size: u64,
) -> Result<()> {
    let mut file_handle = fs::File::create(tmp_path).await?;
    let (inner_tx, mut inner_rx) = tokio::sync::mpsc::channel(10);
    let file_path = file_path.to_string();
    let client = client.clone();
    let url = url.to_string();

    let download_handle = tokio::spawn(async move {
        download::stream_download(&client, &url, &mut file_handle, inner_tx).await
    });

    let progress_forward = tokio::spawn(async move {
        let mut last_bytes = 0u64;
        while let Some(bytes) = inner_rx.recv().await {
            let delta = bytes.saturating_sub(last_bytes);
            last_bytes = bytes;
            let _ = progress_tx
                .send((file_path.clone(), delta, file_size))
                .await;
        }
    });

    let result = match download_handle.await {
        Ok(r) => r,
        Err(e) => {
            return Err(CoreError::Io(std::io::Error::other(format!(
                "download task panicked: {e}"
            ))));
        }
    };
    drop(progress_forward);
    result
}

/// 计算模型文件的本地绝对路径。
///
/// 返回的路径遵循 `{base_dir}/{model_id}/{file_path}` 的格式。
fn resolve_path(base_dir: &Path, model_id: &str, file_path: &str) -> PathBuf {
    base_dir.join(model_id).join(file_path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    async fn compute_sha256(data: &[u8]) -> String {
        use crate::hash;
        let cursor = std::io::Cursor::new(data);
        hash::sha256_stream(cursor).await.unwrap()
    }

    async fn write_file(dir: &Path, relative: &str, content: &[u8]) {
        let path = dir.join(relative);
        tokio::fs::create_dir_all(path.parent().unwrap())
            .await
            .unwrap();
        tokio::fs::write(&path, content).await.unwrap();
    }

    #[tokio::test]
    async fn test_sync_model_all_target_hits() {
        let server = MockServer::start().await;
        let model_id = "test-model";

        let content1 = b"hello world file1";
        let content2 = b"hello world file2";
        let hash1 = compute_sha256(content1).await;
        let hash2 = compute_sha256(content2).await;

        let body = serde_json::json!({
            "Code": 200,
            "Data": {
                "Files": [
                    {"Path": "file1.txt", "Sha256": hash1, "Size": content1.len()},
                    {"Path": "file2.txt", "Sha256": hash2, "Size": content2.len()}
                ]
            },
            "Success": true
        })
        .to_string();

        Mock::given(method("GET"))
            .and(path("/api/v1/models/test-model/repo/files"))
            .respond_with(ResponseTemplate::new(200).set_body_string(body))
            .mount(&server)
            .await;

        let cache_dir = tempfile::tempdir().unwrap();
        let target_dir = tempfile::tempdir().unwrap();

        // 预先在 target_dir 中创建正确的文件。
        write_file(target_dir.path(), "test-model/file1.txt", content1).await;
        write_file(target_dir.path(), "test-model/file2.txt", content2).await;

        let client = reqwest::Client::new();
        let (tx, _rx) = tokio::sync::mpsc::channel(10);

        let report = sync_model(
            &client,
            &server.uri(),
            model_id,
            cache_dir.path(),
            target_dir.path(),
            2,
            tx,
        )
        .await
        .unwrap();

        assert_eq!(report.total_files, 2);
        assert_eq!(report.cached_files, 2);
        assert_eq!(report.downloaded_files, 0);
        assert_eq!(report.failed_files, 0);
    }

    #[tokio::test]
    async fn test_sync_model_cache_hits() {
        let server = MockServer::start().await;
        let model_id = "test-model";

        let content = b"cached file content";
        let hash = compute_sha256(content).await;

        let body = serde_json::json!({
            "Code": 200,
            "Data": {
                "Files": [
                    {"Path": "model.bin", "Sha256": hash, "Size": content.len()}
                ]
            },
            "Success": true
        })
        .to_string();

        Mock::given(method("GET"))
            .and(path("/api/v1/models/test-model/repo/files"))
            .respond_with(ResponseTemplate::new(200).set_body_string(body))
            .mount(&server)
            .await;

        let cache_dir = tempfile::tempdir().unwrap();
        let target_dir = tempfile::tempdir().unwrap();

        // 预先在 cache_dir 中创建正确的文件。
        write_file(cache_dir.path(), "test-model/model.bin", content).await;

        let client = reqwest::Client::new();
        let (tx, _rx) = tokio::sync::mpsc::channel(10);

        let report = sync_model(
            &client,
            &server.uri(),
            model_id,
            cache_dir.path(),
            target_dir.path(),
            2,
            tx,
        )
        .await
        .unwrap();

        assert_eq!(report.total_files, 1);
        assert_eq!(report.cached_files, 1);
        assert_eq!(report.downloaded_files, 0);
        assert_eq!(report.failed_files, 0);

        // 文件应被移动到 target_dir。
        let target_path = target_dir.path().join("test-model/model.bin");
        assert!(target_path.exists());

        // 文件应从 cache_dir 中删除。
        let cache_path = cache_dir.path().join("test-model/model.bin");
        assert!(!cache_path.exists());
    }

    #[tokio::test]
    async fn test_sync_model_download_then_fail_hash() {
        let server = MockServer::start().await;
        let model_id = "test-model";

        let content = b"downloaded content";
        let wrong_hash = "0000000000000000000000000000000000000000000000000000000000000000";

        let body = serde_json::json!({
            "Code": 200,
            "Data": {
                "Files": [
                    {"Path": "data.bin", "Sha256": wrong_hash, "Size": content.len()}
                ]
            },
            "Success": true
        })
        .to_string();

        Mock::given(method("GET"))
            .and(path("/api/v1/models/test-model/repo/files"))
            .respond_with(ResponseTemplate::new(200).set_body_string(body))
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/resolve/test-model/data.bin"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(content.as_slice()))
            .mount(&server)
            .await;

        let cache_dir = tempfile::tempdir().unwrap();
        let target_dir = tempfile::tempdir().unwrap();

        let client = reqwest::Client::new();
        let (tx, _rx) = tokio::sync::mpsc::channel(10);

        let report = sync_model(
            &client,
            &server.uri(),
            model_id,
            cache_dir.path(),
            target_dir.path(),
            2,
            tx,
        )
        .await
        .unwrap();

        assert_eq!(report.total_files, 1);
        assert_eq!(report.cached_files, 0);
        assert_eq!(report.downloaded_files, 0);
        assert_eq!(report.failed_files, 1);

        // 哈希不匹配后，临时文件应保留在 cache_dir 中供调试。
        let cache_model_dir = cache_dir.path().join("test-model");
        if cache_model_dir.exists() {
            let entries: Vec<_> = std::fs::read_dir(&cache_model_dir).unwrap().collect();
            assert!(
                !entries.is_empty(),
                "temp file should be kept for debugging"
            );
        }
    }

    #[tokio::test]
    async fn test_sync_model_api_not_found() {
        let server = MockServer::start().await;
        let model_id = "not-found";

        Mock::given(method("GET"))
            .and(path("/api/v1/models/not-found/repo/files"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;

        let cache_dir = tempfile::tempdir().unwrap();
        let target_dir = tempfile::tempdir().unwrap();

        let client = reqwest::Client::new();
        let (tx, _rx) = tokio::sync::mpsc::channel(10);

        let result = sync_model(
            &client,
            &server.uri(),
            model_id,
            cache_dir.path(),
            target_dir.path(),
            2,
            tx,
        )
        .await;

        assert!(matches!(result, Err(CoreError::ApiRequest(_))));
    }
}
