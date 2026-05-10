use crate::{CoreError, Result, SyncReport, api, cache, download, hash};
use std::path::Path;
use tokio::fs;
use tokio::sync::mpsc::Sender;
use tracing::{error, info};

/// Synchronize an entire model repository from ModelScope to the local filesystem.
///
/// For each file in the repository the following precedence is applied:
/// 1. **Target hit** — If the file already exists in `target_dir` and its SHA-256
///    matches, the file is skipped and any stale copy in `cache_dir` is removed.
/// 2. **Cache hit** — If the file exists in `cache_dir` and its SHA-256 matches,
///    it is atomically moved to `target_dir`.
/// 3. **Download** — Otherwise the file is streamed down to a temporary location
///    in `cache_dir`, its hash is verified, and then it is atomically moved to
///    `target_dir`.
///
/// Downloads are limited by a global `Semaphore` so that at most
/// `max_concurrent` files are downloaded in parallel.
///
/// # Arguments
/// * `client` — HTTP client for API and download requests.
/// * `api_base` — Base URL of the ModelScope instance.
/// * `model_id` — The model identifier.
/// * `cache_dir` — Local staging directory for downloads.
/// * `target_dir` — Final destination directory for model files.
/// * `max_concurrent` — Maximum number of simultaneous file downloads.
/// * `progress_tx` — Channel sender for per-file progress updates
///   `(file_path, downloaded_bytes, total_bytes)`.
///
/// # Returns
/// A [`SyncReport`] summarizing the outcome.
pub async fn sync_model(
    client: &reqwest::Client,
    api_base: &str,
    model_id: &str,
    cache_dir: &Path,
    target_dir: &Path,
    max_concurrent: usize,
    progress_tx: Sender<(String, u64, u64)>,
) -> Result<SyncReport> {
    info!(model_id, "开始同步模型");
    let files = api::fetch_repo_files(client, api_base, model_id).await?;
    info!(model_id, file_count = files.len(), "获取到文件列表");

    let mut report = SyncReport {
        total_files: files.len(),
        cached_files: 0,
        downloaded_files: 0,
        failed_files: 0,
    };

    let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(max_concurrent));
    let mut handles = vec![];

    for file in &files {
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let cache_dir = cache_dir.to_path_buf();
        let target_dir = target_dir.to_path_buf();
        let model_id = model_id.to_string();
        let client = client.clone();
        let progress_tx = progress_tx.clone();
        let api_base = api_base.to_string();
        let file = file.clone();

        let handle = tokio::spawn(async move {
            let _permit = permit;
            let target_path = cache::resolve_path(&target_dir, &model_id, &file.path);
            let cache_path = cache::resolve_path(&cache_dir, &model_id, &file.path);

            // 1. Check target directory.
            if let Ok(true) = verify_file(&target_path, &file.sha256).await {
                info!(path = %file.path, "目标目录命中，跳过下载");
                if cache_path.exists() {
                    let _ = fs::remove_file(&cache_path).await;
                }
                return Ok((file.path, true, false));
            }

            // 2. Check cache directory.
            if let Ok(true) = verify_file(&cache_path, &file.sha256).await {
                info!(path = %file.path, "缓存目录命中，移动到目标目录");
                fs::create_dir_all(target_path.parent().unwrap()).await?;
                fs::rename(&cache_path, &target_path).await?;
                return Ok((file.path, true, false));
            }

            // 3. Download to cache directory.
            info!(path = %file.path, size = file.size, "开始下载文件");
            fs::create_dir_all(cache_path.parent().unwrap()).await?;
            let tmp_path = cache_path.with_extension(format!("tmp.{}", uuid::Uuid::new_v4()));

            let url = format!(
                "{}/api/v1/models/{}/repo?Revision=master&FilePath={}",
                api_base, model_id, file.path
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
                error!(path = %file.path, error = %e, "下载失败");
                let _ = fs::remove_file(&tmp_path).await;
                return Err(e);
            }

            // 4. Verify SHA-256.
            let tmp_file = fs::File::open(&tmp_path).await?;
            let hash = hash::sha256_stream(tmp_file).await?;
            if hash != file.sha256 {
                error!(
                    path = %file.path,
                    expected = %file.sha256,
                    actual = %hash,
                    "SHA-256 校验失败，保留临时文件供调试"
                );
                return Err(CoreError::HashMismatch);
            }

            // 5. Atomically move to target directory.
            fs::create_dir_all(target_path.parent().unwrap()).await?;
            fs::rename(&tmp_path, &target_path).await?;
            info!(path = %file.path, "文件下载并校验成功，已移动到目标目录");

            Ok((file.path, false, true))
        });

        handles.push(handle);
    }

    for handle in handles {
        match handle.await {
            Ok(Ok((_path, cached, downloaded))) => {
                if cached {
                    report.cached_files += 1;
                }
                if downloaded {
                    report.downloaded_files += 1;
                }
            }
            Ok(Err(e)) => {
                error!(error = %e, "文件同步失败");
                report.failed_files += 1;
            }
            Err(e) => {
                error!(error = %e, "同步任务 panic");
                report.failed_files += 1;
            }
        }
    }

    info!(
        total = report.total_files,
        cached = report.cached_files,
        downloaded = report.downloaded_files,
        failed = report.failed_files,
        "同步完成"
    );

    Ok(report)
}

/// Verify that a local file exists and its SHA-256 hash matches the expected value.
async fn verify_file(path: &std::path::Path, expected: &str) -> std::io::Result<bool> {
    if !path.exists() {
        return Ok(false);
    }
    let file = fs::File::open(path).await?;
    let hash = hash::sha256_stream(file).await?;
    Ok(hash == expected)
}

/// Download a single file to a temporary path while forwarding progress.
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
                "download task panicked: {}",
                e
            ))));
        }
    };
    drop(progress_forward);
    result
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

        // Pre-create files in target_dir with correct content.
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

        // Pre-create file in cache_dir with correct content.
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

        // File should be moved to target_dir.
        let target_path = target_dir.path().join("test-model/model.bin");
        assert!(target_path.exists());

        // File should be removed from cache_dir.
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

        // Temp file should remain in cache_dir for debugging after hash mismatch.
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
