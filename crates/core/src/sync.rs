use crate::{cache, hash, download, api, SyncReport, Result, CoreError};
use std::path::Path;
use tokio::fs;
use tokio::sync::mpsc::Sender;

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
    let files = api::fetch_repo_files(client, api_base, model_id).await?;
    let mut report = SyncReport {
        total_files: files.len(),
        cached_files: 0,
        downloaded_files: 0,
        failed_files: 0,
    };

    let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(max_concurrent));
    let mut handles = vec![];

    for file in files {
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let cache_dir = cache_dir.to_path_buf();
        let target_dir = target_dir.to_path_buf();
        let model_id = model_id.to_string();
        let client = client.clone();
        let progress_tx = progress_tx.clone();
        let api_base = api_base.to_string();

        let handle = tokio::spawn(async move {
            let _permit = permit;
            let target_path = cache::resolve_path(&target_dir, &model_id, &file.path);
            let cache_path = cache::resolve_path(&cache_dir, &model_id, &file.path);

            // 1. Check target directory.
            if let Ok(true) = verify_file(&target_path, &file.sha256).await {
                if cache_path.exists() {
                    let _ = fs::remove_file(&cache_path).await;
                }
                return Ok((file.path, true, false));
            }

            // 2. Check cache directory.
            if let Ok(true) = verify_file(&cache_path, &file.sha256).await {
                fs::create_dir_all(target_path.parent().unwrap()).await?;
                fs::rename(&cache_path, &target_path).await?;
                return Ok((file.path, true, false));
            }

            // 3. Download to cache directory.
            fs::create_dir_all(cache_path.parent().unwrap()).await?;
            let tmp_path = cache_path.with_extension(format!("tmp.{}", uuid::Uuid::new_v4()));
            let mut file_handle = fs::File::create(&tmp_path).await?;
            let (inner_tx, mut inner_rx) = tokio::sync::mpsc::channel(10);
            let file_path = file.path.clone();
            let file_size = file.size;

            let download_handle = tokio::spawn(async move {
                let url = format!("{}/resolve/{}/{}", api_base, model_id, file_path);
                download::stream_download(&client, &url, &mut file_handle, inner_tx).await
            });

            // Forward download progress to the caller.
            let file_path_for_progress = file.path.clone();
            let progress_forward = tokio::spawn(async move {
                while let Some(bytes) = inner_rx.recv().await {
                    let _ = progress_tx.send((file_path_for_progress.clone(), bytes, file_size)).await;
                }
            });

            match download_handle.await {
                Ok(r) => r?,
                Err(_) => return Err(CoreError::Io(std::io::Error::new(std::io::ErrorKind::Other, "download task panicked"))),
            }
            drop(progress_forward); // Close the receiver side after download finishes.

            // 4. Verify SHA-256.
            let tmp_file = fs::File::open(&tmp_path).await?;
            let hash = hash::sha256_stream(tmp_file).await?;
            if hash != file.sha256 {
                let _ = fs::remove_file(&tmp_path).await;
                return Err(CoreError::HashMismatch);
            }

            // 5. Atomically move to target directory.
            fs::create_dir_all(target_path.parent().unwrap()).await?;
            fs::rename(&tmp_path, &target_path).await?;

            Ok((file.path, false, true))
        });

        handles.push(handle);
    }

    for handle in handles {
        match handle.await {
            Ok(Ok((_, cached, downloaded))) => {
                if cached { report.cached_files += 1; }
                if downloaded { report.downloaded_files += 1; }
            }
            Ok(Err(_)) => {
                report.failed_files += 1;
            }
            Err(_) => {
                report.failed_files += 1;
            }
        }
    }

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
