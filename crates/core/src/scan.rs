use crate::{CoreError, Result, api, sync};
use std::path::Path;
use tokio::fs;

/// 扫描缓存目录和目标目录，验证并整理已下载的模型文件。
///
/// 对每个推断出的模型 ID，执行以下步骤：
/// 1. 从 ModelScope API 获取文件列表及预期 hash。
/// 2. 验证缓存目录中的对应文件：
///    - 哈希正确则原子移动到目标目录；
///    - 哈希错误则从缓存删除。
/// 3. 验证目标目录中的对应文件：
///    - 哈希正确则保留；
///    - 哈希错误则删除。
/// 4. 打印整理报告。
///
/// 若模型 ID 在 ModelScope API 上不存在，则直接忽略该模型。
pub async fn scan_and_organize(
    client: &reqwest::Client,
    cache_dir: &Path,
    target_dir: &Path,
) -> Result<()> {
    info!(cache_dir = %cache_dir.display(), target_dir = %target_dir.display(), "starting background cache scan");
    let model_ids = discover_model_ids(client, cache_dir, target_dir).await?;
    info!(detected_count = model_ids.len(), model_ids = ?model_ids, "model id discovery completed");

    for model_id in model_ids {
        match process_model(client, cache_dir, target_dir, &model_id).await {
            Ok(report) => info!(
                model_id = %model_id,
                total = report.total_files,
                moved = report.moved_files,
                verified = report.verified_files,
                deleted = report.deleted_files,
                "model scan completed"
            ),
            Err(e) => error!(model_id = %model_id, error = %e, "model scan failed"),
        }
    }

    Ok(())
}

/// 扫描缓存目录和目标目录，推断所有可能的模型 ID。
///
/// 同时从两个目录收集候选，合并去重后通过 ModelScope API 验证有效性。
async fn discover_model_ids(
    client: &reqwest::Client,
    cache_dir: &Path,
    target_dir: &Path,
) -> Result<Vec<String>> {
    let mut candidates = Vec::new();

    // 扫描缓存目录。
    let mut visited_cache = std::collections::HashSet::new();
    if cache_dir.exists() {
        let mut entries = fs::read_dir(cache_dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.is_dir() {
                collect_candidates(&path, cache_dir, &mut candidates, &mut visited_cache).await?;
            }
        }
    }

    // 扫描目标目录。
    let mut visited_target = std::collections::HashSet::new();
    if target_dir.exists() {
        let mut entries = fs::read_dir(target_dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.is_dir() {
                collect_candidates(&path, target_dir, &mut candidates, &mut visited_target).await?;
            }
        }
    }

    candidates.sort();
    candidates.dedup();
    info!(candidate_count = candidates.len(), candidates = ?candidates, "collected model id candidates");

    candidates.sort_by_key(|s| s.len());

    let mut confirmed = Vec::new();
    let mut covered = std::collections::HashSet::new();

    for candidate in candidates {
        // 避免将已确认模型的子目录误识别为独立模型。
        // 例如已确认 Qwen/Qwen-7B-Chat 后，跳过 Qwen/Qwen-7B-Chat/subdir。
        if covered
            .iter()
            .any(|c: &String| candidate.starts_with(&format!("{}/", c)))
        {
            info!(candidate = %candidate, "skipped because it is a subdirectory of a confirmed model");
            continue;
        }

        match api::fetch_repo_files(client, "https://www.modelscope.cn", &candidate).await {
            Ok(_) => {
                info!(candidate = %candidate, "confirmed valid model id");
                confirmed.push(candidate.clone());
                covered.insert(candidate);
            }
            Err(e) => {
                warn!(candidate = %candidate, error = %e, "candidate rejected by api validation");
            }
        }
    }

    Ok(confirmed)
}

/// 递归收集包含文件的目录路径作为候选 model_id。
async fn collect_candidates(
    dir: &Path,
    base_dir: &Path,
    candidates: &mut Vec<String>,
    visited: &mut std::collections::HashSet<std::path::PathBuf>,
) -> Result<()> {
    if !visited.insert(dir.to_path_buf()) {
        return Ok(());
    }

    let mut has_files = false;
    let mut sub_dirs = Vec::new();

    let mut entries = fs::read_dir(dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if path.is_file() {
            has_files = true;
        } else if path.is_dir() {
            sub_dirs.push(path);
        }
    }

    if has_files {
        let relative = dir
            .strip_prefix(base_dir)
            .map_err(|e| CoreError::Io(std::io::Error::other(format!("strip prefix: {}", e))))?;
        let candidate = relative.to_string_lossy().replace('\\', "/");
        if !candidate.is_empty() {
            info!(candidate = %candidate, dir = %dir.display(), "found candidate model id");
            candidates.push(candidate);
        }
    } else {
        info!(dir = %dir.display(), "no files in this directory, recursing into subdirectories");
    }

    for sub_dir in sub_dirs {
        // 递归调用需要使用 Box::pin，因为 async fn 的递归调用在编译期无法确定大小。
        Box::pin(collect_candidates(&sub_dir, base_dir, candidates, visited)).await?;
    }

    Ok(())
}

/// 单个模型的扫描整理报告。
struct ScanReport {
    /// ModelScope API 返回的文件总数。
    total_files: usize,
    /// 验证成功并从缓存移动到目标目录的文件数。
    moved_files: usize,
    /// 目标目录中已验证通过并保留的文件数。
    verified_files: usize,
    /// 验证失败并被删除的文件数（无论来自缓存还是目标目录）。
    deleted_files: usize,
}

/// 处理单个模型：验证并整理本地文件。
///
/// 对每个文件调用 [`crate::verify_and_organize`]，统计处理结果。
/// 文件不存在时直接跳过（不下载）。
async fn process_model(
    client: &reqwest::Client,
    cache_dir: &Path,
    target_dir: &Path,
    model_id: &str,
) -> Result<ScanReport> {
    let files = api::fetch_repo_files(client, "https://www.modelscope.cn", model_id).await?;

    let mut report = ScanReport {
        total_files: files.len(),
        moved_files: 0,
        verified_files: 0,
        deleted_files: 0,
    };

    for file_meta in &files {
        let cache_path = sync::resolve_path(cache_dir, model_id, &file_meta.path);
        let target_path = sync::resolve_path(target_dir, model_id, &file_meta.path);

        match crate::verify_and_organize(&cache_path, &target_path, &file_meta.sha256, true).await?
        {
            crate::FileStatus::Verified => {
                report.verified_files += 1;
                info!(path = %file_meta.path, "verified in target");
            }
            crate::FileStatus::Moved => {
                report.moved_files += 1;
                info!(path = %file_meta.path, "verified and moved to target");
            }
            crate::FileStatus::Deleted => {
                report.deleted_files += 1;
                warn!(
                    path = %file_meta.path,
                    expected = %file_meta.sha256,
                    "hash mismatch, deleted"
                );
            }
            crate::FileStatus::Missing => {
                info!(path = %file_meta.path, "file not present in cache or target");
            }
        }
    }

    Ok(report)
}
