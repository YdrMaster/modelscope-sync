use std::path::{Path, PathBuf};

/// 计算模型文件的本地绝对路径。
///
/// 返回的路径遵循 `{base_dir}/{model_id}/{file_path}` 的格式。
///
/// # Arguments
///
/// - `base_dir`: 根目录（例如缓存目录或目标目录）。
/// - `model_id`: 模型标识符，可能包含 `/` 字符。
/// - `file_path`: 文件在模型仓库内的相对路径。
pub fn resolve_path(base_dir: &Path, model_id: &str, file_path: &str) -> PathBuf {
    base_dir.join(model_id).join(file_path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_resolve_path_basic() {
        let base = PathBuf::from("/cache");
        let path = resolve_path(&base, "Qwen/Qwen-7B-Chat", "model.safetensors");
        assert_eq!(
            path,
            PathBuf::from("/cache/Qwen/Qwen-7B-Chat/model.safetensors")
        );
    }

    #[test]
    fn test_resolve_path_nested_file() {
        let base = PathBuf::from("/data");
        let path = resolve_path(&base, "BAAI/bge-small-zh", "safetensors/model.safetensors");
        assert_eq!(
            path,
            PathBuf::from("/data/BAAI/bge-small-zh/safetensors/model.safetensors")
        );
    }
}
