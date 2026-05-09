use std::path::{Path, PathBuf};

/// Compute the local absolute path for a model file.
///
/// The returned path follows the pattern `{base_dir}/{model_id}/{file_path}`.
///
/// # Arguments
/// * `base_dir` — The root directory (e.g. cache directory or target directory).
/// * `model_id` — The model identifier, which may contain `/` characters.
/// * `file_path` — The relative path of the file within the model repository.
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
        assert_eq!(path, PathBuf::from("/cache/Qwen/Qwen-7B-Chat/model.safetensors"));
    }

    #[test]
    fn test_resolve_path_nested_file() {
        let base = PathBuf::from("/data");
        let path = resolve_path(&base, "BAAI/bge-small-zh", "safetensors/model.safetensors");
        assert_eq!(path, PathBuf::from("/data/BAAI/bge-small-zh/safetensors/model.safetensors"));
    }
}
