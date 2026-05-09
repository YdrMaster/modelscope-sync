use std::path::{Path, PathBuf};

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
