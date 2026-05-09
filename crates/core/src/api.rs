use crate::{FileMeta, Result};
use reqwest::Client;
use serde::Deserialize;

#[derive(Deserialize)]
struct ApiResponse {
    files: Vec<FileMeta>,
}

/// Fetch the list of files and their metadata from the ModelScope API.
///
/// # Arguments
/// * `client` — The HTTP client to use.
/// * `base_url` — Base URL of the ModelScope instance (e.g. `https://www.modelscope.cn`).
/// * `model_id` — The model identifier (e.g. `Qwen/Qwen-7B-Chat`).
pub async fn fetch_repo_files(client: &Client, base_url: &str, model_id: &str) -> Result<Vec<FileMeta>> {
    let url = format!("{}/api/v1/models/{}/repo", base_url, model_id);
    let resp: ApiResponse = client.get(&url).send().await?.json().await?;
    Ok(resp.files)
}

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
        assert_eq!(files[0].sha256, "abc123");
        assert_eq!(files[0].size, 1024);
    }

    #[tokio::test]
    async fn test_fetch_repo_files_not_found() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v1/models/not-found/repo"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;

        let client = reqwest::Client::new();
        let result = fetch_repo_files(&client, &server.uri(), "not-found").await;
        assert!(result.is_err());
    }
}
