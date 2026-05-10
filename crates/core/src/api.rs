use crate::{CoreError, FileMeta, Result};
use reqwest::Client;
use serde::Deserialize;

#[derive(Deserialize)]
struct MsFile {
    #[serde(rename = "Path")]
    path: String,
    #[serde(rename = "Sha256")]
    sha256: String,
    #[serde(rename = "Size")]
    size: u64,
}

#[derive(Deserialize)]
struct MsData {
    #[serde(rename = "Files")]
    files: Vec<MsFile>,
}

#[derive(Deserialize)]
struct MsResponse {
    #[serde(rename = "Data")]
    data: Option<MsData>,
    #[serde(rename = "Message")]
    message: Option<String>,
    #[serde(rename = "Success")]
    success: bool,
}

/// Fetch the list of files and their metadata from the ModelScope API.
///
/// # Arguments
/// * `client` — The HTTP client to use.
/// * `base_url` — Base URL of the ModelScope instance (e.g. `https://www.modelscope.cn`).
/// * `model_id` — The model identifier (e.g. `Qwen/Qwen-7B-Chat`).
pub async fn fetch_repo_files(
    client: &Client,
    base_url: &str,
    model_id: &str,
) -> Result<Vec<FileMeta>> {
    let url = format!(
        "{}/api/v1/models/{}/repo/files?Revision=master",
        base_url, model_id
    );
    let resp: MsResponse = client.get(&url).send().await?.json().await?;
    if !resp.success {
        return Err(CoreError::ApiResponseError {
            message: resp.message.unwrap_or_default(),
        });
    }
    let files = resp
        .data
        .ok_or_else(|| CoreError::ApiResponseError {
            message: "empty response data".to_string(),
        })?
        .files
        .into_iter()
        .map(|f| FileMeta {
            path: f.path,
            sha256: f.sha256,
            size: f.size,
        })
        .collect();
    Ok(files)
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn test_fetch_repo_files_success() {
        let server = MockServer::start().await;
        let body = r#"{"Code":200,"Data":{"Files":[{"Path":"model.safetensors","Sha256":"abc123","Size":1024}]},"Success":true}"#;
        Mock::given(method("GET"))
            .and(path("/api/v1/models/test-model/repo/files"))
            .respond_with(ResponseTemplate::new(200).set_body_string(body))
            .mount(&server)
            .await;

        let client = reqwest::Client::new();
        let files = fetch_repo_files(&client, &server.uri(), "test-model")
            .await
            .unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].path, "model.safetensors");
        assert_eq!(files[0].sha256, "abc123");
        assert_eq!(files[0].size, 1024);
    }

    #[tokio::test]
    async fn test_fetch_repo_files_api_error() {
        let server = MockServer::start().await;
        let body = r#"{"Code":10010202008,"Message":"参数错误","RequestId":"xxx","Success":false}"#;
        Mock::given(method("GET"))
            .and(path("/api/v1/models/bad-model/repo/files"))
            .respond_with(ResponseTemplate::new(200).set_body_string(body))
            .mount(&server)
            .await;

        let client = reqwest::Client::new();
        let result = fetch_repo_files(&client, &server.uri(), "bad-model").await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("参数错误"));
    }

    #[tokio::test]
    async fn test_fetch_repo_files_not_found() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v1/models/not-found/repo/files"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;

        let client = reqwest::Client::new();
        let result = fetch_repo_files(&client, &server.uri(), "not-found").await;
        assert!(result.is_err());
    }
}
