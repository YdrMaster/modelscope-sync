use crate::Result;
use futures::StreamExt;
use reqwest::Client;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc::Sender;

pub async fn stream_download<W: AsyncWrite + Unpin>(
    client: &Client,
    url: &str,
    writer: &mut W,
    progress_tx: Sender<u64>,
) -> Result<()> {
    let resp = client.get(url).send().await?;
    let mut stream = resp.bytes_stream();
    let mut downloaded = 0u64;

    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        writer.write_all(&chunk).await?;
        downloaded += chunk.len() as u64;
        let _ = progress_tx.send(downloaded).await;
    }
    writer.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::{Mock, MockServer, ResponseTemplate};
    use wiremock::matchers::method;

    #[tokio::test]
    async fn test_stream_download_success() {
        let server = MockServer::start().await;
        let body = b"hello world download test";
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(body.as_slice()))
            .mount(&server)
            .await;

        let client = reqwest::Client::new();
        let mut writer = Vec::new();
        let (tx, mut rx) = tokio::sync::mpsc::channel(10);
        
        stream_download(&client, &format!("{}/file.bin", server.uri()), &mut writer, tx).await.unwrap();
        assert_eq!(writer, body);
        
        // 验证进度被发送
        let progress = rx.recv().await;
        assert!(progress.is_some());
        assert_eq!(progress.unwrap(), body.len() as u64);
    }
}
