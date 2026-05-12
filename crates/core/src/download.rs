use crate::Result;
use futures::StreamExt;
use reqwest::Client;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc::Sender;

/// 从指定 URL 流式下载文件并上报进度。
///
/// 数据分块写入提供的 writer，每写入一块后通过 `progress_tx` 发送累计已下载的字节数。
///
/// # Arguments
///
/// - `client`: HTTP 客户端。
/// - `url`: 远程文件 URL。
/// - `writer`: 异步写入器（例如 `tokio::fs::File`）。
/// - `progress_tx`: 用于发送进度更新（累计字节数）的通道发送端。
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
    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, ResponseTemplate};

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

        stream_download(
            &client,
            &format!("{}/file.bin", server.uri()),
            &mut writer,
            tx,
        )
        .await
        .unwrap();
        assert_eq!(writer, body);

        // 验证进度已被发送。
        let progress = rx.recv().await;
        assert!(progress.is_some());
        assert_eq!(progress.unwrap(), body.len() as u64);
    }
}
