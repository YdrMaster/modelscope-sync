use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncReadExt};

/// 异步计算数据流的 SHA-256 哈希值。
///
/// 以 8 KiB 为单位分块读取输入，避免将大文件完整载入内存。
///
/// # Arguments
///
/// - `reader`: 任意异步读取器。
///
/// # Returns
///
/// SHA-256 摘要的小写十六进制字符串。
pub async fn sha256_stream<R: AsyncRead + Unpin>(mut reader: R) -> std::io::Result<String> {
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 8192];
    loop {
        let n = reader.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    let digest = hasher.finalize();
    let mut hex = String::with_capacity(digest.len() * 2);
    for byte in digest.iter() {
        use std::fmt::Write;
        write!(hex, "{:02x}", byte).unwrap();
    }
    Ok(hex)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_sha256_stream_known_content() {
        let data = b"hello world";
        let reader = std::io::Cursor::new(data.as_slice());
        let hash = sha256_stream(reader).await.unwrap();
        assert_eq!(
            hash,
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        );
    }

    #[tokio::test]
    async fn test_sha256_stream_empty() {
        let data = b"";
        let reader = std::io::Cursor::new(data.as_slice());
        let hash = sha256_stream(reader).await.unwrap();
        assert_eq!(
            hash,
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }
}
