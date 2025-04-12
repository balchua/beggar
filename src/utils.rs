use std::path::{Path, PathBuf};

use bytes::Bytes;
use futures::{Stream, StreamExt, pin_mut};
use path_absolutize::Absolutize;
use s3s::{
    S3Result, StdError,
    auth::Credentials,
    dto::{self, Checksum, Timestamp, TimestampFormat},
    s3_error,
};
use stdx::default::default;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use transform_stream::AsyncTryStream;

use crate::error::*;

pub async fn copy_bytes<S, W>(mut stream: S, writer: &mut W) -> Result<u64>
where
    S: Stream<Item = Result<Bytes, StdError>> + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut nwritten: u64 = 0;
    while let Some(result) = stream.next().await {
        let bytes = match result {
            Ok(x) => x,
            Err(e) => return Err(Error::new(e)),
        };
        writer.write_all(&bytes).await?;
        nwritten += bytes.len() as u64;
    }
    writer.flush().await?;
    Ok(nwritten)
}

pub fn bytes_stream<S, E>(
    stream: S,
    content_length: usize,
) -> impl Stream<Item = Result<Bytes, E>> + Send + 'static
where
    S: Stream<Item = Result<Bytes, E>> + Send + 'static,
    E: Send + 'static,
{
    AsyncTryStream::<Bytes, E, _>::new(|mut y| async move {
        pin_mut!(stream);
        let mut remaining: usize = content_length;
        while let Some(result) = stream.next().await {
            let mut bytes = result?;
            if bytes.len() > remaining {
                bytes.truncate(remaining);
            }
            remaining -= bytes.len();
            y.yield_ok(bytes).await;
        }
        Ok(())
    })
}

pub fn hex(input: impl AsRef<[u8]>) -> String {
    hex_simd::encode_to_string(input.as_ref(), hex_simd::AsciiCase::Lower)
}

pub fn to_timestamp(datetime: &chrono::NaiveDateTime) -> Option<Timestamp> {
    let date_time_rfc3339 = datetime.and_utc().to_rfc3339();

    match Timestamp::parse(TimestampFormat::DateTime, date_time_rfc3339.as_str()) {
        Ok(t) => Some(t),
        Err(_) => None,
    }
}

pub fn resolve_abs_path(root_path: &PathBuf, path: impl AsRef<Path>) -> Result<PathBuf> {
    Ok(path.as_ref().absolutize_virtually(root_path)?.into_owned())
}

pub fn init_checksum_hasher(
    crc32: Option<&String>,
    crc32c: Option<&String>,
    sha1: Option<&String>,
    sha256: Option<&String>,
    crc64: Option<&String>,
) -> s3s::checksum::ChecksumHasher {
    let mut checksum: s3s::checksum::ChecksumHasher = default();
    if crc32.is_some() {
        checksum.crc32 = Some(default());
    }
    if crc32c.is_some() {
        checksum.crc32c = Some(default());
    }
    if sha1.is_some() {
        checksum.sha1 = Some(default());
    }
    if sha256.is_some() {
        checksum.sha256 = Some(default());
    }
    if crc64.is_some() {
        checksum.crc64nvme = Some(default());
    }
    checksum
}

/// convert Metadata to string
pub fn metadata_to_string(metadata: Option<&dto::Metadata>) -> String {
    match metadata {
        Some(metadata) => serde_json::to_string(metadata).unwrap_or_default(),
        None => "{}".to_string(),
    }
}

/// convert metadata in string to Metadata
pub fn metadata_from_string(metadata: &str) -> dto::Metadata {
    serde_json::from_str(metadata).unwrap_or_default()
}

/// retrieve the access key from Credentials
pub fn access_key_from_creds(cred: Option<&Credentials>) -> Option<&str> {
    cred.map(|c| c.access_key.as_str())
}

pub fn validate_checksums(
    checksum: &Checksum,
    crc32: Option<&String>,
    crc32c: Option<&String>,
    sha1: Option<&String>,
    sha256: Option<&String>,
    crc64: Option<&String>,
) -> S3Result<()> {
    if checksum.checksum_crc32 != crc32.cloned() {
        return Err(s3_error!(BadDigest, "checksum_crc32 mismatch"));
    }
    if checksum.checksum_crc32c != crc32c.cloned() {
        return Err(s3_error!(BadDigest, "checksum_crc32c mismatch"));
    }
    if checksum.checksum_sha1 != sha1.cloned() {
        return Err(s3_error!(BadDigest, "checksum_sha1 mismatch"));
    }
    if checksum.checksum_sha256 != sha256.cloned() {
        return Err(s3_error!(BadDigest, "checksum_sha256 mismatch"));
    }
    if checksum.checksum_crc64nvme != crc64.cloned() {
        return Err(s3_error!(BadDigest, "checksum_crc64nvme mismatch"));
    }
    Ok(())
}
