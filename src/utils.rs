use std::path::Path;
use std::path::PathBuf;

use crate::error::*;

use s3s::dto::Timestamp;
use s3s::dto::TimestampFormat;
use s3s::StdError;

use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;

use bytes::Bytes;
use futures::pin_mut;
use futures::{Stream, StreamExt};
use path_absolutize::Absolutize;
use transform_stream::AsyncTryStream;

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
