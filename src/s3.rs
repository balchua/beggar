use crate::storage_backend::InternalInfo;
use crate::storage_backend::StorageBackend;
use crate::utils::*;
use crate::DataStore;

use async_trait::async_trait;
use s3s::dto::*;
use s3s::s3_error;
use s3s::S3Result;
use s3s::S3;
use s3s::{S3Request, S3Response};

use std::io;
use std::ops::Neg;
use std::ops::Not;

use tokio::fs;
use tokio::io::AsyncSeekExt;
use tokio_util::io::ReaderStream;

use futures::TryStreamExt;
use md5::{Digest, Md5};
use numeric_cast::NumericCast;
use stdx::default::default;
use tracing::debug;
use uuid::Uuid;

// fn normalize_path(path: &Path, delimiter: &str) -> Option<String> {
//     let mut normalized = String::new();
//     let mut first = true;
//     for component in path.components() {
//         match component {
//             Component::RootDir
//             | Component::CurDir
//             | Component::ParentDir
//             | Component::Prefix(_) => {
//                 return None;
//             }
//             Component::Normal(name) => {
//                 let name = name.to_str()?;
//                 if !first {
//                     normalized.push_str(delimiter);
//                 }
//                 normalized.push_str(name);
//                 first = false;
//             }
//         }
//     }
//     Some(normalized)
// }

/// <https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Range>
fn fmt_content_range(start: u64, end_inclusive: u64, size: u64) -> String {
    format!("bytes {start}-{end_inclusive}/{size}")
}

#[async_trait]
impl<T: DataStore> S3 for StorageBackend<T> {
    #[tracing::instrument]
    async fn get_bucket_location(
        &self,
        req: S3Request<GetBucketLocationInput>,
    ) -> S3Result<S3Response<GetBucketLocationOutput>> {
        let input = req.input;
        let path = self.get_bucket_path(&input.bucket)?;

        if !path.exists() {
            return Err(s3_error!(NoSuchBucket));
        }

        let output = GetBucketLocationOutput::default();
        Ok(S3Response::new(output))
    }

    #[tracing::instrument]
    async fn get_object(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        let input = req.input;

        // select from db here
        let detail = self.get_s3_item_detail(&input.bucket, &input.key).await?;

        if let Some(d) = detail {
            let e_tag = d.e_tag;
            let last_modified = d.last_modified;
            let data_location = d.data_location;
            let metadata = d.metadata;
            let internal_info = d.internal_info;

            let object_path = self.resolve_abs_path(data_location)?;
            let mut file = fs::File::open(&object_path)
                .await
                .map_err(|e| s3_error!(e, NoSuchKey))?;
            let file_metadata = try_!(file.metadata().await);
            let file_len = file_metadata.len();

            let (content_length, content_range) = match input.range {
                None => (file_len, None),
                Some(range) => {
                    let file_range = range.check(file_len)?;
                    let content_length = file_range.end - file_range.start;
                    let content_range =
                        fmt_content_range(file_range.start, file_range.end - 1, file_len);
                    (content_length, Some(content_range))
                }
            };
            let content_length_usize = try_!(usize::try_from(content_length));
            let content_length_i64 = try_!(i64::try_from(content_length));

            match input.range {
                Some(Range::Int { first, .. }) => {
                    try_!(file.seek(io::SeekFrom::Start(first)).await);
                }
                Some(Range::Suffix { length }) => {
                    let neg_offset = length.numeric_cast::<i64>().neg();
                    try_!(file.seek(io::SeekFrom::End(neg_offset)).await);
                }
                None => {}
            }

            let body = bytes_stream(
                ReaderStream::with_capacity(file, 4096),
                content_length_usize,
            );

            let info = serde_json::from_str(&internal_info).ok();
            let checksum = match &info {
                Some(info) => crate::checksum::from_internal_info(info),
                None => default(),
            };

            let last_modified_timestamp = self.to_timestamp(&last_modified);

            debug!("last modified in rfc 3339 format {:?}", last_modified,);
            let output = GetObjectOutput {
                body: Some(StreamingBlob::wrap(body)),
                content_length: Some(content_length_i64),
                content_range,
                last_modified: last_modified_timestamp,
                metadata: serde_json::from_str(&metadata).ok(),
                e_tag: Some(e_tag),
                checksum_crc32: checksum.checksum_crc32,
                checksum_crc32c: checksum.checksum_crc32c,
                checksum_sha1: checksum.checksum_sha1,
                checksum_sha256: checksum.checksum_sha256,
                ..Default::default()
            };
            Ok(S3Response::new(output))
        } else {
            Err(s3_error!(NoSuchKey))
        }
    }

    #[tracing::instrument]
    async fn head_bucket(
        &self,
        req: S3Request<HeadBucketInput>,
    ) -> S3Result<S3Response<HeadBucketOutput>> {
        let input = req.input;
        let path = self.get_bucket_path(&input.bucket)?;

        if !path.exists() {
            return Err(s3_error!(NoSuchBucket));
        }

        Ok(S3Response::new(HeadBucketOutput::default()))
    }

    #[tracing::instrument]
    async fn head_object(
        &self,
        req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        let input = req.input;

        // select from db here
        let detail = self.get_s3_item_detail(&input.bucket, &input.key).await?;
        if let Some(d) = detail {
            let last_modified = d.last_modified;
            let data_location = d.data_location;
            let metadata = d.metadata;

            let object_path = self.resolve_abs_path(data_location)?;
            if !object_path.exists() {
                return Err(s3_error!(NoSuchBucket));
            }
            let file_metadata = try_!(fs::metadata(object_path).await);
            let file_len = file_metadata.len();

            let last_modified_timestamp = self.to_timestamp(&last_modified);
            // TODO: detect content type
            let content_type = mime::APPLICATION_OCTET_STREAM;

            let output = HeadObjectOutput {
                content_length: Some(try_!(i64::try_from(file_len))),
                content_type: Some(content_type),
                last_modified: last_modified_timestamp,
                metadata: serde_json::from_str(&metadata).ok(),
                e_tag: Some(d.e_tag),
                ..Default::default()
            };
            Ok(S3Response::new(output))
        } else {
            Err(s3_error!(NoSuchKey))
        }
    }

    #[tracing::instrument]
    async fn list_buckets(
        &self,
        _: S3Request<ListBucketsInput>,
    ) -> S3Result<S3Response<ListBucketsOutput>> {
        let mut buckets: Vec<Bucket> = Vec::new();

        let buckets_in_db = self.get_all_buckets().await?;

        for bucket in &buckets_in_db {
            let path = self.resolve_abs_path(bucket)?;
            if path.exists() {
                let metadata = try_!(fs::metadata(path).await);
                let created =
                    Timestamp::from(metadata.created().unwrap_or(metadata.modified().unwrap()));
                let bucket = Bucket {
                    creation_date: Some(created),
                    name: Some(bucket.to_owned()),
                };
                debug!("bucket: {:?}", bucket);
                buckets.push(bucket);
            }
        }

        let output = ListBucketsOutput {
            buckets: Some(buckets),
            owner: None,
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    #[tracing::instrument]
    async fn list_objects(
        &self,
        req: S3Request<ListObjectsInput>,
    ) -> S3Result<S3Response<ListObjectsOutput>> {
        let v2_resp = self.list_objects_v2(req.map_input(Into::into)).await?;

        Ok(v2_resp.map_output(|v2| ListObjectsOutput {
            contents: v2.contents,
            delimiter: v2.delimiter,
            encoding_type: v2.encoding_type,
            name: v2.name,
            prefix: v2.prefix,
            max_keys: v2.max_keys,
            ..Default::default()
        }))
    }

    #[tracing::instrument]
    async fn list_objects_v2(
        &self,
        req: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        let input = req.input;

        //get data from db
        let prefix = match &input.prefix {
            Some(prefix) => prefix,
            None => &"".to_string(),
        };
        let items = self
            .get_s3_item_detail_with_filter(&input.bucket, &prefix)
            .await?;
        let mut objects: Vec<Object> = default();
        for item in items {
            let key = item.key.clone();
            let last_modified = self.to_timestamp(&item.last_modified);
            let data_location = item.data_location.clone();
            let path = self.resolve_abs_path(data_location)?;

            if path.exists() {
                let file_metadata = try_!(fs::metadata(path).await);
                let size = file_metadata.len() as i64;
                let object = Object {
                    key: Some(key),
                    last_modified: last_modified,
                    e_tag: Some(item.e_tag),
                    size: Some(size),
                    ..Default::default()
                };
                objects.push(object);
            }
        }

        let key_count = try_!(i32::try_from(objects.len()));

        let output = ListObjectsV2Output {
            key_count: Some(key_count),
            max_keys: Some(key_count),
            contents: Some(objects),
            delimiter: input.delimiter,
            encoding_type: input.encoding_type,
            name: Some(input.bucket),
            prefix: input.prefix,
            ..Default::default()
        };

        debug!("output {:?}", output);
        Ok(S3Response::new(output))
    }

    #[tracing::instrument]
    async fn put_object(
        &self,
        req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        let input = req.input;
        // There is no need to check for the storage_class, since we dont really care
        // if let Some(ref storage_class) = input.storage_class {
        //     let is_valid = ["STANDARD", "REDUCED_REDUNDANCY"].contains(&storage_class.as_str());
        //     if !is_valid {
        //         return Err(s3_error!(InvalidStorageClass));
        //     }
        // }

        let PutObjectInput {
            body,
            bucket,
            key,
            metadata,
            content_length,
            ..
        } = input;

        let body = body.ok_or(s3_error!(IncompleteBody))?;

        let mut checksum = self.init_checksum_hasher(
            &input.checksum_crc32,
            &input.checksum_crc32c,
            &input.checksum_sha1,
            &input.checksum_sha256,
        );

        if key.ends_with('/') {
            self.handle_directory_creation(content_length, bucket.as_str(), key.as_str())
                .await?;
        }

        let object_path = self.get_object_path(&bucket, &key)?;
        let mut file_writer = self.prepare_file_write(&object_path).await?;

        let mut md5_hash = <Md5 as Digest>::new();
        let stream = body.inspect_ok(|bytes| {
            md5_hash.update(bytes.as_ref());
            checksum.update(bytes.as_ref());
        });

        let size = copy_bytes(stream, file_writer.writer()).await?;
        file_writer.done().await?;

        let md5_sum = hex(md5_hash.finalize());

        let checksum = checksum.finalize();
        self.validate_checksums(
            &checksum,
            &input.checksum_crc32,
            &input.checksum_crc32c,
            &input.checksum_sha1,
            &input.checksum_sha256,
        )?;

        debug!(path = %object_path.display(), ?size, %md5_sum, ?checksum, "write file");

        let mut info: InternalInfo = default();
        crate::checksum::modify_internal_info(&mut info, &checksum);
        let e_tag = format!("{md5_sum}");
        // save db here
        self.save_s3_item_detail(
            bucket.as_str(),
            key.as_str(),
            e_tag.as_str(),
            &metadata,
            info,
        )
        .await?;

        let output = PutObjectOutput {
            e_tag: Some(e_tag),
            checksum_crc32: checksum.checksum_crc32,
            checksum_crc32c: checksum.checksum_crc32c,
            checksum_sha1: checksum.checksum_sha1,
            checksum_sha256: checksum.checksum_sha256,
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    #[tracing::instrument]
    async fn create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        let input = req.input;
        let upload_id = self.create_upload_id(req.credentials.as_ref()).await?;

        if let Some(ref metadata) = input.metadata {
            self.save_metadata(&input.bucket, &input.key, metadata, Some(upload_id))
                .await?;
        }

        let output = CreateMultipartUploadOutput {
            bucket: Some(input.bucket),
            key: Some(input.key),
            upload_id: Some(upload_id.to_string()),
            ..Default::default()
        };

        Ok(S3Response::new(output))
    }

    #[tracing::instrument]
    async fn upload_part(
        &self,
        req: S3Request<UploadPartInput>,
    ) -> S3Result<S3Response<UploadPartOutput>> {
        let UploadPartInput {
            body,
            upload_id,
            part_number,
            ..
        } = req.input;

        let body = body.ok_or_else(|| s3_error!(IncompleteBody))?;

        let upload_id = Uuid::parse_str(&upload_id).map_err(|_| s3_error!(InvalidRequest))?;
        if self
            .verify_upload_id(req.credentials.as_ref(), &upload_id)
            .await?
            .not()
        {
            return Err(s3_error!(AccessDenied));
        }

        let file_path = self.resolve_upload_part_path(upload_id, part_number)?;

        println!("upload id: {:?}", upload_id);

        let mut md5_hash = <Md5 as Digest>::new();
        let stream = body.inspect_ok(|bytes| md5_hash.update(bytes.as_ref()));

        let mut file_writer = self.prepare_file_write(&file_path).await?;
        let size = copy_bytes(stream, file_writer.writer()).await?;
        file_writer.done().await?;

        let md5_sum = hex(md5_hash.finalize());

        debug!(path = %file_path.display(), ?size, %md5_sum, "write file");

        let output = UploadPartOutput {
            e_tag: Some(format!("\"{md5_sum}\"")),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    #[tracing::instrument]
    async fn list_parts(
        &self,
        req: S3Request<ListPartsInput>,
    ) -> S3Result<S3Response<ListPartsOutput>> {
        let ListPartsInput {
            bucket,
            key,
            upload_id,
            ..
        } = req.input;

        let mut parts: Vec<Part> = Vec::new();
        let mut iter = try_!(fs::read_dir(&self.root).await);

        let prefix = format!(".upload_id-{upload_id}");

        while let Some(entry) = try_!(iter.next_entry().await) {
            let file_type = try_!(entry.file_type().await);
            if file_type.is_file().not() {
                continue;
            }

            let file_name = entry.file_name();
            let Some(name) = file_name.to_str() else {
                continue;
            };

            let Some(part_segment) = name.strip_prefix(&prefix) else {
                continue;
            };
            let Some(part_number) = part_segment.strip_prefix(".part-") else {
                continue;
            };
            let part_number = part_number.parse::<i32>().unwrap();

            let file_meta = try_!(entry.metadata().await);
            let last_modified = Timestamp::from(try_!(file_meta.modified()));
            let size = try_!(i64::try_from(file_meta.len()));

            let part = Part {
                last_modified: Some(last_modified),
                part_number: Some(part_number),
                size: Some(size),
                ..Default::default()
            };
            parts.push(part);
        }

        let output = ListPartsOutput {
            bucket: Some(bucket),
            key: Some(key),
            upload_id: Some(upload_id),
            parts: Some(parts),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    #[tracing::instrument]
    async fn complete_multipart_upload(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        let CompleteMultipartUploadInput {
            multipart_upload,
            bucket,
            key,
            upload_id,
            ..
        } = req.input;

        let Some(multipart_upload) = multipart_upload else {
            return Err(s3_error!(InvalidPart));
        };

        let upload_id = Uuid::parse_str(&upload_id).map_err(|_| s3_error!(InvalidRequest))?;
        if self
            .verify_upload_id(req.credentials.as_ref(), &upload_id)
            .await?
            .not()
        {
            return Err(s3_error!(AccessDenied));
        }

        self.delete_upload_id(&upload_id).await?;

        if let Ok(Some(metadata)) = self.load_metadata(&bucket, &key, Some(upload_id)).await {
            self.save_metadata(&bucket, &key, &metadata, None).await?;
            let _ = self.delete_metadata(&bucket, &key, Some(upload_id));
        }

        let object_path = self.get_object_path(&bucket, &key)?;
        let mut file_writer = self.prepare_file_write(&object_path).await?;

        let mut cnt: i32 = 0;
        for part in multipart_upload.parts.into_iter().flatten() {
            let part_number = part
                .part_number
                .ok_or_else(|| s3_error!(InvalidRequest, "missing part number"))?;
            cnt += 1;
            if part_number != cnt {
                return Err(s3_error!(InvalidRequest, "invalid part order"));
            }

            let part_path = self.resolve_upload_part_path(upload_id, part_number)?;

            let mut reader = try_!(fs::File::open(&part_path).await);
            let size = try_!(tokio::io::copy(&mut reader, &mut file_writer.writer()).await);

            debug!(from = %part_path.display(), tmp = %file_writer.tmp_path().display(), to = %file_writer.dest_path().display(), ?size, "write file");
            try_!(fs::remove_file(&part_path).await);
        }
        file_writer.done().await?;

        let file_size = try_!(fs::metadata(&object_path).await).len();
        let md5_sum = self.get_md5_sum(&bucket, &key).await?;

        debug!(?md5_sum, path = %object_path.display(), size = ?file_size, "file md5 sum");

        let output = CompleteMultipartUploadOutput {
            bucket: Some(bucket),
            key: Some(key),
            e_tag: Some(format!("\"{md5_sum}\"")),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    #[tracing::instrument]
    async fn abort_multipart_upload(
        &self,
        req: S3Request<AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
        let AbortMultipartUploadInput {
            bucket,
            key,
            upload_id,
            ..
        } = req.input;

        let upload_id = Uuid::parse_str(&upload_id).map_err(|_| s3_error!(InvalidRequest))?;
        if self
            .verify_upload_id(req.credentials.as_ref(), &upload_id)
            .await?
            .not()
        {
            return Err(s3_error!(AccessDenied));
        }

        let _ = self.delete_metadata(&bucket, &key, Some(upload_id));

        let prefix = format!(".upload_id-{upload_id}");
        let mut iter = try_!(fs::read_dir(&self.root).await);
        while let Some(entry) = try_!(iter.next_entry().await) {
            let file_type = try_!(entry.file_type().await);
            if file_type.is_file().not() {
                continue;
            }

            let file_name = entry.file_name();
            let Some(name) = file_name.to_str() else {
                continue;
            };

            if name.starts_with(&prefix) {
                try_!(fs::remove_file(entry.path()).await);
            }
        }

        self.delete_upload_id(&upload_id).await?;

        debug!(bucket = %bucket, key = %key, upload_id = %upload_id, "multipart upload aborted");

        Ok(S3Response::new(AbortMultipartUploadOutput {
            ..Default::default()
        }))
    }
}
