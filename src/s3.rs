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

            let object_path = resolve_abs_path(&self.root, data_location)?;
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

            let last_modified_timestamp = to_timestamp(&last_modified);

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

            let object_path = resolve_abs_path(&self.root, data_location)?;
            if !object_path.exists() {
                return Err(s3_error!(NoSuchBucket));
            }
            let file_metadata = try_!(fs::metadata(object_path).await);
            let file_len = file_metadata.len();

            let last_modified_timestamp = to_timestamp(&last_modified);
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
            let path = resolve_abs_path(&self.root, bucket)?;
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
            let last_modified = to_timestamp(&item.last_modified);
            let data_location = item.data_location.clone();
            let path = resolve_abs_path(&self.root, data_location)?;

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

        // check if bucket exist
        let bucket_path = self.get_bucket_path(&input.bucket)?;

        if bucket_path.exists().not() {
            return Err(s3_error!(NoSuchBucket));
        }

        // check if access key is provided
        let access_key = self.access_key_from_creds(&req.credentials);
        if let Some(ak) = access_key {
            let upload_id = Uuid::new_v4().to_string();
            let bucket = input.bucket;
            let key = input.key;
            let metadata = self.metadata_to_string(input.metadata);

            self.save_multipart_upload(
                upload_id.as_str(),
                bucket.as_str(),
                key.as_str(),
                metadata.as_str(),
                ak,
            )
            .await?;

            let output = CreateMultipartUploadOutput {
                bucket: Some(bucket),
                key: Some(key),
                upload_id: Some(upload_id),
                ..Default::default()
            };

            Ok(S3Response::new(output))
        } else {
            Err(s3_error!(AccessDenied))
        }
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

        let upload_id = Uuid::parse_str(&upload_id)
            .map_err(|_| s3_error!(InvalidRequest))?
            .to_string();
        if self
            .verify_access_key_by_upload_id(req.credentials.as_ref(), &upload_id.as_str())
            .await?
            .not()
        {
            return Err(s3_error!(AccessDenied));
        }

        let file_path = self.resolve_upload_part_path(upload_id.as_str(), part_number)?;

        debug!("upload id: {:?}", upload_id);

        let mut md5_hash = <Md5 as Digest>::new();
        let stream = body.inspect_ok(|bytes| md5_hash.update(bytes.as_ref()));

        let mut file_writer = self.prepare_file_write(&file_path).await?;
        let size = copy_bytes(stream, file_writer.writer()).await?;
        file_writer.done().await?;

        let md5_sum = hex(md5_hash.finalize());

        debug!(path = %file_path.display(), ?size, %md5_sum, "write file");

        //Save to db
        self.save_multipart_upload_part(
            upload_id.as_str(),
            part_number,
            md5_sum.as_str(),
            file_path.into_os_string().to_str().unwrap(),
        )
        .await?;
        let output = UploadPartOutput {
            e_tag: Some(format!("{md5_sum}")),
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

        let upload_id_str = upload_id.as_str();
        let parts_in_db = self.get_parts_by_upload_id(upload_id_str).await?;

        if parts_in_db.len() == 0 {
            return Err(s3_error!(NoSuchUpload));
        }

        let mut parts_to_return: Vec<Part> = Vec::new();
        for part_item in parts_in_db {
            debug!("part: {:?}", part_item);
            let last_modified = to_timestamp(&part_item.last_modified);
            let part_number = part_item.part_number;
            let data_location = part_item.data_location.clone();
            let etag = part_item.md5.clone();

            let file = fs::File::open(&data_location)
                .await
                .map_err(|_| s3_error!(NoSuchUpload))?;
            let file_metadata = try_!(file.metadata().await);
            let size = try_!(i64::try_from(file_metadata.len()));
            let part = Part {
                last_modified: last_modified,
                part_number: Some(part_number),
                size: Some(size),
                e_tag: Some(etag),
                ..Default::default()
            };
            parts_to_return.push(part);
        }
        let output = ListPartsOutput {
            bucket: Some(bucket),
            key: Some(key),
            upload_id: Some(upload_id),
            parts: Some(parts_to_return),
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
            upload_id,
            ..
        } = req.input;

        let Some(_multipart_upload) = multipart_upload else {
            return Err(s3_error!(InvalidPart));
        };

        let upload_id = Uuid::parse_str(&upload_id)
            .map_err(|_| s3_error!(InvalidRequest))?
            .to_string();

        let multipart_upload = self
            .get_multipart_upload_by_upload_id(upload_id.as_str())
            .await?;

        if let Some(m) = multipart_upload {
            if self
                .verify_access_key_by_upload_id(req.credentials.as_ref(), &upload_id)
                .await?
                .not()
            {
                return Err(s3_error!(AccessDenied));
            }

            let metadata = self.metadata_from_string(m.metadata.as_str());
            let bucket = m.bucket;
            let key = m.key;

            let object_path = self.get_object_path(&bucket, &key)?;
            let mut file_writer = self.prepare_file_write(&object_path).await?;

            //get all the parts
            let parts = self.get_parts_by_upload_id(upload_id.as_str()).await?;

            for part in parts {
                let data_location = part.data_location;

                let mut reader = try_!(fs::File::open(&data_location).await);
                let size = try_!(tokio::io::copy(&mut reader, &mut file_writer.writer()).await);
                debug!(from = %data_location, tmp = %file_writer.tmp_path().display(), to = %file_writer.dest_path().display(), ?size, "write file");
                try_!(fs::remove_file(&data_location).await);
            }

            file_writer.done().await?;

            let file_size = try_!(fs::metadata(&object_path).await).len();
            let md5_sum = self.get_md5_sum(&bucket, &key).await?;

            debug!(?md5_sum, path = %object_path.display(), size = ?file_size, "file md5 sum");

            // Insert to the s3_item_detail table
            self.save_s3_item_detail(
                bucket.as_str(),
                key.as_str(),
                md5_sum.as_str(),
                &Some(metadata),
                InternalInfo::default(),
            )
            .await?;

            //finally delete the multipart upload
            self.delete_multipart_upload_by_upload_id(upload_id.as_str())
                .await?;

            let output = CompleteMultipartUploadOutput {
                bucket: Some(bucket),
                key: Some(key),
                e_tag: Some(format!("{md5_sum}")),
                ..Default::default()
            };
            Ok(S3Response::new(output))
        } else {
            Err(s3_error!(NoSuchUpload))
        }
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

        // check if bucket exist
        let bucket_path = self.get_bucket_path(&bucket)?;

        if bucket_path.exists().not() {
            return Err(s3_error!(NoSuchBucket));
        }
        let upload_id = Uuid::parse_str(&upload_id)
            .map_err(|_| s3_error!(InvalidRequest))?
            .to_string();

        if self
            .verify_access_key_by_upload_id(req.credentials.as_ref(), upload_id.as_str())
            .await?
            .not()
        {
            return Err(s3_error!(AccessDenied));
        }

        let parts = self.get_parts_by_upload_id(upload_id.as_str()).await?;

        if parts.len() <= 0 {
            return Err(s3_error!(NoSuchUpload));
        }

        for part in parts {
            let data_location = part.data_location;

            try_!(fs::remove_file(&data_location).await);
        }

        self.delete_multipart_upload_by_upload_id(upload_id.as_str())
            .await?;

        debug!(bucket = %bucket, key = %key, upload_id = %upload_id, "multipart upload aborted");

        Ok(S3Response::new(AbortMultipartUploadOutput {
            ..Default::default()
        }))
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::error::Result;
    use crate::DataStore;
    use crate::MultipartUpload;
    use crate::MultipartUploadPart;
    use crate::S3ItemDetail;
    use async_trait::async_trait;
    // use aws_credential_types::Credentials;
    use mockall::mock;
    use mockall::predicate::*;

    use s3s::auth::Credentials;
    use s3s::auth::SecretKey;
    use tempfile::tempdir;

    mock! {
        #[derive(Debug)]
        pub TestDataStore {}
        #[async_trait]
        impl DataStore for TestDataStore {
            async fn save_s3_item_detail(&self, item: &S3ItemDetail) -> Result<()>;
            async fn get_s3_item_detail(&self, bucket: &str, key: &str) -> Result<Option<S3ItemDetail>>;
            async fn get_s3_item_detail_with_filter(
                &self,
                bucket: &str,
                filter: &str,
            ) -> Result<Vec<S3ItemDetail>>;
            async fn get_all_buckets(&self) -> Result<Vec<String>>;
            async fn save_multipart_upload(&self, upload: &MultipartUpload) -> Result<()>;
            async fn save_multipart_upload_part(&self, part: &MultipartUploadPart) -> Result<()>;
            async fn get_access_key_by_upload_id(&self, upload_id: &str) -> Result<Option<String>>;
            async fn get_parts_by_upload_id(&self, upload_id: &str) -> Result<Vec<MultipartUploadPart>>;
            async fn get_multipart_upload_by_upload_id(
                &self,
                upload_id: &str,
            ) -> Result<Option<MultipartUpload>>;
            async fn delete_multipart_upload_by_upload_id(&self, upload_id: &str) -> Result<()>;
        }
    }

    #[tokio::test]
    async fn test_get_object() {
        let mut mock_ds = MockTestDataStore::new();
        mock_ds
            .expect_get_s3_item_detail()
            .with(eq("test_bucket"), eq("test_key"))
            .times(1)
            .returning(|_, _| {
                // let fixed_time = time::macros::datetime!(2025-02-09 09:48:13 UTC);
                let current_time = chrono::Utc::now();
                Ok(Some(S3ItemDetail {
                    bucket: "test_bucket".to_string(),
                    key: "test_key".to_string(),
                    e_tag: "test_etag".to_string(),
                    last_modified: current_time.naive_utc(),
                    data_location: "test_bucket/test_key".to_string(),
                    metadata: "{}".to_string(),
                    internal_info: "{}".to_string(),
                }))
            });

        let tmp_dir = tempdir().expect("tempdir created successfully");
        let root = tmp_dir.path().as_os_str();
        let backend = StorageBackend::new(root, mock_ds).expect("backend created successfully");

        let object_path = backend.get_object_path("test_bucket", "test_key").unwrap();
        tokio::fs::create_dir_all(object_path.parent().unwrap())
            .await
            .unwrap();
        tokio::fs::write(&object_path, "test content")
            .await
            .unwrap();

        let input = GetObjectInput::builder()
            .bucket("test_bucket".to_owned())
            .key("test_key".to_owned())
            .build()
            .unwrap();

        let req = S3Request::new(input);

        let result = backend.get_object(req).await.expect("get_object failed");

        assert_eq!(result.output.e_tag, Some("test_etag".to_string()));
        assert_eq!(result.output.content_length, Some(12));
    }

    #[tokio::test]
    async fn test_get_bucket_location() {
        let tmp_dir = tempdir().expect("tempdir created successfully");
        let root = tmp_dir.path().as_os_str();
        let mock_ds = MockTestDataStore::new();
        let backend = StorageBackend::new(root, mock_ds).expect("backend created successfully");

        let bucket_path = backend.get_bucket_path("test_bucket").unwrap();
        tokio::fs::create_dir_all(&bucket_path).await.unwrap();

        let input = GetBucketLocationInput::builder()
            .bucket("test_bucket".to_string())
            .build()
            .unwrap();

        let req = S3Request::new(input);

        let result = backend.get_bucket_location(req).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_head_bucket() {
        let tmp_dir = tempdir().expect("tempdir created successfully");
        let root = tmp_dir.path().as_os_str();
        let mock_ds = MockTestDataStore::new();
        let backend = StorageBackend::new(root, mock_ds).expect("backend created successfully");

        let bucket_path = backend.get_bucket_path("test_bucket").unwrap();
        tokio::fs::create_dir_all(&bucket_path).await.unwrap();

        let input = HeadBucketInput::builder()
            .bucket("test_bucket".to_string())
            .build()
            .unwrap();

        let req = S3Request::new(input);

        let result = backend.head_bucket(req).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_head_object() {
        let mut mock_ds = MockTestDataStore::new();
        mock_ds
            .expect_get_s3_item_detail()
            .with(eq("test_bucket"), eq("test_key"))
            .times(1)
            .returning(|_, _| {
                // let fixed_time = time::macros::datetime!(2025-02-09 09:48:13 UTC);
                let current_time = chrono::Utc::now();
                Ok(Some(S3ItemDetail {
                    bucket: "test_bucket".to_string(),
                    key: "test_key".to_string(),
                    e_tag: "test_etag".to_string(),
                    last_modified: current_time.naive_utc(),
                    data_location: "test_bucket/test_key".to_string(),
                    metadata: "{}".to_string(),
                    internal_info: "{}".to_string(),
                }))
            });

        let tmp_dir = tempdir().expect("tempdir created successfully");
        let root = tmp_dir.path().as_os_str();
        let backend = StorageBackend::new(root, mock_ds).expect("backend created successfully");

        let object_path = backend.get_object_path("test_bucket", "test_key").unwrap();
        tokio::fs::create_dir_all(object_path.parent().unwrap())
            .await
            .unwrap();
        tokio::fs::write(&object_path, "test content")
            .await
            .unwrap();

        let input = HeadObjectInput::builder()
            .bucket("test_bucket".to_string())
            .key("test_key".to_string())
            .build()
            .unwrap();

        let req = S3Request::new(input);

        let result = backend.head_object(req).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_list_buckets() {
        let mut mock_ds = MockTestDataStore::new();
        mock_ds
            .expect_get_all_buckets()
            .times(1)
            .returning(|| Ok(vec!["test_bucket".to_string()]));

        let tmp_dir = tempdir().expect("tempdir created successfully");
        let root = tmp_dir.path().as_os_str();
        let backend = StorageBackend::new(root, mock_ds).expect("backend created successfully");

        let bucket_path = backend.get_bucket_path("test_bucket").unwrap();
        tokio::fs::create_dir_all(&bucket_path).await.unwrap();

        let input = ListBucketsInput::builder().build().unwrap();
        let req = S3Request::new(input);

        let result = backend.list_buckets(req).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_list_objects_v2() {
        let mut mock_ds = MockTestDataStore::new();
        mock_ds
            .expect_get_s3_item_detail_with_filter()
            .with(eq("test_bucket"), eq(""))
            .times(1)
            .returning(|_, _| {
                let now = chrono::Utc::now();
                Ok(vec![S3ItemDetail {
                    bucket: "test_bucket".to_string(),
                    key: "test_key".to_string(),
                    e_tag: "test_etag".to_string(),
                    last_modified: now.naive_utc(),
                    data_location: "test_bucket/test_key".to_string(),
                    metadata: "{}".to_string(),
                    internal_info: "{}".to_string(),
                }])
            });

        let tmp_dir = tempdir().expect("tempdir created successfully");
        let root = tmp_dir.path().as_os_str();
        let backend = StorageBackend::new(root, mock_ds).expect("backend created successfully");

        let object_path = backend.get_object_path("test_bucket", "test_key").unwrap();
        tokio::fs::create_dir_all(object_path.parent().unwrap())
            .await
            .unwrap();
        tokio::fs::write(&object_path, "test content")
            .await
            .unwrap();

        let input = ListObjectsV2Input::builder()
            .bucket("test_bucket".to_string())
            .prefix(Some("".to_string()))
            .build()
            .unwrap();

        let req = S3Request::new(input);

        let result = backend.list_objects_v2(req).await.unwrap();
        assert!(result.output.contents.is_some());
        assert_eq!(result.output.contents.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn test_put_object() {
        let mut mock_ds = MockTestDataStore::new();
        mock_ds
            .expect_save_s3_item_detail()
            .times(1)
            .returning(|_| Ok(()));

        let tmp_dir = tempdir().expect("tempdir created successfully");
        let root = tmp_dir.path().as_os_str();
        let backend = StorageBackend::new(root, mock_ds).expect("backend created successfully");

        let object_path = backend.get_object_path("test_bucket", "test_key").unwrap();
        tokio::fs::create_dir_all(object_path.parent().unwrap())
            .await
            .unwrap();

        let body = create_streaming_blob(&tmp_dir).await;

        let input = PutObjectInput::builder()
            .bucket("test_bucket".to_string())
            .key("test_key".to_string())
            .body(Some(body))
            .build()
            .unwrap();

        let req = S3Request::new(input);

        let result = backend.put_object(req).await;
        assert!(result.is_ok());
    }

    async fn create_streaming_blob(tmp_dir: &tempfile::TempDir) -> StreamingBlob {
        let mut temp_file = tokio::fs::File::create(tmp_dir.path().join("temp_file.txt"))
            .await
            .unwrap();
        tokio::io::AsyncWriteExt::write_all(&mut temp_file, b"test content")
            .await
            .unwrap();
        tokio::io::AsyncWriteExt::flush(&mut temp_file)
            .await
            .unwrap();

        let temp_file = tokio::fs::File::open(tmp_dir.path().join("temp_file.txt"))
            .await
            .unwrap();
        let stream = ReaderStream::new(temp_file);
        StreamingBlob::wrap(stream)
    }

    #[tokio::test]
    async fn test_create_multipart_upload() {
        let mut mock_ds = MockTestDataStore::new();
        mock_ds
            .expect_save_multipart_upload()
            .times(1)
            .returning(|_| Ok(()));

        let tmp_dir = tempdir().expect("tempdir created successfully");
        let root = tmp_dir.path().as_os_str();
        let backend = StorageBackend::new(root, mock_ds).expect("backend created successfully");

        let bucket_path = backend.get_bucket_path("test_bucket").unwrap();
        tokio::fs::create_dir_all(&bucket_path).await.unwrap();
        let bucket_name = "test_bucket";
        let key = "test_key";

        let metadata = serde_json::from_str(r#"{"key1": "value1"}"#).ok();

        let input = CreateMultipartUploadInput::builder()
            .bucket(bucket_name.to_string())
            .key(key.to_string())
            .metadata(metadata)
            .build()
            .unwrap();

        let req = build_s3_request(input);
        // S3Request::new(input);
        // let req = S3Request::new(input);

        let result = backend.create_multipart_upload(req).await;

        assert!(result.is_ok());
    }

    fn build_s3_credentials() -> Credentials {
        let secret = SecretKey::from("secret");
        Credentials {
            access_key: "test_access".to_string(),
            secret_key: secret,
        }
    }
    fn build_s3_request<T>(input: T) -> S3Request<T> {
        let creds = build_s3_credentials();
        let mut req = S3Request::new(input);
        req.credentials = Some(creds);
        req
    }

    #[tokio::test]
    async fn test_list_parts() {
        let tmp_dir = tempdir().expect("tempdir created successfully");
        tokio::fs::write(tmp_dir.path().join("test_data_location"), "test content 1")
            .await
            .unwrap();
        tokio::fs::write(
            tmp_dir.path().join("test_data_location_2"),
            "test content 2",
        )
        .await
        .unwrap();
        let data_location1 = tmp_dir
            .path()
            .join("test_data_location")
            .display()
            .to_string();
        let data_location2 = tmp_dir
            .path()
            .join("test_data_location_2")
            .display()
            .to_string();
        let mut mock_ds = MockTestDataStore::new();
        mock_ds
            .expect_get_parts_by_upload_id()
            .with(eq("test_upload_id"))
            .times(1)
            .returning(move |_| {
                let now = chrono::Utc::now().naive_utc();

                Ok(vec![
                    MultipartUploadPart {
                        upload_id: "test_upload_id".to_string(),
                        part_number: 1,
                        md5: "test_md5".to_string(),
                        data_location: data_location1.clone(),
                        last_modified: now,
                    },
                    MultipartUploadPart {
                        upload_id: "test_upload_id".to_string(),
                        part_number: 2,
                        md5: "test_md5_2".to_string(),
                        data_location: data_location2.clone(),
                        last_modified: now,
                    },
                ])
            });

        let root = tmp_dir.path().as_os_str();
        let backend = StorageBackend::new(root, mock_ds).expect("backend created successfully");

        let input = ListPartsInput::builder()
            .bucket("test_bucket".to_string())
            .key("test_key".to_string())
            .upload_id("test_upload_id".to_string())
            .build()
            .unwrap();

        let req = S3Request::new(input);

        let result = backend.list_parts(req).await.unwrap();
        assert!(result.output.parts.is_some());
        assert_eq!(result.output.parts.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn test_complete_multipart_upload() {
        let tmp_dir = tempdir().expect("tempdir created successfully");
        tokio::fs::write(tmp_dir.path().join("test_data_location"), "test content 1")
            .await
            .unwrap();
        tokio::fs::write(
            tmp_dir.path().join("test_data_location_2"),
            "test content 2",
        )
        .await
        .unwrap();
        let data_location1 = tmp_dir
            .path()
            .join("test_data_location")
            .display()
            .to_string();
        let data_location2 = tmp_dir
            .path()
            .join("test_data_location_2")
            .display()
            .to_string();
        let upload_id = Uuid::new_v4().to_string();
        let upload_id_clone = upload_id.clone();

        let bucket_name = "test_bucket";
        let key_name = "test_key";
        let content = "test_content";

        let mut mock_ds = MockTestDataStore::new();
        mock_ds
            .expect_get_multipart_upload_by_upload_id()
            .times(1)
            .returning(move |_| {
                Ok(Some(MultipartUpload {
                    upload_id: upload_id_clone.to_string(),
                    bucket: bucket_name.to_string(),
                    key: key_name.to_string(),
                    metadata: "{}".to_string(),
                    access_key: "test_access".to_string(),
                    last_modified: chrono::Utc::now().naive_utc(),
                }))
            });

        mock_ds
            .expect_get_access_key_by_upload_id()
            .times(1)
            .returning(|_| Ok(Some("test_access".to_string())));

        let upload_id_clone = upload_id.clone();
        mock_ds
            .expect_get_parts_by_upload_id()
            .with(eq(upload_id_clone.clone()))
            .times(1)
            .returning(move |_| {
                let now = chrono::Utc::now().naive_utc();

                Ok(vec![
                    MultipartUploadPart {
                        upload_id: upload_id_clone.to_string(),
                        part_number: 1,
                        md5: "test_md5".to_string(),
                        data_location: data_location1.clone(),
                        last_modified: now,
                    },
                    MultipartUploadPart {
                        upload_id: upload_id_clone.to_string(),
                        part_number: 2,
                        md5: "test_md5_2".to_string(),
                        data_location: data_location2.clone(),
                        last_modified: now,
                    },
                ])
            });

        mock_ds
            .expect_save_s3_item_detail()
            .times(1)
            .returning(|_| Ok(()));

        mock_ds
            .expect_delete_multipart_upload_by_upload_id()
            .times(1)
            .returning(|_| Ok(()));

        let root = tmp_dir.path().as_os_str();
        let backend = StorageBackend::new(root, mock_ds).expect("backend created successfully");

        let md5sum = format!("{:?}", md5::Md5::digest(content.as_bytes()));
        let field = Some(CompletedMultipartUpload {
            parts: Some(vec![CompletedPart {
                e_tag: Some(md5sum.to_string()),
                part_number: Some(1),
                ..Default::default()
            }]),
        });
        let input = CompleteMultipartUploadInput::builder()
            .bucket(bucket_name.to_string())
            .key(key_name.to_string())
            .upload_id(upload_id.to_string())
            .multipart_upload(field)
            .build()
            .unwrap();
        let bucket_path = backend.get_bucket_path(bucket_name).unwrap();
        tokio::fs::create_dir_all(&bucket_path).await.unwrap();

        let object_path = backend.get_object_path(bucket_name, key_name).unwrap();
        tokio::fs::create_dir_all(object_path.parent().unwrap())
            .await
            .unwrap();
        tokio::fs::write(&object_path, content).await.unwrap();
        let req = build_s3_request(input);

        let result = backend.complete_multipart_upload(req).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_abort_multipart_upload() {
        let mut mock_ds = MockTestDataStore::new();
        mock_ds
            .expect_get_parts_by_upload_id()
            .times(1)
            .returning(|_| Ok(vec![]));

        mock_ds
            .expect_get_access_key_by_upload_id()
            .times(1)
            .returning(|_| Ok(Some("test_access".to_string())));

        let tmp_dir = tempdir().expect("tempdir created successfully");
        let root = tmp_dir.path().as_os_str();
        let backend = StorageBackend::new(root, mock_ds).expect("backend created successfully");

        let bucket_name = "test_bucket";
        let key_name = "test_key";
        let upload_id = Uuid::new_v4().to_string();
        let content = "test_content";

        let input = AbortMultipartUploadInput::builder()
            .bucket(bucket_name.to_string())
            .key(key_name.to_string())
            .upload_id(upload_id.to_string())
            .build()
            .unwrap();

        let bucket_path = backend.get_bucket_path(bucket_name).unwrap();
        tokio::fs::create_dir_all(&bucket_path).await.unwrap();

        let object_path = backend.get_object_path(bucket_name, key_name).unwrap();
        tokio::fs::create_dir_all(object_path.parent().unwrap())
            .await
            .unwrap();
        tokio::fs::write(&object_path, content).await.unwrap();
        let req = build_s3_request(input);

        let result = backend.abort_multipart_upload(req).await;
        assert!(result.is_err());
    }
}
