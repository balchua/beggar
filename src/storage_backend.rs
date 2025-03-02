use std::{
    env,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use md5::{Digest, Md5};
use s3s::{
    S3Result,
    auth::Credentials,
    dto::{self, PartNumber},
    s3_error,
};
use tokio::{
    fs,
    fs::File,
    io::{AsyncReadExt, BufWriter},
};
use tracing::{debug, info, warn};

use crate::{
    DataStore, MultipartUpload, MultipartUploadPart, S3ItemDetail,
    error::*,
    utils::{self, hex, resolve_abs_path},
};

#[derive(Debug)]
pub struct StorageBackend<T: DataStore> {
    pub(crate) root: PathBuf,
    tmp_file_counter: AtomicU64,
    pub datastore: T,
}

pub(crate) type InternalInfo = serde_json::Map<String, serde_json::Value>;

fn clean_old_tmp_files(root: &Path) -> std::io::Result<()> {
    let entries = match std::fs::read_dir(root) {
        Ok(entries) => Ok(entries),
        Err(ref io_err) if io_err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(io_err) => Err(io_err),
    }?;
    for entry in entries {
        let entry = entry?;
        let file_name = entry.file_name();
        let Some(file_name) = file_name.to_str() else {
            continue;
        };
        // See `FileSystem::prepare_file_write`
        if file_name.starts_with(".tmp.") && file_name.ends_with(".internal.part") {
            std::fs::remove_file(entry.path())?;
        }
    }
    Ok(())
}

impl<T: DataStore> StorageBackend<T> {
    pub fn new(root: impl AsRef<Path>, datastore: T) -> Result<Self> {
        let root = env::current_dir()?.join(root).canonicalize()?;
        clean_old_tmp_files(&root)?;
        let tmp_file_counter = AtomicU64::new(0);
        Ok(Self {
            root,
            tmp_file_counter,
            datastore,
        })
    }

    /// Validates an S3 key according to S3 specifications
    ///
    /// S3 key validation rules:
    /// - Must not be empty
    /// - Must be 1-1024 bytes in length when UTF-8 encoded
    /// - Must not contain control characters or certain special characters
    ///
    /// This should be used before any operations that store or retrieve data
    /// based on S3 keys to prevent path traversal and other security issues.
    pub fn validate_s3_key(&self, key: &str) -> bool {
        // Basic length validation
        if key.is_empty() || key.len() > 1024 {
            warn!(key = %key, "S3 key failed length validation");
            return false;
        }

        // Look for unsafe characters - notably "../" path traversal attempts
        if key.contains("../") || key.contains("./") || key.contains("//") {
            warn!(key = %key, "S3 key contains potentially unsafe path sequences");
            return false;
        }

        // Reject keys with control characters
        if key.chars().any(char::is_control) {
            warn!(key = %key, "S3 key contains control characters");
            return false;
        }

        // Additional S3 specific validation could be added here

        debug!(key = %key, "S3 key passed validation");
        true
    }

    pub(crate) fn resolve_upload_part_path(
        &self,
        upload_id: &str,
        part_number: PartNumber,
    ) -> Result<PathBuf> {
        resolve_abs_path(
            &self.root,
            format!(".upload_id-{upload_id}.part-{part_number}"),
        )
    }

    /// resolve object path under the virtual root
    pub(crate) fn get_object_path(&self, bucket: &str, key: &str) -> Result<PathBuf> {
        let dir = Path::new(&bucket);
        let file_path = Path::new(&key);
        resolve_abs_path(&self.root, dir.join(file_path))
    }

    /// resolve bucket path under the virtual root
    pub(crate) fn get_bucket_path(&self, bucket: &str) -> Result<PathBuf> {
        let dir = Path::new(&bucket);
        resolve_abs_path(&self.root, dir)
    }

    /// get md5 sum
    pub(crate) async fn get_md5_sum(&self, bucket: &str, key: &str) -> Result<String> {
        let object_path = self.get_object_path(bucket, key)?;
        let mut file = File::open(&object_path).await?;
        let mut buf = vec![0; 65536];
        let mut md5_hash = Md5::new();
        loop {
            let nread = file.read(&mut buf).await?;
            if nread == 0 {
                break;
            }
            md5_hash.update(&buf[..nread]);
        }
        Ok(hex(md5_hash.finalize()))
    }

    /// verify access key by upload id taken from the database
    pub(crate) async fn verify_access_key_by_upload_id(
        &self,
        cred: Option<&Credentials>,
        upload_id: &str,
    ) -> Result<bool> {
        let access_key = self.get_access_key_by_upload_id(upload_id).await?;

        if let Some(ak) = access_key {
            Ok(ak == cred.map(|c| c.access_key.as_str()).unwrap_or_default())
        } else {
            Ok(false)
        }
    }

    /// Write to the filesystem atomically.
    /// This is done by first writing to a temporary location and then moving
    /// the file.
    pub(crate) async fn prepare_file_write<'a>(&self, path: &'a Path) -> Result<FileWriter<'a>> {
        let tmp_name = format!(
            ".tmp.{}.internal.part",
            self.tmp_file_counter.fetch_add(1, Ordering::SeqCst)
        );
        let tmp_path = resolve_abs_path(&self.root, tmp_name)?;
        let file = File::create(&tmp_path).await?;
        let writer = BufWriter::new(file);
        Ok(FileWriter {
            tmp_path,
            dest_path: path,
            writer,
            clean_tmp: true,
        })
    }

    pub(crate) async fn handle_directory_creation(
        &self,
        content_length: Option<i64>,
        bucket: &str,
        key: &str,
    ) -> S3Result<()> {
        if content_length.is_some_and(|len| len > 0) {
            info!("content_length is greater than 0");
            return Err(s3_error!(
                UnexpectedContent,
                "Unexpected request body when creating a directory object."
            ));
        }
        let object_path = self.get_object_path(bucket, key)?;
        try_!(fs::create_dir_all(&object_path).await);
        Ok(())
    }

    pub(crate) async fn save_s3_item_detail(
        &self,
        bucket: &str,
        key: &str,
        e_tag: &str,
        metadata: Option<&dto::Metadata>,
        internal_info: InternalInfo,
    ) -> Result<()> {
        // Validate the key before saving
        if !self.validate_s3_key(key) {
            return Err(Error::from_string("Invalid S3 key format"));
        }

        let internal_info_str = serde_json::to_string(&internal_info)?;
        let metadata_str = utils::metadata_to_string(metadata);
        let path = bucket.to_string() + "/" + key;

        let item = S3ItemDetail::builder()
            .bucket(bucket.to_string())
            .key(key.to_string())
            .e_tag(e_tag.to_string())
            .metadata(Some(metadata_str))
            .internal_info(Some(internal_info_str))
            .data_location(path)
            .build();
        self.datastore.save_s3_item_detail(&item).await
    }

    pub(crate) async fn get_s3_item_detail(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<Option<S3ItemDetail>> {
        self.datastore.get_s3_item_detail(bucket, key).await
    }

    pub(crate) async fn get_s3_item_detail_with_filter(
        &self,
        bucket: &str,
        filter: &str,
    ) -> Result<Vec<S3ItemDetail>> {
        self.datastore
            .get_s3_item_detail_with_filter(bucket, filter)
            .await
    }

    pub(crate) async fn get_all_buckets(&self) -> Result<Vec<String>> {
        self.datastore.get_all_buckets().await
    }

    pub(crate) async fn save_multipart_upload(
        &self,
        upload_id: &str,
        bucket: &str,
        key: &str,
        metadata: &str,
        access_key: &str,
    ) -> Result<()> {
        let upload = MultipartUpload::builder()
            .upload_id(upload_id.to_string())
            .bucket(bucket.to_string())
            .key(key.to_string())
            .metadata(metadata.to_string())
            .access_key(access_key.to_string())
            .build();
        self.datastore.save_multipart_upload(&upload).await
    }

    pub(crate) async fn save_multipart_upload_part(
        &self,
        upload_id: &str,
        part_number: i32,
        md5: &str,
        data_location: &str,
    ) -> Result<()> {
        let part = MultipartUploadPart::builder()
            .upload_id(upload_id.to_string())
            .part_number(part_number)
            .md5(md5.to_string())
            .data_location(data_location.to_string())
            .build();
        self.datastore.save_multipart_upload_part(&part).await
    }

    pub(crate) async fn get_access_key_by_upload_id(
        &self,
        upload_id: &str,
    ) -> Result<Option<String>> {
        self.datastore.get_access_key_by_upload_id(upload_id).await
    }

    pub(crate) async fn get_parts_by_upload_id(
        &self,
        upload_id: &str,
    ) -> Result<Vec<MultipartUploadPart>> {
        self.datastore.get_parts_by_upload_id(upload_id).await
    }

    pub(crate) async fn get_multipart_upload_by_upload_id(
        &self,
        upload_id: &str,
    ) -> Result<Option<MultipartUpload>> {
        self.datastore
            .get_multipart_upload_by_upload_id(upload_id)
            .await
    }

    pub(crate) async fn delete_multipart_upload_by_upload_id(&self, upload_id: &str) -> Result<()> {
        self.datastore
            .delete_multipart_upload_by_upload_id(upload_id)
            .await
    }
}

pub(crate) struct FileWriter<'a> {
    tmp_path: PathBuf,
    dest_path: &'a Path,
    writer: BufWriter<File>,
    clean_tmp: bool,
}

impl<'a> FileWriter<'a> {
    pub(crate) fn tmp_path(&self) -> &Path {
        &self.tmp_path
    }

    pub(crate) fn dest_path(&self) -> &'a Path {
        self.dest_path
    }

    pub(crate) fn writer(&mut self) -> &mut BufWriter<File> {
        &mut self.writer
    }

    pub(crate) async fn done(mut self) -> Result<()> {
        if let Some(final_dir_path) = self.dest_path().parent() {
            fs::create_dir_all(&final_dir_path).await?;
        }

        if !self.dest_path().is_dir() {
            fs::rename(&self.tmp_path, self.dest_path()).await?;
        }

        self.clean_tmp = false;
        Ok(())
    }
}

impl Drop for FileWriter<'_> {
    fn drop(&mut self) {
        if self.clean_tmp {
            let _ = std::fs::remove_file(&self.tmp_path);
        }
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use mockall::{mock, predicate::*};
    use s3s::{
        auth::{Credentials, SecretKey},
        dto::{self, Checksum},
    };
    use tempfile::tempdir;
    use uuid::Uuid;

    use super::*;
    use crate::{MultipartUpload, MultipartUploadPart};

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
    async fn test_save_s3_item_detail() {
        let mut mock_ds = MockTestDataStore::new();
        mock_ds
            .expect_get_s3_item_detail()
            .times(1)
            .returning(|_, _| Ok(None));

        // Create a directory inside of `env::temp_dir()`
        let tmp_dir = tempdir().expect("tempdir created successfully");
        let root = tmp_dir.path().as_os_str();
        let backend = StorageBackend::new(root, mock_ds).expect("backend created successfully");

        let result = backend.get_s3_item_detail("test_bucket", "test_key").await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
    }

    #[test]
    fn test_resolve_upload_part_path() {
        // initialize the temp directory
        // Create a directory inside of `env::temp_dir()`
        let tmp_dir = tempdir().expect("tempdir created successfully");
        let root = tmp_dir.path().as_os_str();
        let mock_ds = MockTestDataStore::new();
        let backend = StorageBackend::new(root, mock_ds).expect("backend created successfully");

        let upload_id = Uuid::new_v4().to_string();
        let part_number = 1;
        let path = backend
            .resolve_upload_part_path(upload_id.as_str(), part_number)
            .unwrap();
        let expected_path = tmp_dir
            .path()
            .join(format!(".upload_id-{upload_id}.part-{part_number}"));
        assert_eq!(path, expected_path);
    }

    #[test]
    fn test_get_object_path() {
        // initialize the temp directory
        // Create a directory inside of `env::temp_dir()`
        let tmp_dir = tempdir().expect("tempdir created successfully");
        let root = tmp_dir.path().as_os_str();
        let mock_ds = MockTestDataStore::new();
        let backend = StorageBackend::new(root, mock_ds).expect("backend created successfully");

        let bucket = "test_bucket";
        let key = "test_key.txt";
        let path = backend.get_object_path(bucket, key).unwrap();
        let expected_path = tmp_dir.path().join(bucket).join(key);
        assert_eq!(path, expected_path);
    }

    #[test]
    fn test_get_bucket_path() {
        // initialize the temp directory
        // Create a directory inside of `env::temp_dir()`
        let tmp_dir = tempdir().expect("tempdir created successfully");
        let root = tmp_dir.path().as_os_str();
        let mock_ds = MockTestDataStore::new();
        let backend = StorageBackend::new(root, mock_ds).expect("backend created successfully");

        let bucket = "test_bucket";
        let path = backend.get_bucket_path(bucket).unwrap();
        let expected_path = tmp_dir.path().join(bucket);
        assert_eq!(path, expected_path);
    }

    #[tokio::test]
    async fn test_delete_multipart_upload_by_upload_id() {
        let tmp_dir = tempdir().expect("tempdir created successfully");
        let root = tmp_dir.path().as_os_str();
        let mut mock_ds = MockTestDataStore::new();
        mock_ds
            .expect_delete_multipart_upload_by_upload_id()
            .times(1)
            .returning(|_| Ok(()));
        let backend = StorageBackend::new(root, mock_ds).expect("backend created successfully");

        let upload_id = "test_upload_id";

        // Call the delete function
        let result = backend
            .delete_multipart_upload_by_upload_id(upload_id)
            .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_get_multipart_upload_by_upload_id() {
        let mut mock_ds = MockTestDataStore::new();
        mock_ds
            .expect_get_multipart_upload_by_upload_id()
            .times(1)
            .returning(|_| Ok(None));

        // Create a directory inside of `env::temp_dir()`
        let tmp_dir = tempdir().expect("tempdir created successfully");
        let root = tmp_dir.path().as_os_str();
        let backend = StorageBackend::new(root, mock_ds).expect("backend created successfully");

        let result = backend
            .get_multipart_upload_by_upload_id("test_upload_id")
            .await;
        assert!(result.is_ok());
    }

    #[test]
    fn test_metadata_to_string() {
        let tmp_dir = tempdir().expect("tempdir created successfully");
        let root = tmp_dir.path().as_os_str();
        let mock_ds = MockTestDataStore::new();
        let _backend = StorageBackend::new(root, mock_ds).expect("backend created successfully");

        let metadata_string = "{\"hello\":\"world\"}";
        let metadata = utils::metadata_from_string(metadata_string);
        let expected_metadata: dto::Metadata =
            serde_json::from_str(r#"{"hello": "world"}"#).unwrap();
        assert_eq!(metadata, expected_metadata);

        let metadata_string = "";
        let metadata = utils::metadata_from_string(metadata_string);
        assert_eq!(metadata, dto::Metadata::new());
    }

    #[test]
    fn test_access_key_from_creds() {
        let tmp_dir = tempdir().expect("tempdir created successfully");
        let root = tmp_dir.path().as_os_str();
        let mock_ds = MockTestDataStore::new();
        let _backend = StorageBackend::new(root, mock_ds).expect("backend created successfully");

        let creds = Some(Credentials {
            access_key: "test_access_key".to_string(),
            secret_key: SecretKey::from("test_secret_key"),
        });
        let access_key = utils::access_key_from_creds(creds.as_ref());
        assert_eq!(access_key, Some("test_access_key"));

        let access_key = utils::access_key_from_creds(None);
        assert_eq!(access_key, None);
    }

    #[tokio::test]
    async fn test_get_md5_sum() {
        // initialize the temp directory
        // Create a directory inside of `env::temp_dir()`
        let tmp_dir = tempdir().expect("tempdir created successfully");
        let root = tmp_dir.path().as_os_str();
        let mock_ds = MockTestDataStore::new();
        let backend = StorageBackend::new(root, mock_ds).expect("backend created successfully");

        let bucket = "test_bucket";
        let key = "test_key.txt";
        let file_path = backend.get_object_path(bucket, key).unwrap();
        tokio::fs::create_dir_all(file_path.parent().unwrap())
            .await
            .unwrap();
        tokio::fs::write(&file_path, "test content").await.unwrap();

        // generate the md5sum from this link https://www.md5hashgenerator.com/
        let expected_md5_sum = "9473fdd0d880a43c21b7778d34872157";
        let md5_sum = backend.get_md5_sum(bucket, key).await.unwrap();

        assert_eq!(md5_sum, expected_md5_sum);
    }

    #[tokio::test]
    async fn test_verify_access_key_by_upload_id() {
        let mut mock_ds = MockTestDataStore::new();
        mock_ds
            .expect_get_access_key_by_upload_id()
            .times(2)
            .returning(|_| Ok(Some("test_access_key".to_string())));

        let tmp_dir = tempdir().expect("tempdir created successfully");
        let root = tmp_dir.path().as_os_str();
        let backend = StorageBackend::new(root, mock_ds).expect("backend created successfully");

        let creds = Some(Credentials {
            access_key: "test_access_key".to_string(),
            secret_key: SecretKey::from("test_secret_key"),
        });
        let upload_id = "test_upload_id";

        let result = backend
            .verify_access_key_by_upload_id(creds.as_ref(), upload_id)
            .await
            .unwrap();
        assert!(result);

        let creds = Some(Credentials {
            access_key: "wrong_access_key".to_string(),
            secret_key: SecretKey::from("test_secret_key"),
        });
        let result = backend
            .verify_access_key_by_upload_id(creds.as_ref(), upload_id)
            .await
            .unwrap();
        assert!(!result);
    }

    #[test]
    fn test_init_checksum_hasher() {
        let tmp_dir = tempdir().expect("tempdir created successfully");
        let root = tmp_dir.path().as_os_str();
        let mock_ds = MockTestDataStore::new();
        let _backend = StorageBackend::new(root, mock_ds).expect("backend created successfully");

        let crc32 = Some("crc32".to_string());
        let crc32c = Some("crc32c".to_string());
        let sha1 = Some("sha1".to_string());
        let sha256 = Some("sha256".to_string());

        let checksum_hasher = utils::init_checksum_hasher(
            crc32.as_ref(),
            crc32c.as_ref(),
            sha1.as_ref(),
            sha256.as_ref(),
        );

        assert!(checksum_hasher.crc32.is_some());
        assert!(checksum_hasher.crc32c.is_some());
        assert!(checksum_hasher.sha1.is_some());
        assert!(checksum_hasher.sha256.is_some());

        let checksum_hasher = utils::init_checksum_hasher(None, None, None, None);

        assert!(checksum_hasher.crc32.is_none());
        assert!(checksum_hasher.crc32c.is_none());
        assert!(checksum_hasher.sha1.is_none());
        assert!(checksum_hasher.sha256.is_none());
    }

    #[test]
    fn test_validate_checksums() {
        let tmp_dir = tempdir().expect("tempdir created successfully");
        let root = tmp_dir.path().as_os_str();
        let mock_ds = MockTestDataStore::new();
        let _backend = StorageBackend::new(root, mock_ds).expect("backend created successfully");

        let crc32 = Some("crc32".to_string());
        let crc32c = Some("crc32c".to_string());
        let sha1 = Some("sha1".to_string());
        let sha256 = Some("sha256".to_string());

        let checksum = Checksum {
            checksum_crc32: Some("crc32".to_string()),
            checksum_crc32c: Some("crc32c".to_string()),
            checksum_sha1: Some("sha1".to_string()),
            checksum_sha256: Some("sha256".to_string()),
        };

        let result = utils::validate_checksums(
            &checksum,
            crc32.as_ref(),
            crc32c.as_ref(),
            sha1.as_ref(),
            sha256.as_ref(),
        );
        assert!(result.is_ok());

        // Test case: checksum_crc32 mismatch
        let checksum = Checksum {
            checksum_crc32: Some("wrong_crc32".to_string()),
            checksum_crc32c: Some("crc32c".to_string()),
            checksum_sha1: Some("sha1".to_string()),
            checksum_sha256: Some("sha256".to_string()),
        };
        let result = utils::validate_checksums(
            &checksum,
            crc32.as_ref(),
            crc32c.as_ref(),
            sha1.as_ref(),
            sha256.as_ref(),
        );
        // let result = utils::validate_checksums(&checksum, &crc32, &crc32c, &sha1,
        // &sha256);
        assert!(result.is_err());

        // Test case: checksum_crc32c mismatch
        let checksum = Checksum {
            checksum_crc32: Some("crc32".to_string()),
            checksum_crc32c: Some("wrong_crc32c".to_string()),
            checksum_sha1: Some("sha1".to_string()),
            checksum_sha256: Some("sha256".to_string()),
        };
        let result = utils::validate_checksums(
            &checksum,
            crc32.as_ref(),
            crc32c.as_ref(),
            sha1.as_ref(),
            sha256.as_ref(),
        );
        assert!(result.is_err());

        // Test case: checksum_sha1 mismatch
        let checksum = Checksum {
            checksum_crc32: Some("crc32".to_string()),
            checksum_crc32c: Some("crc32c".to_string()),
            checksum_sha1: Some("wrong_sha1".to_string()),
            checksum_sha256: Some("sha256".to_string()),
        };
        let result = utils::validate_checksums(
            &checksum,
            crc32.as_ref(),
            crc32c.as_ref(),
            sha1.as_ref(),
            sha256.as_ref(),
        );
        assert!(result.is_err());

        // Test case: checksum_sha256 mismatch
        let checksum = Checksum {
            checksum_crc32: Some("crc32".to_string()),
            checksum_crc32c: Some("crc32c".to_string()),
            checksum_sha1: Some("sha1".to_string()),
            checksum_sha256: Some("wrong_sha256".to_string()),
        };
        let result = utils::validate_checksums(
            &checksum,
            crc32.as_ref(),
            crc32c.as_ref(),
            sha1.as_ref(),
            sha256.as_ref(),
        );
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_handle_directory_creation() {
        let tmp_dir = tempdir().expect("tempdir created successfully");
        let root = tmp_dir.path().as_os_str();
        let mock_ds = MockTestDataStore::new();
        let backend = StorageBackend::new(root, mock_ds).expect("backend created successfully");

        let bucket = "test_bucket";
        let key = "test_key";

        let result = backend.handle_directory_creation(None, bucket, key).await;
        assert!(result.is_ok());

        let result = backend
            .handle_directory_creation(Some(1), bucket, key)
            .await;
        assert!(result.is_err());
        let result = backend
            .handle_directory_creation(Some(0), bucket, key)
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_save_multipart_upload() {
        let mut mock_ds = MockTestDataStore::new();
        mock_ds
            .expect_save_multipart_upload()
            .times(1)
            .returning(|_| Ok(()));

        let tmp_dir = tempdir().expect("tempdir created successfully");
        let root = tmp_dir.path().as_os_str();
        let backend = StorageBackend::new(root, mock_ds).expect("backend created successfully");

        let upload = MultipartUpload::builder()
            .upload_id("test_upload_id".to_string())
            .bucket("test_bucket".to_string())
            .key("test_key".to_string())
            .metadata("test_metadata".to_string())
            .access_key("test_access_key".to_string())
            .build();

        let result = backend
            .save_multipart_upload(
                &upload.upload_id,
                &upload.bucket,
                &upload.key,
                &upload.metadata,
                &upload.access_key,
            )
            .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_create_multipart_upload_part() {
        let mut mock_ds = MockTestDataStore::new();
        mock_ds
            .expect_save_multipart_upload_part()
            .times(1)
            .returning(|_| Ok(()));

        let tmp_dir = tempdir().expect("tempdir created successfully");
        let root = tmp_dir.path().as_os_str();
        let backend = StorageBackend::new(root, mock_ds).expect("backend created successfully");

        let upload_id = "test_upload_id";
        let part_number = 1;
        let md5 = "test_md5";
        let data_location = "test_data_location";

        let result = backend
            .save_multipart_upload_part(upload_id, part_number, md5, data_location)
            .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_get_access_key_by_upload_id() {
        let mut mock_ds = MockTestDataStore::new();
        mock_ds
            .expect_get_access_key_by_upload_id()
            .times(1)
            .returning(|_| Ok(Some("test_access_key".to_string())));

        let tmp_dir = tempdir().expect("tempdir created successfully");
        let root = tmp_dir.path().as_os_str();
        let backend = StorageBackend::new(root, mock_ds).expect("backend created successfully");

        let upload_id = "test_upload_id";

        let result = backend.get_access_key_by_upload_id(upload_id).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some("test_access_key".to_string()));
    }

    #[tokio::test]
    async fn test_get_parts_by_upload_id() {
        let mut mock_ds = MockTestDataStore::new();
        mock_ds
            .expect_get_parts_by_upload_id()
            .times(1)
            .returning(|_| Ok(vec![]));

        let tmp_dir = tempdir().expect("tempdir created successfully");
        let root = tmp_dir.path().as_os_str();
        let backend = StorageBackend::new(root, mock_ds).expect("backend created successfully");

        let upload_id = "test_upload_id";

        let result = backend.get_parts_by_upload_id(upload_id).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), vec![]);
    }

    #[tokio::test]
    async fn test_get_s3_item_detail_with_filter() {
        let mut mock_ds = MockTestDataStore::new();
        mock_ds
            .expect_get_s3_item_detail_with_filter()
            .times(1)
            .returning(|_, _| Ok(vec![]));

        let tmp_dir = tempdir().expect("tempdir created successfully");
        let root = tmp_dir.path().as_os_str();
        let backend = StorageBackend::new(root, mock_ds).expect("backend created successfully");

        let bucket = "test_bucket";
        let filter = "test_filter";

        let result = backend.get_s3_item_detail_with_filter(bucket, filter).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), vec![]);
    }
}
