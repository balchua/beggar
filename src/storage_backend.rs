use crate::utils::hex;
use crate::{error::*, DataStore, S3ItemDetail};

use s3s::auth::Credentials;
use s3s::dto::{self, Checksum, Timestamp, TimestampFormat};
use s3s::{s3_error, S3Result};
use stdx::default::default;

use std::env;
use std::ops::Not;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::fs;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, BufWriter};

use md5::{Digest, Md5};
use path_absolutize::Absolutize;
use s3s::dto::PartNumber;
use uuid::Uuid;

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

    pub(crate) fn resolve_abs_path(&self, path: impl AsRef<Path>) -> Result<PathBuf> {
        Ok(path.as_ref().absolutize_virtually(&self.root)?.into_owned())
    }

    pub(crate) fn resolve_upload_part_path(
        &self,
        upload_id: Uuid,
        part_number: PartNumber,
    ) -> Result<PathBuf> {
        self.resolve_abs_path(format!(".upload_id-{upload_id}.part-{part_number}"))
    }

    /// resolve object path under the virtual root
    pub(crate) fn get_object_path(&self, bucket: &str, key: &str) -> Result<PathBuf> {
        let dir = Path::new(&bucket);
        let file_path = Path::new(&key);
        self.resolve_abs_path(dir.join(file_path))
    }

    /// resolve bucket path under the virtual root
    pub(crate) fn get_bucket_path(&self, bucket: &str) -> Result<PathBuf> {
        let dir = Path::new(&bucket);
        self.resolve_abs_path(dir)
    }

    /// resolve metadata path under the virtual root (custom format)
    pub(crate) fn get_metadata_path(
        &self,
        bucket: &str,
        key: &str,
        upload_id: Option<Uuid>,
    ) -> Result<PathBuf> {
        let encode = |s: &str| base64_simd::URL_SAFE_NO_PAD.encode_to_string(s);
        let u_ext = upload_id
            .map(|u| format!(".upload-{u}"))
            .unwrap_or_default();
        let file_path = format!(
            ".bucket-{}.object-{}{u_ext}.metadata.json",
            encode(bucket),
            encode(key)
        );
        self.resolve_abs_path(file_path)
    }

    // pub(crate) fn get_internal_info_path(&self, bucket: &str, key: &str) -> Result<PathBuf> {
    //     let encode = |s: &str| base64_simd::URL_SAFE_NO_PAD.encode_to_string(s);
    //     let file_path = format!(
    //         ".bucket-{}.object-{}.internal.json",
    //         encode(bucket),
    //         encode(key)
    //     );
    //     self.resolve_abs_path(file_path)
    // }

    /// load metadata from fs
    pub(crate) async fn load_metadata(
        &self,
        bucket: &str,
        key: &str,
        upload_id: Option<Uuid>,
    ) -> Result<Option<dto::Metadata>> {
        let path = self.get_metadata_path(bucket, key, upload_id)?;
        if path.exists().not() {
            return Ok(None);
        }
        let content = fs::read(&path).await?;
        let map = serde_json::from_slice(&content)?;
        Ok(Some(map))
    }

    /// save metadata to fs
    pub(crate) async fn save_metadata(
        &self,
        bucket: &str,
        key: &str,
        metadata: &dto::Metadata,
        upload_id: Option<Uuid>,
    ) -> Result<()> {
        let path = self.get_metadata_path(bucket, key, upload_id)?;
        let content = serde_json::to_vec(metadata)?;
        fs::write(&path, &content).await?;
        Ok(())
    }

    /// remove metadata from fs
    pub(crate) fn delete_metadata(
        &self,
        bucket: &str,
        key: &str,
        upload_id: Option<Uuid>,
    ) -> Result<()> {
        let path = self.get_metadata_path(bucket, key, upload_id)?;
        std::fs::remove_file(path)?;
        Ok(())
    }

    // pub(crate) async fn load_internal_info(
    //     &self,
    //     bucket: &str,
    //     key: &str,
    // ) -> Result<Option<InternalInfo>> {
    //     let path = self.get_internal_info_path(bucket, key)?;
    //     if path.exists().not() {
    //         return Ok(None);
    //     }
    //     let content = fs::read(&path).await?;
    //     let map = serde_json::from_slice(&content)?;
    //     Ok(Some(map))
    // }

    // pub(crate) async fn save_internal_info(
    //     &self,
    //     bucket: &str,
    //     key: &str,
    //     info: &InternalInfo,
    // ) -> Result<()> {
    //     let path = self.get_internal_info_path(bucket, key)?;
    //     let content = serde_json::to_vec(info)?;
    //     fs::write(&path, &content).await?;
    //     Ok(())
    // }

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

    fn get_upload_info_path(&self, upload_id: &Uuid) -> Result<PathBuf> {
        self.resolve_abs_path(format!(".upload-{upload_id}.json"))
    }

    pub(crate) async fn create_upload_id(&self, cred: Option<&Credentials>) -> Result<Uuid> {
        let upload_id = Uuid::new_v4();
        let upload_info_path = self.get_upload_info_path(&upload_id)?;

        let ak: Option<&str> = cred.map(|c| c.access_key.as_str());

        let content = serde_json::to_vec(&ak)?;
        fs::write(&upload_info_path, &content).await?;

        Ok(upload_id)
    }

    pub(crate) async fn verify_upload_id(
        &self,
        cred: Option<&Credentials>,
        upload_id: &Uuid,
    ) -> Result<bool> {
        let upload_info_path = self.get_upload_info_path(upload_id)?;
        if upload_info_path.exists().not() {
            return Ok(false);
        }

        let content = fs::read(&upload_info_path).await?;
        let ak: Option<String> = serde_json::from_slice(&content)?;

        Ok(ak.as_deref() == cred.map(|c| c.access_key.as_str()))
    }

    pub(crate) async fn delete_upload_id(&self, upload_id: &Uuid) -> Result<()> {
        let upload_info_path = self.get_upload_info_path(upload_id)?;
        if upload_info_path.exists() {
            fs::remove_file(&upload_info_path).await?;
        }
        Ok(())
    }

    /// Write to the filesystem atomically.
    /// This is done by first writing to a temporary location and then moving the file.
    pub(crate) async fn prepare_file_write<'a>(&self, path: &'a Path) -> Result<FileWriter<'a>> {
        let tmp_name = format!(
            ".tmp.{}.internal.part",
            self.tmp_file_counter.fetch_add(1, Ordering::SeqCst)
        );
        let tmp_path = self.resolve_abs_path(tmp_name)?;
        let file = File::create(&tmp_path).await?;
        let writer = BufWriter::new(file);
        Ok(FileWriter {
            tmp_path,
            dest_path: path,
            writer,
            clean_tmp: true,
        })
    }

    pub(crate) fn init_checksum_hasher(
        &self,
        crc32: &Option<String>,
        crc32c: &Option<String>,
        sha1: &Option<String>,
        sha256: &Option<String>,
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
        checksum
    }

    pub(crate) fn validate_checksums(
        &self,
        checksum: &Checksum,
        crc32: &Option<String>,
        crc32c: &Option<String>,
        sha1: &Option<String>,
        sha256: &Option<String>,
    ) -> S3Result<()> {
        if checksum.checksum_crc32 != *crc32 {
            return Err(s3_error!(BadDigest, "checksum_crc32 mismatch"));
        }
        if checksum.checksum_crc32c != *crc32c {
            return Err(s3_error!(BadDigest, "checksum_crc32c mismatch"));
        }
        if checksum.checksum_sha1 != *sha1 {
            return Err(s3_error!(BadDigest, "checksum_sha1 mismatch"));
        }
        if checksum.checksum_sha256 != *sha256 {
            return Err(s3_error!(BadDigest, "checksum_sha256 mismatch"));
        }
        Ok(())
    }

    pub(crate) async fn handle_directory_creation(
        &self,
        content_length: Option<i64>,
        bucket: &str,
        key: &str,
    ) -> S3Result<()> {
        if content_length.map(|len| len > 0).unwrap_or(false) {
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
        metadata: &Option<dto::Metadata>,
        internal_info: InternalInfo,
    ) -> Result<()> {
        let internal_info_str = serde_json::to_string(&internal_info)?;
        let mut metadata_str = String::from("{}");
        if let Some(meta) = metadata {
            metadata_str = serde_json::to_string(meta)?;
        }
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

    pub(crate) fn to_timestamp(&self, datetime: &chrono::NaiveDateTime) -> Option<Timestamp> {
        let date_time_rfc3339 = datetime.and_utc().to_rfc3339();

        match Timestamp::parse(TimestampFormat::DateTime, date_time_rfc3339.as_str()) {
            Ok(t) => Some(t),
            Err(_) => None,
        }
    }

    pub(crate) async fn get_all_buckets(&self) -> Result<Vec<String>> {
        self.datastore.get_all_buckets().await
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
    use super::*;
    use async_trait::async_trait;
    use mockall::mock;
    use mockall::predicate::*;
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
        }
    }

    #[tokio::test]
    async fn test_save_s3_item_detail() {
        let mut mock_ds = MockTestDataStore::new();
        mock_ds
            .expect_save_s3_item_detail()
            .times(1)
            .returning(|_| Ok(()));

        // initialize the temp directory
        // Create a directory inside of `env::temp_dir()`
        let tmp_dir = tempdir().expect("tempdir created successfully");
        let root = tmp_dir.path().as_os_str();
        let metadata: Option<dto::Metadata> = serde_json::from_str(r#"{"hello": "world"}"#).ok();
        let internal_info = r#"{"internal": "info"}"#.to_string();
        let info: InternalInfo = serde_json::from_str(&internal_info).ok().unwrap();
        let backend = StorageBackend::new(root, mock_ds).expect("backend created successfully");

        let result = backend
            .save_s3_item_detail("bb", "item-1.txt", "random_md5sum", &metadata, info)
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_get_s3_item_detail() {
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
}
