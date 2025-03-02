use sqlx::FromRow;

#[derive(Debug, Clone, FromRow)]
pub struct MultipartUpload {
    pub upload_id: String,
    pub bucket: String,
    pub key: String,
    pub metadata: String,
    pub last_modified: chrono::NaiveDateTime,
    pub access_key: String,
}

impl MultipartUpload {
    #[must_use]
    pub fn builder() -> MultipartUploadBuilder {
        MultipartUploadBuilder::default()
    }
}

#[derive(Debug, Default)]
pub struct MultipartUploadBuilder {
    upload_id: Option<String>,
    bucket: Option<String>,
    key: Option<String>,
    metadata: Option<String>,
    access_key: Option<String>,
}

/// Builder for [`MultipartUpload`].
impl MultipartUploadBuilder {
    /// Sets the upload ID.
    #[must_use]
    pub fn upload_id(mut self, upload_id: String) -> Self {
        self.upload_id = Some(upload_id);
        self
    }

    /// Sets the bucket name.
    #[must_use]
    pub fn bucket(mut self, bucket: String) -> Self {
        self.bucket = Some(bucket);
        self
    }

    /// Sets the object key.
    #[must_use]
    pub fn key(mut self, key: String) -> Self {
        self.key = Some(key);
        self
    }

    /// Sets the metadata.
    #[must_use]
    pub fn metadata(mut self, metadata: String) -> Self {
        self.metadata = Some(metadata);
        self
    }

    /// Sets the access key.
    #[must_use]
    pub fn access_key(mut self, access_key: String) -> Self {
        self.access_key = Some(access_key);
        self
    }

    /// Builds a [`MultipartUpload`] from this builder.
    ///
    /// # Panics
    ///
    /// This function will panic if any of the required fields (`upload_id`, `bucket`,
    /// `key`, `metadata`, or `access_key`) are not set.
    #[must_use]
    pub fn build(self) -> MultipartUpload {
        MultipartUpload {
            upload_id: self.upload_id.expect("upload_id is required"),
            bucket: self.bucket.expect("bucket is required"),
            key: self.key.expect("key is required"),
            metadata: self.metadata.expect("metadata is required"),
            last_modified: chrono::Utc::now().naive_utc(),
            access_key: self.access_key.expect("access_key is required"),
        }
    }
}
