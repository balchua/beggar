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

impl MultipartUploadBuilder {
    pub fn upload_id(mut self, upload_id: String) -> Self {
        self.upload_id = Some(upload_id);
        self
    }

    pub fn bucket(mut self, bucket: String) -> Self {
        self.bucket = Some(bucket);
        self
    }

    pub fn key(mut self, key: String) -> Self {
        self.key = Some(key);
        self
    }

    pub fn metadata(mut self, metadata: String) -> Self {
        self.metadata = Some(metadata);
        self
    }

    pub fn access_key(mut self, access_key: String) -> Self {
        self.access_key = Some(access_key);
        self
    }

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
