use chrono::NaiveDateTime;
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, sqlx::FromRow)]
pub struct S3ItemDetail {
    pub bucket: String,
    pub key: String,
    pub e_tag: String,
    pub last_modified: NaiveDateTime,
    pub data_location: String,
    pub metadata: String,
    pub internal_info: String,
}

#[derive(Debug, Default)]
pub struct S3ItemDetailBuilder {
    bucket: Option<String>,
    key: Option<String>,
    e_tag: Option<String>,
    last_modified: Option<NaiveDateTime>,
    data_location: Option<String>,
    metadata: Option<String>,
    internal_info: Option<String>,
}

impl S3ItemDetail {
    #[must_use]
    pub fn builder() -> S3ItemDetailBuilder {
        S3ItemDetailBuilder::default()
    }
}

impl S3ItemDetailBuilder {

    #[must_use]
    pub fn bucket(mut self, bucket: String) -> Self {
        self.bucket = Some(bucket);
        self
    }


    #[must_use]
    pub fn key(mut self, key: String) -> Self {
        self.key = Some(key);
        self
    }

    #[must_use]
    pub fn e_tag(mut self, e_tag: String) -> Self {
        self.e_tag = Some(e_tag);
        self
    }


    #[must_use]
    pub fn last_modified(mut self, last_modified: NaiveDateTime) -> Self {
        self.last_modified = Some(last_modified);
        self
    }

    #[must_use]
    pub fn data_location(mut self, data_location: String) -> Self {
        self.data_location = Some(data_location);
        self
    }

    #[must_use]
    pub fn metadata(mut self, metadata: Option<String>) -> Self {
        self.metadata = metadata;
        self
    }

    #[must_use]
    pub fn internal_info(mut self, internal_info: Option<String>) -> Self {
        self.internal_info = internal_info;
        self
    }


    /// Creates a new [`S3ItemDetail`] from the builder.
    ///
    /// # Panics
    ///
    /// Panics if any of the required fields (`bucket`, `key`, `e_tag`, `data_location`,
    /// `metadata`, or `internal_info`) are not set.
    #[must_use]
    pub fn build(self) -> S3ItemDetail {
        S3ItemDetail {
            bucket: self.bucket.expect("bucket is required"),
            key: self.key.expect("key is required"),
            e_tag: self.e_tag.expect("e_tag is required"),
            last_modified: chrono::Utc::now().naive_utc(),
            data_location: self.data_location.expect("data_location is required"),
            metadata: self.metadata.expect("metadata is required"),
            internal_info: self.internal_info.expect("internal_info is required"),
        }
    }
}
