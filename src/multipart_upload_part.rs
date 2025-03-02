use sqlx::types::chrono::NaiveDateTime;

#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct MultipartUploadPart {
    pub upload_id: String,
    pub part_number: i32,
    pub last_modified: NaiveDateTime,
    pub md5: String,
    pub data_location: String,
}

impl MultipartUploadPart {
    #[must_use]
    pub fn builder() -> MultipartUploadPartBuilder {
        MultipartUploadPartBuilder::default()
    }
}

#[derive(Debug, Clone, Default)]
pub struct MultipartUploadPartBuilder {
    upload_id: Option<String>,
    part_number: Option<i32>,
    md5: Option<String>,
    data_location: Option<String>,
}

impl MultipartUploadPartBuilder {
    #[must_use]
    pub fn upload_id(mut self, upload_id: String) -> Self {
        self.upload_id = Some(upload_id);
        self
    }

    #[must_use]
    pub fn part_number(mut self, part_number: i32) -> Self {
        self.part_number = Some(part_number);
        self
    }

    #[must_use]
    pub fn md5(mut self, md5: String) -> Self {
        self.md5 = Some(md5);
        self
    }

    #[must_use]
    pub fn data_location(mut self, data_location: String) -> Self {
        self.data_location = Some(data_location);
        self
    }

    /// Creates a `MultipartUploadPart` from the builder.
    ///
    /// # Panics
    ///
    /// Panics if any of the required fields have not been set:
    /// - `upload_id`
    /// - `part_number`
    /// - `md5`
    /// - `data_location`
    #[must_use]
    pub fn build(self) -> MultipartUploadPart {
        MultipartUploadPart {
            upload_id: self.upload_id.expect("upload_id must be set"),
            part_number: self.part_number.expect("part_number must be set"),
            last_modified: chrono::Utc::now().naive_utc(),
            md5: self.md5.expect("md5 must be set"),
            data_location: self.data_location.expect("data_location must be set"),
        }
    }
}
