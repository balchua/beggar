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
    pub fn builder() -> MultipartUploadPartBuilder {
        MultipartUploadPartBuilder::default()
    }
}

#[derive(Debug, Clone, Default)]
pub struct MultipartUploadPartBuilder {
    upload_id: Option<String>,
    part_number: Option<i32>,
    last_modified: Option<NaiveDateTime>,
    md5: Option<String>,
    data_location: Option<String>,
}

impl MultipartUploadPartBuilder {
    pub fn upload_id(mut self, upload_id: String) -> Self {
        self.upload_id = Some(upload_id);
        self
    }

    pub fn part_number(mut self, part_number: i32) -> Self {
        self.part_number = Some(part_number);
        self
    }

    pub fn last_modified(mut self, last_modified: NaiveDateTime) -> Self {
        self.last_modified = Some(last_modified);
        self
    }

    pub fn md5(mut self, md5: String) -> Self {
        self.md5 = Some(md5);
        self
    }

    pub fn data_location(mut self, data_location: String) -> Self {
        self.data_location = Some(data_location);
        self
    }

    pub fn build(self) -> MultipartUploadPart {
        MultipartUploadPart {
            upload_id: self.upload_id.expect("upload_id must be set"),
            part_number: self.part_number.expect("part_number must be set"),
            last_modified: self.last_modified.expect("last_modified must be set"),
            md5: self.md5.expect("md5 must be set"),
            data_location: self.data_location.expect("data_location must be set"),
        }
    }
}
