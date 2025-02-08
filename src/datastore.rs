use core::fmt;
use std::time::Duration;

use async_trait::async_trait;
use sqlx::postgres::PgPoolOptions;
use sqlx::{Pool, Postgres};

use crate::error::Result;
use crate::{MultipartUpload, MultipartUploadPart, S3ItemDetail, Settings};

#[async_trait]
pub trait DataStore: Send + Sync + 'static + std::fmt::Debug {
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

pub struct PostgresDatastore {
    pool: Pool<Postgres>,
}

impl PostgresDatastore {
    pub fn new(settings: &Settings) -> Self {
        let host = &settings.datasource.host;
        let port = settings.datasource.port;
        let db = &settings.datasource.db;
        let user = &settings.datasource.user;
        let password = &settings.datasource.password;
        let schema = &settings.datasource.schema; // or use a field from settings if available

        let max_connections = settings.datasource.max_connections;
        let min_connections = settings.datasource.min_connections;
        let test_before_acquire = settings.datasource.test_before_acquire;
        let acquire_slow_threshold =
            Duration::from_millis(settings.datasource.acquire_slow_threshold);
        let pool_options = PgPoolOptions::new()
            .max_connections(max_connections)
            .min_connections(min_connections)
            .acquire_slow_threshold(acquire_slow_threshold)
            .test_before_acquire(test_before_acquire);

        let connection_string = format!(
            "postgres://{user}:{password}@{host}:{port}/{db}?options=-csearch_path={schema}"
        );

        let pool = pool_options
            .connect_lazy(&connection_string)
            .expect("Unable to create PgPool");

        Self { pool }
    }

    pub async fn migrate(&self) -> Result {
        sqlx::migrate!("./migrations").run(&self.pool).await?;
        Ok(())
    }
}

#[async_trait]
impl DataStore for PostgresDatastore {
    async fn save_s3_item_detail(&self, item: &S3ItemDetail) -> Result<()> {
        sqlx::query!(
            r#"
            INSERT INTO s3_item_detail (bucket, key, metadata, internal_info, last_modified, md5, data_location)
            VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP, $5, $6)
            ON CONFLICT (bucket, key) DO UPDATE
            SET metadata = $3,
            internal_info = $4,
            md5 = $5,
            data_location = $6
            "#,
            item.bucket,
            item.key,
            item.metadata,
            item.internal_info,
            item.e_tag,
            item.data_location
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn get_s3_item_detail(&self, bucket: &str, key: &str) -> Result<Option<S3ItemDetail>> {
        let result = sqlx::query_as!(
            S3ItemDetail,
            r#"
            SELECT bucket, key, metadata, internal_info, last_modified, md5 as e_tag, data_location
            FROM s3_item_detail
            WHERE bucket = $1 AND key = $2 
            "#,
            bucket,
            key
        )
        .fetch_optional(&self.pool)
        .await?;

        Ok(result)
    }

    async fn get_s3_item_detail_with_filter(
        &self,
        bucket: &str,
        filter: &str,
    ) -> Result<Vec<S3ItemDetail>> {
        let filter_with_wildcard = format!("{}%", filter);
        let result = sqlx::query_as!(
            S3ItemDetail,
            r#"
            SELECT bucket, key, metadata, internal_info, last_modified, md5 as e_tag, data_location
            FROM s3_item_detail
            WHERE bucket = $1 AND key LIKE $2
            ORDER by key asc
            "#,
            bucket,
            filter_with_wildcard
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(result)
    }

    async fn get_all_buckets(&self) -> Result<Vec<String>> {
        let result_set = sqlx::query!(
            r#"
            SELECT DISTINCT bucket
            FROM s3_item_detail
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        let result: Vec<String> = result_set.iter().map(|row| row.bucket.clone()).collect();

        Ok(result)
    }

    async fn save_multipart_upload(&self, upload: &MultipartUpload) -> Result<()> {
        sqlx::query!(
            r#"
            INSERT INTO multipart_upload (upload_id, bucket, key, last_modified, metadata, access_key)
            VALUES ($1, $2, $3, CURRENT_TIMESTAMP, $4, $5)
            ON CONFLICT (upload_id, bucket, key) DO UPDATE
            SET metadata = $4,
            access_key = $5
            "#,
            upload.upload_id,
            upload.bucket,
            upload.key,
            upload.metadata,
            upload.access_key,
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn save_multipart_upload_part(&self, part: &MultipartUploadPart) -> Result<()> {
        sqlx::query!(
            r#"
            INSERT INTO multipart_upload_part (upload_id, part_number, last_modified, md5, data_location)
            VALUES ($1, $2, CURRENT_TIMESTAMP, $3, $4)
            ON CONFLICT (upload_id, part_number) DO UPDATE
            SET md5 = $3,
            data_location = $4
            "#,
            part.upload_id,
            part.part_number,
            part.md5,
            part.data_location,
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn get_access_key_by_upload_id(&self, upload_id: &str) -> Result<Option<String>> {
        let result = sqlx::query!(
            r#"
            SELECT access_key
            FROM multipart_upload
            WHERE upload_id = $1
            "#,
            upload_id
        )
        .fetch_optional(&self.pool)
        .await?;

        Ok(result.map(|row| row.access_key))
    }

    async fn get_parts_by_upload_id(&self, upload_id: &str) -> Result<Vec<MultipartUploadPart>> {
        let result = sqlx::query_as!(
            MultipartUploadPart,
            r#"
            SELECT upload_id, part_number, md5, last_modified,data_location
            FROM multipart_upload_part
            WHERE upload_id = $1
            ORDER BY part_number ASC
            "#,
            upload_id
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(result)
    }

    async fn get_multipart_upload_by_upload_id(
        &self,
        upload_id: &str,
    ) -> Result<Option<MultipartUpload>> {
        let result = sqlx::query_as!(
            MultipartUpload,
            r#"
            SELECT upload_id, bucket, key, last_modified, metadata, access_key
            FROM multipart_upload
            WHERE upload_id = $1
            "#,
            upload_id
        )
        .fetch_optional(&self.pool)
        .await?;

        Ok(result)
    }

    async fn delete_multipart_upload_by_upload_id(&self, upload_id: &str) -> Result<()> {
        sqlx::query!(
            r#"
            DELETE FROM multipart_upload
            WHERE upload_id = $1
            "#,
            upload_id
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}

impl fmt::Debug for PostgresDatastore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresDatastore").finish()
    }
}
