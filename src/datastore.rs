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

#[cfg(test)]
mod test_utils {

    use postgresql_archive::configuration::zonky;
    use postgresql_embedded::{PostgreSQL, Settings, VersionReq};

    use crate::error::Result;

    pub async fn initialize_database() -> Result<(PostgreSQL, Settings)> {
        let postgres_settings = Settings {
            releases_url: zonky::URL.to_string(),
            port: 0,
            version: VersionReq::parse("=17.2.0").unwrap(),
            ..Default::default()
        };

        let mut postgresql = PostgreSQL::new(postgres_settings);
        postgresql.setup().await?;
        postgresql.start().await?;

        let database_name = "ps_db";

        postgresql.create_database(database_name).await?;

        let settings = postgresql.settings().clone();
        // let pg = Arc::new(postgresql);

        Ok((postgresql, settings.clone()))
    }

    pub async fn cleanup_database(pg: PostgreSQL) {
        pg.stop().await.expect("failed to stop postgres");
    }
}

#[cfg(test)]
mod tests {

    use crate::Ds;

    use super::*;
    use test_utils::cleanup_database;
    use test_utils::initialize_database;

    #[tokio::test]
    async fn test_save_and_get_s3_item_detail() -> Result<()> {
        let (pg, pg_settings) = initialize_database().await?;

        let s = Settings {
            datasource: Ds {
                host: pg_settings.host,
                port: pg_settings.port,
                db: "ps_db".to_string(),
                user: pg_settings.username,
                password: pg_settings.password,
                min_connections: 1,
                max_connections: 2,
                schema: "public".to_string(),
                acquire_slow_threshold: 100,
                test_before_acquire: true,
            },
        };

        let datastore = PostgresDatastore::new(&s);
        datastore.migrate().await?;

        let item = S3ItemDetail {
            bucket: "test_bucket".to_string(),
            key: "test_key".to_string(),
            metadata: "{}".to_string(),
            internal_info: "{}".to_string(),
            last_modified: chrono::Utc::now().naive_utc(),
            e_tag: "test_etag".to_string(),
            data_location: "test_location".to_string(),
        };

        datastore.save_s3_item_detail(&item).await?;

        let retrieved_item = datastore
            .get_s3_item_detail("test_bucket", "test_key")
            .await?;

        assert!(retrieved_item.is_some());
        assert_eq!(retrieved_item.unwrap().e_tag, "test_etag".to_string());

        cleanup_database(pg).await;

        Ok(())
    }

    #[tokio::test]
    async fn test_get_s3_item_detail_with_filter() -> Result<()> {
        let (pg, pg_settings) = initialize_database().await?;

        let s = Settings {
            datasource: Ds {
                host: pg_settings.host,
                port: pg_settings.port,
                db: "ps_db".to_string(),
                user: pg_settings.username,
                password: pg_settings.password,
                min_connections: 1,
                max_connections: 2,
                schema: "public".to_string(),
                acquire_slow_threshold: 100,
                test_before_acquire: true,
            },
        };

        let datastore = PostgresDatastore::new(&s);
        datastore.migrate().await?;

        let item1 = S3ItemDetail {
            bucket: "test_bucket".to_string(),
            key: "test_key_1".to_string(),
            metadata: "{}".to_string(),
            internal_info: "{}".to_string(),
            last_modified: chrono::Utc::now().naive_utc(),
            e_tag: "test_etag_1".to_string(),
            data_location: "test_location_1".to_string(),
        };

        let item2 = S3ItemDetail {
            bucket: "test_bucket".to_string(),
            key: "test_key_2".to_string(),
            metadata: "{}".to_string(),
            internal_info: "{}".to_string(),
            last_modified: chrono::Utc::now().naive_utc(),
            e_tag: "test_etag_2".to_string(),
            data_location: "test_location_2".to_string(),
        };

        datastore.save_s3_item_detail(&item1).await?;
        datastore.save_s3_item_detail(&item2).await?;

        let retrieved_items = datastore
            .get_s3_item_detail_with_filter("test_bucket", "test_key")
            .await?;

        assert_eq!(retrieved_items.len(), 2);
        assert_eq!(retrieved_items[0].e_tag, "test_etag_1".to_string());
        assert_eq!(retrieved_items[1].e_tag, "test_etag_2".to_string());

        cleanup_database(pg).await;

        Ok(())
    }

    #[tokio::test]
    async fn test_get_all_buckets() -> Result<()> {
        let (pg, pg_settings) = initialize_database().await?;

        let s = Settings {
            datasource: Ds {
                host: pg_settings.host,
                port: pg_settings.port,
                db: "ps_db".to_string(),
                user: pg_settings.username,
                password: pg_settings.password,
                min_connections: 1,
                max_connections: 2,
                schema: "public".to_string(),
                acquire_slow_threshold: 100,
                test_before_acquire: true,
            },
        };

        let datastore = PostgresDatastore::new(&s);
        datastore.migrate().await?;

        let item1 = S3ItemDetail {
            bucket: "test_bucket_1".to_string(),
            key: "test_key_1".to_string(),
            metadata: "{}".to_string(),
            internal_info: "{}".to_string(),
            last_modified: chrono::Utc::now().naive_utc(),
            e_tag: "test_etag_1".to_string(),
            data_location: "test_location_1".to_string(),
        };

        let item2 = S3ItemDetail {
            bucket: "test_bucket_2".to_string(),
            key: "test_key_2".to_string(),
            metadata: "{}".to_string(),
            internal_info: "{}".to_string(),
            last_modified: chrono::Utc::now().naive_utc(),
            e_tag: "test_etag_2".to_string(),
            data_location: "test_location_2".to_string(),
        };

        datastore.save_s3_item_detail(&item1).await?;
        datastore.save_s3_item_detail(&item2).await?;

        let retrieved_buckets = datastore.get_all_buckets().await?;

        assert_eq!(retrieved_buckets.len(), 2);
        assert!(retrieved_buckets.contains(&"test_bucket_1".to_string()));
        assert!(retrieved_buckets.contains(&"test_bucket_2".to_string()));

        cleanup_database(pg).await;

        Ok(())
    }

    #[tokio::test]
    async fn test_save_and_get_multipart_upload() -> Result<()> {
        let (pg, pg_settings) = initialize_database().await?;

        let s = Settings {
            datasource: Ds {
                host: pg_settings.host,
                port: pg_settings.port,
                db: "ps_db".to_string(),
                user: pg_settings.username,
                password: pg_settings.password,
                min_connections: 1,
                max_connections: 2,
                schema: "public".to_string(),
                acquire_slow_threshold: 100,
                test_before_acquire: true,
            },
        };

        let datastore = PostgresDatastore::new(&s);
        datastore.migrate().await?;

        let upload = MultipartUpload {
            upload_id: "test_upload_id".to_string(),
            bucket: "test_bucket".to_string(),
            key: "test_key".to_string(),
            last_modified: chrono::Utc::now().naive_utc(),
            metadata: "{}".to_string(),
            access_key: "test_access_key".to_string(),
        };

        datastore.save_multipart_upload(&upload).await?;

        let retrieved_upload = datastore
            .get_multipart_upload_by_upload_id("test_upload_id")
            .await?;

        assert!(retrieved_upload.is_some());
        assert_eq!(
            retrieved_upload.unwrap().access_key,
            "test_access_key".to_string()
        );

        cleanup_database(pg).await;

        Ok(())
    }

    #[tokio::test]
    async fn test_save_and_get_multipart_upload_part() -> Result<()> {
        let (pg, pg_settings) = initialize_database().await?;

        let s = Settings {
            datasource: Ds {
                host: pg_settings.host,
                port: pg_settings.port,
                db: "ps_db".to_string(),
                user: pg_settings.username,
                password: pg_settings.password,
                min_connections: 1,
                max_connections: 2,
                schema: "public".to_string(),
                acquire_slow_threshold: 100,
                test_before_acquire: true,
            },
        };

        let datastore = PostgresDatastore::new(&s);
        datastore.migrate().await?;

        // First, save a MultipartUpload
        let upload = MultipartUpload {
            upload_id: "test_upload_id".to_string(),
            bucket: "test_bucket".to_string(),
            key: "test_key".to_string(),
            last_modified: chrono::Utc::now().naive_utc(),
            metadata: "{}".to_string(),
            access_key: "test_access_key".to_string(),
        };
        datastore.save_multipart_upload(&upload).await?;

        // Then, save the MultipartUploadPart
        let part = MultipartUploadPart {
            upload_id: "test_upload_id".to_string(),
            part_number: 1,
            last_modified: chrono::Utc::now().naive_utc(),
            md5: "test_md5".to_string(),
            data_location: "test_location".to_string(),
        };

        datastore.save_multipart_upload_part(&part).await?;

        let retrieved_parts = datastore.get_parts_by_upload_id("test_upload_id").await?;

        assert_eq!(retrieved_parts.len(), 1);
        assert_eq!(retrieved_parts[0].md5, "test_md5".to_string());

        cleanup_database(pg).await;

        Ok(())
    }

    #[tokio::test]
    async fn test_get_access_key_by_upload_id() -> Result<()> {
        let (pg, pg_settings) = initialize_database().await?;

        let s = Settings {
            datasource: Ds {
                host: pg_settings.host,
                port: pg_settings.port,
                db: "ps_db".to_string(),
                user: pg_settings.username,
                password: pg_settings.password,
                min_connections: 1,
                max_connections: 2,
                schema: "public".to_string(),
                acquire_slow_threshold: 100,
                test_before_acquire: true,
            },
        };

        let datastore = PostgresDatastore::new(&s);
        datastore.migrate().await?;

        let upload = MultipartUpload {
            upload_id: "test_upload_id".to_string(),
            bucket: "test_bucket".to_string(),
            key: "test_key".to_string(),
            last_modified: chrono::Utc::now().naive_utc(),
            metadata: "{}".to_string(),
            access_key: "test_access_key".to_string(),
        };

        datastore.save_multipart_upload(&upload).await?;

        let retrieved_access_key = datastore
            .get_access_key_by_upload_id("test_upload_id")
            .await?;

        assert!(retrieved_access_key.is_some());
        assert_eq!(retrieved_access_key.unwrap(), "test_access_key".to_string());

        cleanup_database(pg).await;

        Ok(())
    }

    #[tokio::test]
    async fn test_delete_multipart_upload_by_upload_id() -> Result<()> {
        let (pg, pg_settings) = initialize_database().await?;

        let s = Settings {
            datasource: Ds {
                host: pg_settings.host,
                port: pg_settings.port,
                db: "ps_db".to_string(),
                user: pg_settings.username,
                password: pg_settings.password,
                min_connections: 1,
                max_connections: 2,
                schema: "public".to_string(),
                acquire_slow_threshold: 100,
                test_before_acquire: true,
            },
        };

        let datastore = PostgresDatastore::new(&s);
        datastore.migrate().await?;

        let upload = MultipartUpload {
            upload_id: "test_upload_id".to_string(),
            bucket: "test_bucket".to_string(),
            key: "test_key".to_string(),
            last_modified: chrono::Utc::now().naive_utc(),
            metadata: "{}".to_string(),
            access_key: "test_access_key".to_string(),
        };

        datastore.save_multipart_upload(&upload).await?;

        datastore
            .delete_multipart_upload_by_upload_id("test_upload_id")
            .await?;

        let retrieved_upload = datastore
            .get_multipart_upload_by_upload_id("test_upload_id")
            .await?;

        assert!(retrieved_upload.is_none());

        cleanup_database(pg).await;

        Ok(())
    }
}
