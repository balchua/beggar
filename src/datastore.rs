use core::fmt;
use std::time::Duration;

use async_trait::async_trait;
use sqlx::postgres::{PgPoolOptions, PgConnectOptions};
use sqlx::{Pool, Postgres};
use tracing::{info, error, debug, instrument};

use crate::error::Result;
use crate::{MultipartUpload, MultipartUploadPart, S3ItemDetail, Settings};

// Constants for security and performance
const MAX_QUERY_SIZE: usize = 1000; // Limit query results
const CONNECTION_TIMEOUT: u64 = 30; // Connection timeout in seconds
const STATEMENT_TIMEOUT: &str = "30000"; // SQL statement timeout in milliseconds

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
    /// Connects to the database asynchronously
    /// This is the only way to create a new PostgresDatastore instance
    #[instrument(level = "info", name = "db_connect", skip(settings))]
    pub async fn connect(settings: &Settings) -> Result<Self> {
        let host = &settings.datasource.host;
        let port = settings.datasource.port;
        let db = &settings.datasource.db;
        let user = &settings.datasource.user;
        let password = &settings.datasource.password;
        let schema = &settings.datasource.schema;

        let max_connections = settings.datasource.max_connections;
        let min_connections = settings.datasource.min_connections;
        let test_before_acquire = settings.datasource.test_before_acquire;
        let acquire_slow_threshold =
            Duration::from_millis(settings.datasource.acquire_slow_threshold);
        
        // Log sanitized connection info (no password)
        let sanitized_connection = format!(
            "postgres://{}:*****@{}:{}/{}?options=-csearch_path={}",
            user, host, port, db, schema
        );
        info!(target: "database", connection = %sanitized_connection, "Initializing database connection");

        // Create connection options with security settings
        let connect_options = PgConnectOptions::new()
            .host(host)
            .port(port)
            .username(user)
            .password(password)
            .database(db)
            .options([
                ("application_name", "beggar_s3_server"),
                ("search_path", schema),
                ("statement_timeout", STATEMENT_TIMEOUT), // Prevent long-running queries
                ("log_statement", "none"),                // Don't log statements with credentials
                ("tcp_keepalives_idle", "60"),            // Keep connections alive
            ]);
        
        let pool_options = PgPoolOptions::new()
            .max_connections(max_connections)
            .min_connections(min_connections)
            .acquire_timeout(Duration::from_secs(CONNECTION_TIMEOUT))
            .acquire_slow_threshold(acquire_slow_threshold)
            .test_before_acquire(test_before_acquire);
        
        // Establish connection with immediate validation
        debug!(target: "database", "Creating connection pool");
        
        let pool = match pool_options.connect_with(connect_options).await {
            Ok(pool) => {
                // Verify we can actually execute a query
                debug!(target: "database", "Validating database connection");
                match sqlx::query("SELECT 1").execute(&pool).await {
                    Ok(_) => {
                        info!(target: "database", "Database connection established and validated");
                        pool
                    },
                    Err(e) => {
                        error!(
                            target: "database", 
                            error = %e,
                            "Database connection validation failed"
                        );
                        return Err(e.into());
                    }
                }
            },
            Err(e) => {
                error!(
                    target: "database",
                    error = %e, 
                    host = %host,
                    port = %port,
                    db = %db,
                    "Failed to establish database connection"
                );
                return Err(e.into());
            }
        };

        Ok(Self { pool })
    }

    /// Only for tests - creates a datastore with an existing pool
    #[cfg(test)]
    pub fn with_pool(pool: Pool<Postgres>) -> Self {
        Self { pool }
    }

    #[instrument(level = "info", name = "db_migration", skip(self))]
    pub async fn migrate(&self) -> Result<()> {
        info!(target: "database", "Running database migrations");
        match sqlx::migrate!("./migrations").run(&self.pool).await {
            Ok(_) => {
                info!(target: "database", "Database migrations completed successfully");
                Ok(())
            },
            Err(e) => {
                error!(error = %e, "Database migration failed");
                Err(e.into())
            }
        }
    }
    
    // New method to sanitize database inputs for logging
    fn sanitize_for_logging(input: &str) -> String {
        // Simple sanitization for logging purposes
        let shortened = if input.len() > 50 {
            format!("{}...", &input[..47])
        } else {
            input.to_string()
        };
        
        // Remove potentially dangerous characters for logging
        shortened
            .chars()
            .filter(|c| c.is_alphanumeric() || c.is_whitespace() || *c == '_' || *c == '-' || *c == '/')
            .collect()
    }
    
    // New health check method
    #[instrument(level = "info", name = "db_health_check", skip(self))]
    pub async fn check_connection_health(&self) -> Result<()> {
        debug!(target: "database", "Checking database connection health");
        match sqlx::query("SELECT 1").execute(&self.pool).await {
            Ok(_) => {
                info!(target: "database", "Database connection is healthy");
                Ok(())
            }
            Err(e) => {
                error!(
                    target: "database",
                    error = %e,
                    "Database connection health check failed"
                );
                Err(e.into())
            }
        }
    }
}

#[async_trait]
impl DataStore for PostgresDatastore {
    #[instrument(level = "debug", name = "save_item", skip(self, item), fields(bucket = %item.bucket, key = %item.key))]
    async fn save_s3_item_detail(&self, item: &S3ItemDetail) -> Result<()> {
        debug!(
            target: "storage",
            "Saving S3 item detail"
        );
        
        match sqlx::query!(
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
        .await {
            Ok(_) => {
                info!(
                    target: "storage",
                    bucket = %item.bucket,
                    key = %item.key,
                    "S3 item detail saved successfully"
                );
                Ok(())
            },
            Err(e) => {
                error!(
                    target: "storage",
                    error = %e,
                    bucket = %item.bucket,
                    key = %item.key,
                    "Failed to save S3 item detail"
                );
                Err(e.into())
            }
        }
    }

    #[instrument(level = "debug", name = "get_item", skip(self), fields(bucket = %bucket, key = %key))]
    async fn get_s3_item_detail(&self, bucket: &str, key: &str) -> Result<Option<S3ItemDetail>> {
        debug!(
            bucket = %Self::sanitize_for_logging(bucket),
            key = %Self::sanitize_for_logging(key),
            "Retrieving S3 item detail"
        );
        
        match sqlx::query_as!(
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
        .await {
            Ok(result) => {
                // Only log at info level if found - reduces noise for common "not found" cases
                if result.is_some() {
                    info!(
                        target: "storage",
                        bucket = %bucket,
                        key = %key,
                        "S3 item detail retrieved successfully"
                    );
                } else {
                    debug!(
                        target: "storage",
                        bucket = %bucket,
                        key = %key,
                        "S3 item detail not found"
                    );
                }
                Ok(result)
            },
            Err(e) => {
                error!(
                    error = %e,
                    bucket = %Self::sanitize_for_logging(bucket),
                    key = %Self::sanitize_for_logging(key),
                    "Failed to retrieve S3 item detail"
                );
                Err(e.into())
            }
        }
    }

    #[instrument(level = "debug", name = "get_items_with_filter", skip(self), fields(bucket = %bucket, filter = %filter))]
    async fn get_s3_item_detail_with_filter(
        &self,
        bucket: &str,
        filter: &str,
    ) -> Result<Vec<S3ItemDetail>> {
        debug!(
            bucket = %Self::sanitize_for_logging(bucket),
            filter = %Self::sanitize_for_logging(filter),
            "Retrieving S3 items with filter"
        );
        
        // Add LIMIT to prevent too many results (DoS protection)
        let filter_with_wildcard = format!("{}%", filter);
        match sqlx::query_as!(
            S3ItemDetail,
            r#"
            SELECT bucket, key, metadata, internal_info, last_modified, md5 as e_tag, data_location
            FROM s3_item_detail
            WHERE bucket = $1 AND key LIKE $2
            ORDER by key asc
            LIMIT $3
            "#,
            bucket,
            filter_with_wildcard,
            MAX_QUERY_SIZE as i32
        )
        .fetch_all(&self.pool)
        .await {
            Ok(result) => {
                debug!(
                    bucket = %Self::sanitize_for_logging(bucket),
                    filter = %Self::sanitize_for_logging(filter),
                    count = result.len(),
                    "Retrieved S3 items with filter"
                );
                Ok(result)
            },
            Err(e) => {
                error!(
                    error = %e,
                    bucket = %Self::sanitize_for_logging(bucket),
                    filter = %Self::sanitize_for_logging(filter),
                    "Failed to retrieve S3 items with filter"
                );
                Err(e.into())
            }
        }
    }

    #[instrument(level = "debug", name = "get_all_buckets", skip(self))]
    async fn get_all_buckets(&self) -> Result<Vec<String>> {
        debug!("Retrieving all buckets");
        
        // Add LIMIT to prevent potential DoS with too many buckets
        match sqlx::query!(
            r#"
            SELECT DISTINCT bucket
            FROM s3_item_detail
            LIMIT $1
            "#,
            MAX_QUERY_SIZE as i32
        )
        .fetch_all(&self.pool)
        .await {
            Ok(result_set) => {
                let result: Vec<String> = result_set.iter().map(|row| row.bucket.clone()).collect();
                debug!(count = result.len(), "Retrieved all buckets");
                Ok(result)
            },
            Err(e) => {
                error!(error = %e, "Failed to retrieve all buckets");
                Err(e.into())
            }
        }
    }

    async fn save_multipart_upload(&self, upload: &MultipartUpload) -> Result<()> {
        debug!(
            bucket = %Self::sanitize_for_logging(&upload.bucket),
            key = %Self::sanitize_for_logging(&upload.key),
            upload_id = %upload.upload_id,
            "Saving multipart upload"
        );
        
        match sqlx::query!(
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
        .await {
            Ok(_) => {
                info!(
                    bucket = %Self::sanitize_for_logging(&upload.bucket),
                    key = %Self::sanitize_for_logging(&upload.key),
                    upload_id = %upload.upload_id,
                    "Multipart upload saved successfully"
                );
                Ok(())
            },
            Err(e) => {
                error!(
                    error = %e,
                    bucket = %Self::sanitize_for_logging(&upload.bucket),
                    key = %Self::sanitize_for_logging(&upload.key),
                    upload_id = %upload.upload_id,
                    "Failed to save multipart upload"
                );
                Err(e.into())
            }
        }
    }

    async fn save_multipart_upload_part(&self, part: &MultipartUploadPart) -> Result<()> {
        debug!(
            upload_id = %part.upload_id,
            part_number = part.part_number,
            "Saving multipart upload part"
        );
        
        match sqlx::query!(
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
        .await {
            Ok(_) => {
                debug!(
                    upload_id = %part.upload_id,
                    part_number = part.part_number,
                    "Multipart upload part saved successfully"
                );
                Ok(())
            },
            Err(e) => {
                error!(
                    error = %e,
                    upload_id = %part.upload_id,
                    part_number = part.part_number,
                    "Failed to save multipart upload part"
                );
                Err(e.into())
            }
        }
    }

    async fn get_access_key_by_upload_id(&self, upload_id: &str) -> Result<Option<String>> {
        debug!(upload_id = %upload_id, "Retrieving access key by upload ID");
        
        match sqlx::query!(
            r#"
            SELECT access_key
            FROM multipart_upload
            WHERE upload_id = $1
            "#,
            upload_id
        )
        .fetch_optional(&self.pool)
        .await {
            Ok(result) => {
                debug!(
                    upload_id = %upload_id,
                    found = result.is_some(),
                    "Access key retrieval completed"
                );
                Ok(result.map(|row| row.access_key))
            },
            Err(e) => {
                error!(
                    error = %e,
                    upload_id = %upload_id,
                    "Failed to retrieve access key by upload ID"
                );
                Err(e.into())
            }
        }
    }

    async fn get_parts_by_upload_id(&self, upload_id: &str) -> Result<Vec<MultipartUploadPart>> {
        debug!(upload_id = %upload_id, "Retrieving parts by upload ID");
        
        // Add LIMIT to prevent too many results (DoS protection)
        match sqlx::query_as!(
            MultipartUploadPart,
            r#"
            SELECT upload_id, part_number, md5, last_modified, data_location
            FROM multipart_upload_part
            WHERE upload_id = $1
            ORDER BY part_number ASC
            LIMIT $2
            "#,
            upload_id,
            MAX_QUERY_SIZE as i32
        )
        .fetch_all(&self.pool)
        .await {
            Ok(result) => {
                debug!(
                    upload_id = %upload_id,
                    count = result.len(),
                    "Retrieved parts by upload ID"
                );
                Ok(result)
            },
            Err(e) => {
                error!(
                    error = %e,
                    upload_id = %upload_id,
                    "Failed to retrieve parts by upload ID"
                );
                Err(e.into())
            }
        }
    }

    async fn get_multipart_upload_by_upload_id(
        &self,
        upload_id: &str,
    ) -> Result<Option<MultipartUpload>> {
        debug!(upload_id = %upload_id, "Retrieving multipart upload by ID");
        
        match sqlx::query_as!(
            MultipartUpload,
            r#"
            SELECT upload_id, bucket, key, last_modified, metadata, access_key
            FROM multipart_upload
            WHERE upload_id = $1
            "#,
            upload_id
        )
        .fetch_optional(&self.pool)
        .await {
            Ok(result) => {
                debug!(
                    upload_id = %upload_id,
                    found = result.is_some(),
                    "Multipart upload retrieval completed"
                );
                Ok(result)
            },
            Err(e) => {
                error!(
                    error = %e,
                    upload_id = %upload_id,
                    "Failed to retrieve multipart upload by ID"
                );
                Err(e.into())
            }
        }
    }

    #[instrument(level = "info", name = "delete_multipart_upload", skip(self), fields(upload_id = %upload_id))]
    async fn delete_multipart_upload_by_upload_id(&self, upload_id: &str) -> Result<()> {
        debug!(target: "storage", "Deleting multipart upload by ID");
        
        match sqlx::query!(
            r#"
            DELETE FROM multipart_upload
            WHERE upload_id = $1
            "#,
            upload_id
        )
        .execute(&self.pool)
        .await {
            Ok(result) => {
                let rows_affected = result.rows_affected();
                info!(
                    target: "storage",
                    upload_id = %upload_id,
                    rows_affected = %rows_affected,
                    "Multipart upload deleted"
                );
                Ok(())
            },
            Err(e) => {
                error!(
                    error = %e,
                    upload_id = %upload_id,
                    "Failed to delete multipart upload by ID"
                );
                Err(e.into())
            }
        }
    }
}

impl fmt::Debug for PostgresDatastore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresDatastore").finish()
    }
}
