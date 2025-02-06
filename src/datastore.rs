use async_trait::async_trait;
use sqlx::PgPool;

use crate::error::Result;
use crate::Settings;

#[async_trait]
pub trait DataStore {
    async fn save_s3_item_detail(&self) -> Result<()>;
}

pub struct PostgresDatastore {
    pool: PgPool,
}

impl PostgresDatastore {
    pub fn new(settings: &Settings) -> Self {
        let host = &settings.datasource.host;
        let port = settings.datasource.port;
        let db = &settings.datasource.db;
        let user = &settings.datasource.user;
        let password = &settings.datasource.password;
        let schema = &settings.datasource.schema; // or use a field from settings if available

        let connection_string =
            format!("postgres://{user}:{password}@{host}:{port}/{db}?search_path={schema}");

        let pool = PgPool::connect_lazy(&connection_string).expect("Unable to create PgPool");

        Self { pool }
    }
}

#[async_trait]
impl DataStore for PostgresDatastore {
    async fn save_s3_item_detail(&self) -> Result<()> {
        // Implement the logic to save the S3 item detail to the Postgres database
        sqlx::query!(
            r#"
INSERT INTO s3_item_detail (
    bucket,
    key,
    metadata,
    last_modified,
    md5,
    data_location
)
VALUES (
    'my-bucket',
    'some/key/path',
    '{"custom":"metadata"}',
    '2023-10-03 12:34:56',
    'abcd1234abcd1234abcd1234abcd1234',
    'my-bucket/some/key/path'
);
"#
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}
