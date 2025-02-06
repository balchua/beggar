use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct Settings {
    pub datasource: DataSource,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DataSource {
    pub host: String,
    pub port: u16,
    pub db: String,
    pub user: String,
    pub password: String,
    pub schema: String,
}
