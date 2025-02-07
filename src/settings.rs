use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct Settings {
    pub datasource: Ds,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Ds {
    pub host: String,
    pub port: u16,
    pub db: String,
    pub user: String,
    pub password: String,
    pub schema: String,
    pub max_connections: u32,
    pub min_connections: u32,
    pub test_before_acquire: bool,
    pub acquire_slow_threshold: u64,
}
