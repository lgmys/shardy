use std::path::PathBuf;

use sqlx::SqlitePool;

#[derive(Clone)]
pub struct AppState {
    pub client: aws_sdk_s3::Client,
    pub master_db: SqlitePool,
    pub shard_db: SqlitePool,
    pub db_file: PathBuf,
    pub shard_filename: String,
    pub shard_id: uuid::Uuid,
    pub shard_start_time: String,
}
