use std::{collections::HashMap, path::PathBuf, sync::Arc};

use sqlx::SqlitePool;
use tokio::sync::Mutex;

use crate::messages::MessageSearchResponse;

#[derive(Clone)]
pub struct ApiState {
    pub master_db: SqlitePool,
    pub client: aws_sdk_s3::Client,
    pub commands: Arc<Mutex<Vec<String>>>,
    pub results: Arc<Mutex<HashMap<String, MessageSearchResponse>>>,
}

#[derive(Clone)]
pub struct WorkerState {
    pub client: aws_sdk_s3::Client,
    pub shard_db: SqlitePool,
    pub db_file: PathBuf,
    pub shard_filename: String,
    pub shard_id: uuid::Uuid,
    pub shard_start_time: String,
}
