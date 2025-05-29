use std::{collections::HashMap, sync::Arc};

use sqlx::SqlitePool;
use tokio::sync::Mutex;

use crate::{messages::MessageSearchResponse, shards::ShardHandle};

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
    pub shard: Arc<Mutex<ShardHandle>>,
}
