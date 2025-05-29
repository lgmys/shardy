use anyhow::Result;
use aws_sdk_s3::Client;
use serde::{Deserialize, Serialize};
use sqlx::Column;
use sqlx::Row;
use sqlx::{FromRow, SqlitePool};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tempfile::NamedTempFile;
use time::format_description;
use tokio::sync::Mutex;

use crate::db::connect_with_options;
use crate::messages::Message;
use crate::messages::MessageSearchRequest;
use crate::messages::MessageSearchResponse;
use crate::object_storage::download_database;
use crate::object_storage::upload_db_to_s3;
use crate::schema::create_logs_table;

pub type QueryResults = Vec<HashMap<String, String>>;

#[derive(Debug, FromRow, Clone, Deserialize, Serialize)]
pub struct ShardMetadata {
    pub name: String,
    pub id: String,
    pub storage_key: String,
    pub timestamp: String,
}

#[derive(Clone)]
pub struct Shard {
    s3client: Client,
    metadata: ShardMetadata,
    pool: SqlitePool,
    shard_filename: String,
}

impl Shard {
    pub async fn new(s3client: Client) -> Result<Shard> {
        println!("new shard created");

        let format = format_description::parse("[year]-[month]-[day]_[hour]_[minute]")?;
        let shard_id = uuid::Uuid::new_v4();
        let shard_start_range = time::UtcDateTime::now();
        let shart_start_range_string = shard_start_range.format(&format)?;
        let shard_filename = format!("logs.{}.{}.db", &shart_start_range_string, &shard_id);

        // TODO: This this should be autorotated periodically somehow
        let mut shard_path = std::env::temp_dir();
        shard_path.push(format!("sqlite_temp_{}.db", uuid::Uuid::new_v4()));
        let shard_url = format!("sqlite:{}", shard_path.display());
        let shard_pool = connect_with_options(&shard_url).await?;

        sqlx::query("SELECT 1 = 1").execute(&shard_pool).await?;

        create_logs_table(&shard_pool).await?;

        Ok(Shard {
            s3client,
            metadata: ShardMetadata {
                timestamp: shard_start_range.to_string(),
                storage_key: shard_filename.clone(),
                id: shard_id.to_string(),
                name: "logs".to_owned(),
            },
            pool: shard_pool,
            shard_filename: shard_path.to_str().unwrap().to_owned(),
        })
    }

    pub async fn notify_coordinator(&self) -> Result<()> {
        println!("shard sent, notify_coordinator: {:?}", &self.metadata);

        let client = reqwest::Client::new();

        client
            .post("http://localhost:3000/_shard")
            .body(serde_json::to_string(&self.metadata)?)
            .header("content-type", "application/json")
            .send()
            .await?;

        Ok(())
    }

    pub async fn sync_shard_to_storage(&self) -> Result<(), Box<dyn std::error::Error>> {
        println!("wal force: {:?}", &self.metadata.id);
        sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
            .execute(&self.pool)
            .await?;

        // Small delay to ensure file system consistency
        tokio::time::sleep(Duration::from_millis(100)).await;

        println!("wal force done: {:?}", &self.metadata.id);

        println!("upload: {:?}", &self.metadata.id);

        upload_db_to_s3(
            &self.s3client,
            &self.shard_filename,
            &self.metadata.storage_key,
        )
        .await?;

        println!("upload done: {:?}", &self.metadata.id);

        self.notify_coordinator().await?;

        Ok(())
    }

    async fn open_database_from_s3(&self, key: &str) -> Result<(SqlitePool, NamedTempFile)> {
        // Download database to temp file
        let temp_file = download_database(&self.s3client, key).await?;

        // Open SQLite connection
        let database_url = format!("sqlite:{}", temp_file.path().display());
        let pool = SqlitePool::connect(&database_url).await?;

        // Configure for read operations
        sqlx::query("PRAGMA query_only = ON").execute(&pool).await?;
        sqlx::query("PRAGMA cache_size = 64000")
            .execute(&pool)
            .await?; // 64MB cache
        sqlx::query("PRAGMA mmap_size = 268435456")
            .execute(&pool)
            .await?; // 256MB mmap

        Ok((pool, temp_file))
    }

    pub async fn execute_shard_query(
        &self,
        shard: &ShardMetadata,
        query: &str,
    ) -> Result<QueryResults> {
        let mut results: QueryResults = vec![];

        let (pool, _temp_file) = self.open_database_from_s3(&shard.storage_key).await?;

        let rows = sqlx::query(query).fetch_all(&pool).await?;

        for row in rows {
            let mut row_as_map: HashMap<String, String> = HashMap::new();

            for (i, col) in row.columns().iter().enumerate() {
                let column_name = col.name();

                if let Ok(val) = row.try_get::<i64, _>(i) {
                    row_as_map.insert(column_name.to_owned(), format!("{}", val));
                } else if let Ok(val) = row.try_get::<String, _>(i) {
                    row_as_map.insert(column_name.to_owned(), format!("{}", val));
                }
            }

            results.push(row_as_map);
        }

        pool.close().await;

        Ok(results)
    }

    pub async fn create_log(&self) {
        let id = uuid::Uuid::new_v4().to_string();
        let timestamp = time::UtcDateTime::now();
        let message = format!("http log {}", timestamp);

        if let Err(err) =
            sqlx::query("INSERT INTO logs (id, timestamp, message) VALUES (?1, ?2, ?3)")
                .bind(id)
                .bind(timestamp.to_string())
                .bind(message)
                .execute(&self.pool)
                .await
        {
            println!("error {}", err);
        } else {
            println!("log created");
        }
    }
}

pub async fn schedule_query(
    master_db: &SqlitePool,
    commands: Arc<Mutex<Vec<String>>>,
    results: Arc<Mutex<HashMap<String, MessageSearchResponse>>>,
    pattern: &str,
    query: &str,
) -> Result<QueryResults> {
    // TODO: make shard time window dynamic (based on query itself partially)
    let shards = sqlx::query_as::<_, ShardMetadata>(
        "SELECT * FROM shards WHERE name = ?1 AND timestamp > datetime('now', '-60 minutes')",
    )
    .bind(pattern)
    .fetch_all(master_db)
    .await?;

    println!("==============");
    println!("running query: {} on {} shard(s)", query, shards.len());

    let mut queries: Vec<String> = vec![];

    for shard in shards {
        let uuid = uuid::Uuid::new_v4();
        let uuid = uuid.to_string();

        queries.push(uuid.clone());

        commands.lock().await.push(
            serde_json::to_string(&Message::SearchRequest(MessageSearchRequest {
                shard: shard.clone(),
                id: uuid,
                query: query.to_owned(),
            }))
            .unwrap(),
        );
    }

    println!("{} queries running", queries.len());

    let mut combined_results: QueryResults = vec![];

    let mut count: usize = 0;

    let start = time::UtcDateTime::now().unix_timestamp();

    loop {
        let end = time::UtcDateTime::now().unix_timestamp();

        if end - start == 5 {
            println!("timeout");
            break;
        }

        for id in queries.clone() {
            if let Some(result) = results.lock().await.remove(&id) {
                combined_results.append(&mut result.payload.clone());
                count += 1;
            }
        }

        tokio::time::sleep(Duration::from_millis(50)).await;

        if count != queries.len() {
            println!("{} queries done", count);
        } else {
            break;
        }
    }

    println!("all {} queries done", count);

    println!("results: {}", combined_results.len());

    println!("==============");

    Ok(combined_results)
}

pub async fn store_shard(pool: &SqlitePool, metadata: &ShardMetadata) -> Result<()> {
    let exists = sqlx::query("SELECT * FROM shards WHERE id = ?1")
        .bind(&metadata.id)
        .fetch_optional(pool)
        .await?;

    // If shard already exists, do nothing
    if exists.is_some() {
        return Ok(());
    }

    sqlx::query("INSERT INTO shards (id, name, storage_key, timestamp) VALUES(?1, ?2, ?3, ?4)")
        .bind(&metadata.id)
        .bind(&metadata.name)
        .bind(&metadata.storage_key)
        .bind(&metadata.timestamp)
        .execute(pool)
        .await?;

    Ok(())
}
