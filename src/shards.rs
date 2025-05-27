use anyhow::Result;
use aws_sdk_s3::Client;
use futures::future::join_all;
use serde::{Deserialize, Serialize};
use sqlx::Column;
use sqlx::Row;
use sqlx::{FromRow, SqlitePool};
use std::collections::HashMap;
use std::time::Duration;
use tempfile::NamedTempFile;

use crate::object_storage::download_database;
use crate::object_storage::upload_db_to_s3;

#[derive(Debug, FromRow, Deserialize, Serialize)]
pub struct Shard {
    pub name: String,
    pub id: String,
    pub s3_path: String,
    pub timestamp: String,
}

pub async fn post_shard(payload: &Shard) -> Result<()> {
    let client = reqwest::Client::new();

    client
        .post("http://localhost:3000/_shard")
        .body(serde_json::to_string(payload)?)
        .header("content-type", "application/json")
        .send()
        .await?;

    Ok(())
}

pub async fn checkpoint_and_sync(
    pool: &SqlitePool,
    s3_client: &Client,
    db_path: &str,
    bucket: &str,
    key: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // Force checkpoint to consolidate WAL into main database
    sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
        .execute(pool)
        .await?;

    // Small delay to ensure file system consistency
    tokio::time::sleep(Duration::from_millis(100)).await;

    upload_db_to_s3(s3_client, db_path, bucket, key).await?;

    Ok(())
}

pub async fn store_shard(db: &SqlitePool, shard: &Shard) -> Result<()> {
    sqlx::query("INSERT INTO shards (id, name, s3_path, timestamp) VALUES(?1, ?2, ?3, ?4)")
        .bind(&shard.id)
        .bind(&shard.name)
        .bind(&shard.s3_path)
        .bind(&shard.timestamp)
        .execute(db)
        .await?;

    Ok(())
}

/// Download and open SQLite database from S3
async fn open_database_from_s3(
    client: &Client,
    bucket: &str,
    key: &str,
) -> Result<(SqlitePool, NamedTempFile)> {
    // Download database to temp file
    let temp_file = download_database(client, bucket, key).await?;

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

async fn execute_shard_query(
    client: &Client,
    bucket: &str,
    shard: &Shard,
    query: &str,
) -> Result<Vec<HashMap<String, String>>> {
    let mut results: Vec<HashMap<String, String>> = vec![];

    let (pool, _temp_file) = open_database_from_s3(client, bucket, &shard.s3_path).await?;

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

pub async fn execute_query(
    client: &Client,
    master_db: &SqlitePool,
    bucket: &str,
    pattern: &str,
    query: &str,
) -> Result<Vec<HashMap<String, String>>> {
    // TODO: make shard time window dynamic (based on query itself partially)
    let shards = sqlx::query_as::<_, Shard>(
        "SELECT * FROM shards WHERE name = ?1 AND timestamp > datetime('now', '-60 minutes')",
    )
    .bind(pattern)
    .fetch_all(master_db)
    .await?;

    println!("==============");
    println!("running query: {} on {} shard(s)", query, shards.len());

    let mut combined_results: Vec<HashMap<String, String>> = vec![];

    let futures: Vec<_> = shards
        .iter()
        .map(|shard| execute_shard_query(client, bucket, &shard, query))
        .collect();

    let future_results = join_all(futures).await;

    for res in future_results {
        combined_results.append(&mut res.unwrap().clone());
    }

    println!("results: {}", combined_results.len());

    Ok(combined_results)
}
