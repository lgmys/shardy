use std::{collections::HashMap, env, path::PathBuf, time::Duration};

use aws_sdk_s3::{
    Client,
    config::{Credentials, Region},
};

use anyhow::Result;
use axum::{
    Json, Router, debug_handler,
    extract::State,
    response::IntoResponse,
    routing::{get, post},
};

use serde::{Deserialize, Serialize};
use sqlx::{Column, SqlitePool, ValueRef};
use sqlx::{FromRow, Row};

#[derive(Clone)]
struct AppState {
    client: aws_sdk_s3::Client,
    master_db: SqlitePool,
    shard_db: SqlitePool,
    db_file: PathBuf,
    shard_filename: String,
    shard_id: uuid::Uuid,
}

struct AppError(anyhow::Error);

const BUCKET: &'static str = "logs";

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Task {
    key: String,
    status: String,
}

use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode};
use std::str::FromStr;
use tempfile::NamedTempFile;
use time::format_description;
use tokio::{fs::File, io::AsyncWriteExt};

async fn connect_with_options(db_path: &str) -> Result<SqlitePool, sqlx::Error> {
    let options = SqliteConnectOptions::from_str(&format!("sqlite:{}", db_path))?
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(sqlx::sqlite::SqliteSynchronous::Normal)
        .busy_timeout(Duration::from_secs(30));

    SqlitePool::connect_with(options).await
}

async fn create_logs_table(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
       CREATE TABLE IF NOT EXISTS logs (
           id TEXT PRIMARY KEY,
           timestamp DATETIME NOT NULL,
           message TEXT NOT NULL
       )
       "#,
    )
    .execute(pool)
    .await?;

    Ok(())
}

async fn create_shards_table(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
       CREATE TABLE IF NOT EXISTS shards (
           id TEXT PRIMARY KEY,
           name TEXT NOT NULL,
           s3_path TEXT NOT NULL
       )
       "#,
    )
    .execute(pool)
    .await?;

    Ok(())
}

async fn upload_db_to_s3(
    client: &Client,
    db_path: &str,
    bucket: &str,
    key: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let body = tokio::fs::read(db_path).await?;

    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(body.into())
        .send()
        .await?;

    println!("Database synced to S3: s3://{}/{}", bucket, key);
    Ok(())
}

async fn checkpoint_and_sync(
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

    // Upload to S3
    upload_db_to_s3(s3_client, db_path, bucket, key).await?;

    Ok(())
}

async fn periodical_sync(state: AppState) -> Result<()> {
    let mut interval = tokio::time::interval(Duration::from_secs(30));

    let db_file = &state
        .db_file
        .to_str()
        .ok_or(anyhow::anyhow!("failure obtaining db file path"))?;

    loop {
        println!("periodical sync running {}", db_file);

        if checkpoint_and_sync(
            &state.shard_db,
            &state.client,
            db_file,
            BUCKET,
            &state.shard_filename,
        )
        .await
        .is_err()
        {
            println!("could not sync")
        }

        let shard = Shard {
            name: "logs".to_owned(),
            id: state.shard_id.to_string(),
            s3_path: state.shard_filename.clone(),
        };

        post_shard(&shard).await?;

        interval.tick().await;
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let key_id = "root".to_string();
    let secret_key = "changeme".to_string();
    let cred = Credentials::new(key_id, secret_key, None, None, "loaded-from-custom-env");
    let store_url = "http://localhost:9000";

    let mut cwd = std::env::current_dir()?;
    cwd.push("./master.db");
    let master_path = format!("sqlite:{}", cwd.display());
    let master_pool = connect_with_options(&master_path).await?;

    let format = format_description::parse("[year]-[month]-[day]_[hour]_[minute]")?;

    let shard_id = uuid::Uuid::new_v4();

    let shard_filename = format!(
        "logs.{}.{}.db",
        time::UtcDateTime::now().format(&format)?,
        &shard_id
    );

    sqlx::query("SELECT 1 = 1").execute(&master_pool).await?;

    let mut shard_path = std::env::temp_dir();
    shard_path.push(format!("sqlite_temp_{}.db", uuid::Uuid::new_v4()));
    let shard_url = format!("sqlite:{}", shard_path.display());

    println!("shard database {}", &shard_url);
    let shard_pool = connect_with_options(&shard_url).await?;

    sqlx::query("PRAGMA journal_mode = WAL")
        .execute(&shard_pool)
        .await?;

    sqlx::query("SELECT 1 = 1").execute(&shard_pool).await?;

    create_logs_table(&shard_pool).await?;
    create_shards_table(&master_pool).await?;

    let args: Vec<String> = env::args().collect();
    let subcommand = args.get(2).unwrap_or(&"api".to_owned()).clone();

    println!("Running in mode: {}", &subcommand);

    let s3_config = aws_sdk_s3::config::Builder::new()
        .endpoint_url(store_url)
        .credentials_provider(cred)
        .region(Region::new("eu-central-1"))
        .behavior_version_latest()
        .force_path_style(true)
        .build();

    let client = aws_sdk_s3::Client::from_conf(s3_config);

    let state = AppState {
        client: client.clone(),
        shard_db: shard_pool,
        master_db: master_pool,
        db_file: shard_path.clone(),
        shard_filename: shard_filename.clone(),
        shard_id: shard_id.clone(),
    };

    println!("api started");

    tokio::spawn(periodical_sync(state.clone()));

    start_app(state.clone()).await?;

    Ok(())
}

async fn info() -> &'static str {
    "it works"
}

#[derive(Debug, FromRow, Deserialize, Serialize)]
struct Shard {
    name: String,
    id: String,
    s3_path: String,
}

#[derive(Deserialize, Debug)]
struct SearchPayload {
    query: String,
}

/// Download SQLite database from S3 to a temporary file
async fn download_database(s3_client: &Client, bucket: &str, key: &str) -> Result<NamedTempFile> {
    println!("Downloading database from s3://{}/{}", bucket, key);

    // Get object from S3
    let response = s3_client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await?;

    // Create temporary file
    let temp_file = NamedTempFile::new()?;

    // Stream data from S3 to temp file
    let mut stream = response.body.into_async_read();
    let mut file_handle = File::from_std(temp_file.reopen()?);

    tokio::io::copy(&mut stream, &mut file_handle).await?;
    file_handle.flush().await?;

    println!(
        "Database downloaded to temporary file: {:?}",
        temp_file.path()
    );
    Ok(temp_file)
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

    println!("Database opened successfully");
    Ok((pool, temp_file))
}

/// Execute a simple query and return results as JSON-serializable structs
async fn execute_query(
    client: &Client,
    master_db: &SqlitePool,
    bucket: &str,
    pattern: &str,
    query: &str,
) -> Result<Vec<HashMap<String, String>>> {
    let shards = sqlx::query_as::<_, Shard>("SELECT * FROM shards WHERE name = ?1")
        .bind(pattern)
        .fetch_all(master_db)
        .await?;

    println!("============== running query:  {}", query);

    dbg!(&shards);

    let mut results: Vec<HashMap<String, String>> = vec![];

    for shard in shards {
        // TODO: figure out what files to load from S3 based on the pattern and the query
        let (pool, _temp_file) = open_database_from_s3(client, bucket, &shard.s3_path).await?;

        println!("Executing query: {}", query);
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
    }

    Ok(results)
}

async fn search(state: State<AppState>, payload: Json<SearchPayload>) -> impl IntoResponse {
    let query = payload.query.to_lowercase();
    let pattern: Vec<&str> = query.split("from ").collect();
    let pattern = pattern.get(1).unwrap_or(&"").trim();

    // Fetch multiple buckets in parallel
    if let Ok(results) = execute_query(
        &state.client,
        &state.master_db,
        BUCKET,
        &pattern,
        &payload.query,
    )
    .await
    {
        Json(results).into_response()
    } else {
        return AppError(anyhow::anyhow!("err")).into_response();
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        todo!()
    }
}

async fn store_shard(state: State<AppState>, payload: Json<Shard>) -> impl IntoResponse {
    if sqlx::query("INSERT INTO shards (id, name, s3_path) VALUES(?1, ?2, ?3)")
        .bind(&payload.id)
        .bind(&payload.name)
        .bind(&payload.s3_path)
        .execute(&state.master_db)
        .await
        .is_err()
    {
        return "error registering shard".into_response();
    }

    "search".into_response()
}

async fn post_shard(payload: &Shard) -> Result<()> {
    let client = reqwest::Client::new();

    client
        .post("http://localhost:3000/_shard")
        .body(serde_json::to_string(payload)?)
        .header("content-type", "application/json")
        .send()
        .await?;

    Ok(())
}

// save log to in-memory sqlite
#[debug_handler]
async fn logs(state: State<AppState>) -> impl IntoResponse {
    // first, smart log parser should be defined so that we tell the noise from actual data.

    let id = uuid::Uuid::new_v4().to_string();
    let timestamp = time::UtcDateTime::now();
    let message = format!("http log {}", timestamp);

    if let Err(err) = sqlx::query("INSERT INTO logs (id, timestamp, message) VALUES (?1, ?2, ?3)")
        .bind(id)
        .bind(timestamp.to_string())
        .bind(message)
        .execute(&state.shard_db)
        .await
    {
        return AppError(anyhow::anyhow!(err)).into_response();
    }

    "logged".into_response()
}

async fn start_app(state: AppState) -> Result<()> {
    let app = Router::new()
        .route("/", get(info))
        .route("/logs", post(logs))
        .route("/_shard", post(store_shard))
        .route("/search", post(search))
        .with_state(state.clone());

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;
    axum::serve(listener, app).await?;

    Ok(())
}
