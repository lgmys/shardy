use std::{env, path::PathBuf, time::Duration};

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

use db::connect_with_options;
use errors::AppError;
use object_storage::upload_db_to_s3;
use serde::Deserialize;
use shards::{Shard, checkpoint_and_sync, execute_query, post_shard};
use sqlx::SqlitePool;
use state::AppState;
use time::format_description;

mod db;
mod errors;
mod object_storage;
mod shards;
mod state;

const BUCKET: &'static str = "logs";

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
           s3_path TEXT NOT NULL,
           timestamp DATETIME NOT NULL
       )
       "#,
    )
    .execute(pool)
    .await?;

    Ok(())
}

async fn run_sync_periodically(state: AppState) -> Result<()> {
    let mut interval = tokio::time::interval(Duration::from_secs(60));

    let db_file = &state
        .db_file
        .to_str()
        .ok_or(anyhow::anyhow!("failure obtaining db file path"))?;

    loop {
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
            timestamp: state.shard_start_time.clone(),
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
    let shard_start_range = time::UtcDateTime::now();
    let shart_start_range_string = shard_start_range.format(&format)?;
    let shard_filename = format!("logs.{}.{}.db", &shart_start_range_string, &shard_id);

    sqlx::query("SELECT 1 = 1").execute(&master_pool).await?;

    let mut shard_path = std::env::temp_dir();
    shard_path.push(format!("sqlite_temp_{}.db", uuid::Uuid::new_v4()));
    let shard_url = format!("sqlite:{}", shard_path.display());

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
        shard_start_time: shard_start_range.to_string(),
    };

    tokio::spawn(run_sync_periodically(state.clone()));

    start_app(state.clone()).await?;

    Ok(())
}

// Routes
async fn info() -> &'static str {
    "it works"
}

#[derive(Deserialize, Debug)]
struct SearchPayload {
    query: String,
}

async fn search(state: State<AppState>, payload: Json<SearchPayload>) -> impl IntoResponse {
    let query = payload.query.to_lowercase();
    let pattern: Vec<&str> = query.split("from ").collect();
    let pattern = pattern.get(1).unwrap_or(&"").trim();
    let pattern: Vec<&str> = pattern.split("where").collect();
    let pattern = pattern.get(0).unwrap_or(&"").trim();

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

async fn store_shard(state: State<AppState>, payload: Json<Shard>) -> impl IntoResponse {
    if shards::store_shard(&state.master_db, &payload)
        .await
        .is_err()
    {
        return "error registering shard".into_response();
    }

    "acknowledged".into_response()
}

async fn logs(state: State<AppState>) -> impl IntoResponse {
    // TODO: we should decide on mappings and the index automatically
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
