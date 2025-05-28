use anyhow::Result;
use aws_sdk_s3::{
    Client,
    config::{Credentials, Region},
};
use axum::Router;
use std::{env, sync::Arc};
use time::format_description;
use tokio::sync::Mutex;

mod coordinator;
mod db;
mod errors;
mod messages;
mod object_storage;
mod routes;
mod schema;
mod shards;
mod state;
mod sync;
mod worker;

use db::connect_with_options;
use routes::get_router;
use schema::{create_logs_table, create_shards_table};
use state::{ApiState, WorkerState};

const BUCKET: &'static str = "logs";

fn get_s3_client() -> Client {
    let key_id = "root".to_string();
    let secret_key = "changeme".to_string();

    let cred = Credentials::new(key_id, secret_key, None, None, "loaded-from-custom-env");
    let store_url = "http://localhost:9000";

    let s3_config = aws_sdk_s3::config::Builder::new()
        .endpoint_url(store_url)
        .credentials_provider(cred)
        .region(Region::new("eu-central-1"))
        .behavior_version_latest()
        .force_path_style(true)
        .build();

    return aws_sdk_s3::Client::from_conf(s3_config);
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    let subcommand = args.get(2).unwrap_or(&"api".to_owned()).clone();
    println!("Running in mode: {}", &subcommand);

    let cwd = std::env::current_dir()?;

    let mut master_path = cwd.clone();
    master_path.push("./master.db");
    let master_path = format!("sqlite:{}", master_path.display());
    let master_pool = connect_with_options(&master_path).await?;

    create_shards_table(&master_pool).await?;

    sqlx::query("SELECT 1 = 1").execute(&master_pool).await?;

    let client = get_s3_client();

    let commands = Arc::new(Mutex::new(vec![]));

    let state = ApiState {
        client: client.clone(),
        master_db: master_pool,
        commands,
    };

    if subcommand == "worker" {
        start_worker().await?;
    } else {
        tokio::spawn(coordinator::start_coordinator(state.clone()));
        start_web(state.clone()).await?;
    }

    Ok(())
}

async fn start_web(state: ApiState) -> Result<()> {
    let router = Router::new()
        .merge(get_router(state.clone()))
        .with_state(state.clone());

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;
    axum::serve(listener, router).await?;

    Ok(())
}

async fn start_worker() -> Result<()> {
    let client = get_s3_client();

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

    create_logs_table(&shard_pool).await?;

    sqlx::query("SELECT 1 = 1").execute(&shard_pool).await?;

    let state = WorkerState {
        client: client.clone(),
        shard_db: shard_pool,
        db_file: shard_path.clone(),
        shard_filename: shard_filename.clone(),
        shard_id: shard_id.clone(),
        shard_start_time: shard_start_range.to_string(),
    };

    // NOTE: periodically syncs temp log file to s3
    tokio::spawn(sync::run_sync_periodically(state.clone()));
    let _ = tokio::spawn(worker::start(state.clone())).await?;

    Ok(())
}
