use anyhow::Result;
use aws_sdk_s3::{
    Client,
    config::{Credentials, Region},
};
use axum::Router;
use shards::{ShardHandle, ShardMetadata, new_shard};
use std::{collections::HashMap, env, sync::Arc};
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
    let search_results = Arc::new(Mutex::new(HashMap::new()));

    let state = ApiState {
        client: client.clone(),
        master_db: master_pool,
        commands,
        results: search_results,
    };

    if subcommand == "worker" {
        init_worker().await?;
    } else {
        tokio::spawn(coordinator::start_coordinator(state.clone()));
        init_web(state.clone()).await?;
    }

    Ok(())
}

async fn init_web(state: ApiState) -> Result<()> {
    let router = Router::new()
        .merge(get_router(state.clone()))
        .with_state(state.clone());

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;
    axum::serve(listener, router).await?;

    Ok(())
}

async fn init_worker() -> Result<()> {
    let client = get_s3_client();

    let state = WorkerState {
        client: client.clone(),
        shard: Arc::new(Mutex::new(new_shard().await?)),
    };

    // NOTE: periodically syncs temp log file to s3

    tokio::spawn(sync::run_sync_periodically(state.clone()));
    tokio::spawn(sync::regenerate_shard(state.clone()));

    let _ = tokio::spawn(worker::start(state.clone())).await?;

    Ok(())
}
