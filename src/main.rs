use anyhow::Result;
use axum::Router;
use object_storage::get_s3_client;
use std::{collections::HashMap, env, sync::Arc};
use tokio::sync::Mutex;
use worker::init_worker;

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
use schema::create_shards_table;
use state::ApiState;

const BUCKET: &'static str = "logs";

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
