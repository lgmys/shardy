use anyhow::Result;
use object_storage::get_s3_client;
use std::{collections::HashMap, env, sync::Arc};
use tokio::sync::Mutex;
use worker::init_worker;

mod coordinator;
mod db;
mod errors;
mod messages;
mod object_storage;
mod schema;
mod shards;
mod state;
mod sync;
mod web;
mod worker;

use db::connect_with_options;
use schema::create_shards_table;
use state::ApiState;

const BUCKET: &'static str = "logs";

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    let subcommand = args.get(2).unwrap_or(&"api".to_owned()).clone();
    println!("Running in mode: {}", &subcommand);

    if subcommand == "worker" {
        init_worker().await?;
    } else {
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
        tokio::spawn(coordinator::start_coordinator(state.clone()));
        web::init_web(state.clone()).await?;
    }

    Ok(())
}
