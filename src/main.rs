use anyhow::Result;
use object_storage::get_s3_client;
use std::{collections::HashMap, sync::Arc};
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
mod web;
mod worker;

use db::connect_with_options;
use schema::create_shards_table;
use state::ApiState;

use clap::Parser;

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Name of the person to greet
    #[arg(short, long)]
    mode: String,
}

const BUCKET: &'static str = "logs";

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    println!("Running in mode: {}", &args.mode);

    if args.mode == "worker" {
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
