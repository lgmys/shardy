use std::time::Duration;

use anyhow::Result;

use crate::{
    get_shard,
    shards::{checkpoint_and_sync, post_shard},
    state::WorkerState,
};

pub async fn regenerate_shard(state: WorkerState) -> Result<()> {
    let mut interval = tokio::time::interval(Duration::from_secs(60));

    loop {
        interval.tick().await;
        println!("regenerate");

        let (pool, shard_filename, s3_path) = {
            let shard = { state.shard.lock().await };
            (
                shard.pool.clone(),
                shard.shard_filename.clone(),
                shard.metadata.s3_path.clone(),
            )
        };

        if checkpoint_and_sync(&pool, &state.client, &shard_filename, &s3_path)
            .await
            .is_err()
        {
            println!("could not sync")
        }

        let new_shard = get_shard().await?;

        {
            let mut shard = { state.shard.lock().await };
            shard.shard_filename = new_shard.shard_filename;
            shard.pool = new_shard.pool;
            shard.metadata = new_shard.metadata;
        }
    }
}

pub async fn run_sync_periodically(state: WorkerState) -> Result<()> {
    let mut interval = tokio::time::interval(Duration::from_secs(30));

    loop {
        interval.tick().await;
        println!("periodical");

        let (pool, s3_path, shard_filename) = {
            let shard = state.shard.lock().await;

            (
                shard.pool.clone(),
                shard.metadata.s3_path.clone(),
                shard.shard_filename.clone(),
            )
        };

        if checkpoint_and_sync(&pool, &state.client, &shard_filename, &s3_path)
            .await
            .is_err()
        {
            println!("could not sync")
        }

        let metadata = {
            let shard = state.shard.lock().await;
            shard.metadata.clone()
        };

        post_shard(&metadata).await?;
    }
}
