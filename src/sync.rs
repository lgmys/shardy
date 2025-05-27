use std::time::Duration;

use anyhow::Result;

use crate::{
    BUCKET,
    shards::{Shard, checkpoint_and_sync, post_shard},
    state::AppState,
};

pub async fn run_sync_periodically(state: AppState) -> Result<()> {
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
