use axum::{
    Json, Router,
    extract::State,
    response::IntoResponse,
    routing::{get, post},
};
use serde::Deserialize;

use crate::{
    errors::AppError,
    messages::{Message, MessageLog},
    shards::{self, ShardMetadata, schedule_query},
    state::ApiState,
};

pub fn get_router(state: ApiState) -> Router<ApiState> {
    Router::new()
        .route("/", get(info))
        .route("/logs", post(logs))
        .route("/_shard", post(store_shard))
        .route("/search", post(search))
        .with_state(state.clone())
}

// Routes
async fn info() -> &'static str {
    "it works"
}

#[derive(Deserialize, Debug)]
struct SearchPayload {
    query: String,
}

async fn search(state: State<ApiState>, payload: Json<SearchPayload>) -> impl IntoResponse {
    let query = payload.query.to_lowercase();
    let pattern: Vec<&str> = query.split("from ").collect();
    let pattern = pattern.get(1).unwrap_or(&"").trim();
    let pattern: Vec<&str> = pattern.split("where").collect();
    let pattern = pattern.get(0).unwrap_or(&"").trim();

    if let Ok(results) = schedule_query(
        &state.master_db,
        state.commands.clone(),
        state.results.clone(),
        &pattern,
        &query,
    )
    .await
    {
        Json(results).into_response()
    } else {
        return AppError(anyhow::anyhow!("err")).into_response();
    }
}

async fn store_shard(state: State<ApiState>, payload: Json<ShardMetadata>) -> impl IntoResponse {
    if shards::store_shard(&state.master_db, &payload)
        .await
        .is_err()
    {
        return "error registering shard".into_response();
    }

    "acknowledged".into_response()
}

async fn logs(state: State<ApiState>) -> impl IntoResponse {
    // TODO: we should decide on mappings and the index automatically

    state.commands.lock().await.push(
        serde_json::to_string(&Message::Log(MessageLog {
            log: format!("log message"),
        }))
        .unwrap(),
    );

    "logged".into_response()
}
