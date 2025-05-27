use axum::{
    Json, Router,
    extract::State,
    response::IntoResponse,
    routing::{get, post},
};
use serde::Deserialize;

use crate::{
    BUCKET,
    errors::AppError,
    shards::{self, Shard, execute_query},
    state::AppState,
};

pub fn get_router(state: AppState) -> Router<AppState> {
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
