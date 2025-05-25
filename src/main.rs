use std::{env, time::Duration};

use aws_sdk_s3::config::{Credentials, Region};

use anyhow::Result;
use axum::{
    Router, debug_handler,
    extract::State,
    response::IntoResponse,
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Sqlite, SqlitePool};

#[derive(Clone)]
struct AppState {
    client: aws_sdk_s3::Client,
    db: SqlitePool,
}

struct AppError(anyhow::Error);

const BUCKET: &'static str = "logs";

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Task {
    key: String,
    status: String,
}

use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode};
use std::str::FromStr;

async fn connect_with_options(db_path: &str) -> Result<SqlitePool, sqlx::Error> {
    let options = SqliteConnectOptions::from_str(&format!("sqlite:{}", db_path))?
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(sqlx::sqlite::SqliteSynchronous::Normal)
        .busy_timeout(Duration::from_secs(30));

    SqlitePool::connect_with(options).await
}

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

#[tokio::main]
async fn main() -> Result<()> {
    let key_id = "root".to_string();
    let secret_key = "changeme".to_string();
    let cred = Credentials::new(key_id, secret_key, None, None, "loaded-from-custom-env");
    let store_url = "http://localhost:9000";

    let mut temp_path = std::env::temp_dir();
    temp_path.push(format!("sqlite_temp_{}.db", uuid::Uuid::new_v4()));
    let database_url = format!("sqlite:{}", temp_path.display());

    println!("{}", &database_url);

    let pool = connect_with_options(&database_url).await?;

    sqlx::query("PRAGMA journal_mode = WAL")
        .execute(&pool)
        .await?;

    sqlx::query("SELECT 1 = 1").execute(&pool).await?;

    create_logs_table(&pool).await?;

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
        db: pool,
    };

    print!("api started");

    start_app(state.clone()).await?;

    Ok(())
}

async fn info() -> &'static str {
    "it works"
}

fn get_key(stream: &str, time: time::UtcDateTime) -> String {
    let index = format!("logs");
    let block = uuid::Uuid::new_v4();
    let timestamp = time.unix_timestamp_nanos();

    return format!("{index}/{stream}/{timestamp}.{block}");
}

fn get_current_key(stream: &str) -> String {
    let now = time::UtcDateTime::now();

    return get_key(stream, now);
}

impl IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        todo!()
    }
}

// save log to in-memory sqlite
#[debug_handler]
async fn logs(state: State<AppState>) -> impl IntoResponse {
    // let key = get_current_key("logs");

    let id = uuid::Uuid::new_v4().to_string();
    let timestamp = time::UtcDateTime::now();
    let message = format!("http log {}", timestamp);

    if let Err(err) = sqlx::query("INSERT INTO logs (id, timestamp, message) VALUES (?1, ?2, ?3)")
        .bind(id)
        .bind(timestamp.to_string())
        .bind(message)
        .execute(&state.db)
        .await
    {
        return AppError(anyhow::anyhow!(err)).into_response();
    }

    // parse schema name

    // let values = format!("{}", time::UtcDateTime::now());

    // if let Err(err) = state
    //     .client
    //     .put_object()
    //     .bucket(BUCKET)
    //     .body(ByteStream::from(values.into_bytes()))
    //     .key(key)
    //     .send()
    //     .await
    // {
    //     return AppError(anyhow::anyhow!(err)).into_response();
    // }

    "logged".into_response()
}

async fn start_app(state: AppState) -> Result<()> {
    let app = Router::new()
        .route("/", get(info))
        .route("/logs", post(logs))
        .with_state(state.clone());

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;
    axum::serve(listener, app).await?;

    Ok(())
}
