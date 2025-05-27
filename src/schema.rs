use sqlx::SqlitePool;

pub async fn create_logs_table(pool: &SqlitePool) -> Result<(), sqlx::Error> {
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

pub async fn create_shards_table(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
       CREATE TABLE IF NOT EXISTS shards (
           id TEXT PRIMARY KEY,
           name TEXT NOT NULL,
           s3_path TEXT NOT NULL,
           timestamp DATETIME NOT NULL
       )
       "#,
    )
    .execute(pool)
    .await?;

    Ok(())
}
