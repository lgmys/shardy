use anyhow::Result;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::{Credentials, Region};
use tempfile::NamedTempFile;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

use crate::BUCKET;

pub fn get_s3_client() -> Client {
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

pub async fn upload_db_to_s3(
    client: &Client,
    db_file: &str,
    key: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let body = tokio::fs::read(db_file).await?;

    client
        .put_object()
        .bucket(BUCKET)
        .key(key)
        .body(body.into())
        .send()
        .await?;

    println!("Database synced to S3: s3://{}/{}", BUCKET, key);
    Ok(())
}

/// Download SQLite database from S3 to a temporary file
pub async fn download_database(s3_client: &Client, key: &str) -> Result<NamedTempFile> {
    // Get object from S3
    let response = s3_client
        .get_object()
        .bucket(BUCKET)
        .key(key)
        .send()
        .await?;

    // Create temporary file
    let temp_file = NamedTempFile::new()?;

    // Stream data from S3 to temp file
    let mut stream = response.body.into_async_read();
    let mut file_handle = File::from_std(temp_file.reopen()?);

    tokio::io::copy(&mut stream, &mut file_handle).await?;
    file_handle.flush().await?;

    Ok(temp_file)
}
