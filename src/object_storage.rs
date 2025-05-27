use anyhow::Result;
use aws_sdk_s3::Client;
use tempfile::NamedTempFile;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

pub async fn upload_db_to_s3(
    client: &Client,
    db_path: &str,
    bucket: &str,
    key: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let body = tokio::fs::read(db_path).await?;

    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(body.into())
        .send()
        .await?;

    println!("Database synced to S3: s3://{}/{}", bucket, key);
    Ok(())
}

/// Download SQLite database from S3 to a temporary file
pub async fn download_database(
    s3_client: &Client,
    bucket: &str,
    key: &str,
) -> Result<NamedTempFile> {
    // Get object from S3
    let response = s3_client
        .get_object()
        .bucket(bucket)
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
