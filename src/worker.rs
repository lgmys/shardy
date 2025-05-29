use std::time::Duration;

use crate::{
    get_s3_client,
    messages::{Message, MessageSearchResponse},
    shards::Shard,
};

use anyhow::Result;
use tokio::io::{self, AsyncReadExt};

pub async fn init_worker() -> Result<()> {
    let _ = tokio::spawn(start()).await?;

    Ok(())
}

pub async fn start() -> Result<()> {
    let client = get_s3_client();

    let socket = tokio::net::TcpSocket::new_v4()?;
    let mut stream = socket.connect("127.0.0.1:6666".parse()?).await?;

    let (mut r, w) = stream.split();

    println!("connected coordinator");

    let shard = Shard::new(client).await?;

    let shard_clone = shard.clone();

    let mut i = tokio::time::interval(Duration::from_secs(15));

    tokio::spawn(async move {
        loop {
            i.tick().await;

            println!("sync to object storage");

            shard_clone
                .sync_shard_to_storage()
                .await
                .expect("error syncing shard");
        }
    });

    loop {
        // Wait for the socket to be readable
        r.readable().await.expect("not readable");

        let mut len_bytes = [0u8; 4];

        match r.read_exact(&mut len_bytes).await {
            Ok(0) => {
                println!("disconnected");
                break;
            }
            Err(e) => {
                eprintln!("{}", e);
            }
            _ => {}
        };

        let len = u32::from_be_bytes(len_bytes) as usize;

        let mut buf = vec![0; len];

        // Try to read data, this may still fail with `WouldBlock`
        // if the readiness event is a false positive.
        match r.read_exact(&mut buf).await {
            Ok(n) => {
                if n == 0 {
                    continue;
                }

                let message_string = String::from_utf8(buf[..n].into())?;

                let message: Message =
                    serde_json::from_str(&message_string).expect("could not parse message");

                match message {
                    Message::Log(message_log) => {
                        shard.create_log().await;
                    }
                    Message::SearchRequest(message_search_request) => {
                        let shard_results = match shard
                            .execute_shard_query(
                                &message_search_request.shard,
                                &message_search_request.query,
                            )
                            .await
                        {
                            Ok(shard_results) => shard_results,
                            Err(e) => {
                                println!(
                                    "query failure, shard: {}, error: {}",
                                    &message_search_request.shard.id, e
                                );

                                vec![]
                            }
                        };

                        let search_response = MessageSearchResponse {
                            id: message_search_request.id,
                            payload: shard_results,
                        };

                        let search_response = Message::SearchResponse(search_response);
                        let search_response = serde_json::to_vec(&search_response)?;

                        if w.try_write(&search_response.len().to_be_bytes()).is_err() {
                            println!("error");
                        }
                        if w.try_write(&search_response).is_err() {
                            println!("error");
                        }
                    }
                    _ => {}
                }
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                continue;
            }
            Err(e) => {
                eprintln!("{}", e);
                break;
            }
        }
    }

    Ok(())
}
