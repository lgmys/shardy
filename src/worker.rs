use crate::{
    BUCKET,
    messages::{Message, MessageSearchResponse},
    shards::execute_shard_query,
    state::WorkerState,
};

use anyhow::Result;
use tokio::io::{self, AsyncReadExt};

pub async fn create_log(state: &WorkerState) {
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
        println!("error {}", err);
    } else {
        println!("log created");
    }
}

pub async fn start(state: WorkerState) -> Result<()> {
    let socket = tokio::net::TcpSocket::new_v4()?;
    let mut stream = socket.connect("127.0.0.1:6666".parse()?).await?;

    let (mut r, w) = stream.split();

    println!("connected coordinator");

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
                        create_log(&state).await;
                    }
                    Message::SearchRequest(message_search_request) => {
                        let shard_results = match execute_shard_query(
                            &state.client,
                            BUCKET,
                            &message_search_request.shard,
                            &message_search_request.query,
                        )
                        .await
                        {
                            Ok(shard_results) => shard_results,
                            Err(_) => {
                                println!("query failure");

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
