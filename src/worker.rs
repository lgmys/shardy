use crate::{BUCKET, messages::Message, shards::execute_shard_query, state::WorkerState};

use anyhow::Result;
use tokio::io;

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

    let (r, w) = stream.split();

    println!("connected coordinator");

    loop {
        // Wait for the socket to be readable
        r.readable().await?;

        // Creating the buffer **after** the `await` prevents it from
        // being stored in the async task.
        let mut buf = [0; 4096];

        // Try to read data, this may still fail with `WouldBlock`
        // if the readiness event is a false positive.
        match r.try_read(&mut buf) {
            Ok(0) => {
                continue;
            }
            Ok(n) => {
                let message_string = String::from_utf8(buf[..n].into())?;
                println!("{:?}", &message_string);

                let message: Message = serde_json::from_str(&message_string)?;

                match message {
                    Message::Log(message_log) => {
                        dbg!(message_log);

                        create_log(&state).await;
                    }
                    Message::SearchRequest(message_search_request) => {
                        dbg!(&message_search_request);

                        let shard_results = execute_shard_query(
                            &state.client,
                            BUCKET,
                            &message_search_request.shard,
                            &message_search_request.query,
                        )
                        .await?;

                        dbg!(shard_results);

                        if w.try_write(&format!("omg").into_bytes()).is_err() {
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
                return Err(e.into());
            }
        }
    }
}
