use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::messages::Message;
use crate::state::ApiState;
use anyhow::Result;

pub async fn start_coordinator(state: ApiState) -> Result<()> {
    let listener = TcpListener::bind("0.0.0.0:6666").await?;

    loop {
        let (socket, _) = listener.accept().await?;
        let (mut r, mut w) = socket.into_split();

        let state_copy = state.clone();

        tokio::spawn(async move {
            loop {
                let command = { state_copy.commands.lock().await.pop() };

                if let Some(command) = command {
                    println!("new command: {}", command);

                    let buf = command.into_bytes();

                    w.write_all(&buf.len().to_be_bytes())
                        .await
                        .expect("failed to write length");
                    w.write_all(&buf)
                        .await
                        .expect("failed to write data to socket");

                    w.flush().await.expect("could not flush");
                }
            }
        });

        let state_copy = state.clone();

        tokio::spawn(async move {
            println!("worker connected");

            loop {
                let mut len_bytes = [0u8; 4];

                if r.read_exact(&mut len_bytes).await.is_err() {
                    continue;
                }

                let len = u32::from_be_bytes(len_bytes) as usize;

                let mut message = vec![0; len];

                match r.read_exact(&mut message).await {
                    Ok(n) => {
                        if n == 0 {
                            continue;
                        }

                        let message =
                            String::from_utf8(message.into()).expect("could not read string");
                        println!("{:?}", &message);

                        let message: Message =
                            serde_json::from_str(&message).expect("could not parse message");

                        match message {
                            Message::SearchResponse(message_search_response) => {
                                state_copy.results.lock().await.insert(
                                    message_search_response.id.clone(),
                                    message_search_response.clone(),
                                );
                            }
                            _ => {}
                        };
                    }
                    Err(_) => {
                        continue;
                    }
                }
            }
        });
    }
}
