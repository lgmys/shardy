use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

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

                    w.write_all(&command.into_bytes())
                        .await
                        .expect("failed to write data to socket");
                }
            }
        });

        tokio::spawn(async move {
            println!("worker connected");

            let mut buf = vec![0; 1024];

            loop {
                let n = r
                    .read(&mut buf)
                    .await
                    .expect("failed to read data from socket");

                if n != 0 {
                    println!("read data from worker");
                }
            }
        });
    }
}
