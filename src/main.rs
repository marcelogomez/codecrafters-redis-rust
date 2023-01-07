use redis_starter_rust::resp_to_debug_str;
#[allow(unused_imports)]
use std::env;
#[allow(unused_imports)]
use std::fs;
use std::net::SocketAddr;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::net::TcpStream;

#[tokio::main]
async fn main() {
    // Uncomment this block to pass the first stage
    let listener = TcpListener::bind("0.0.0.0:6379").await.unwrap();
    let mut connections = vec![];
    // TODO: Handle accept errors
    while let Ok((socket, addr)) = listener.accept().await {
        connections.push(tokio::task::spawn(async move {
            handle_client(socket, addr).await
        }));
    }

    // Wait for all streams to finish
    for connection in connections {
        if let Err(e) = connection.await {
            eprintln!("{}", e);
        }
    }
    // We can't add dependencies so we can't use futures::ForEachConcurrent
    // futures::stream::iter(connections.into_iter())
    //     .for_each_concurrent(0, |t| async {
    //         if let Err(e) = t.await {
    //             eprintln!("{}", e);
    //         }
    //     })
    //     .await;
}

async fn handle_client(mut socket: TcpStream, addr: SocketAddr) {
    eprintln!("Connected to addr {}", addr);
    let mut ping_command = [0u8; 14];
    while let Some(n) = socket.read(&mut ping_command).await.ok().filter(|&n| n > 0) {
        match n {
            // The PING command is an array with a single bulk string value that says ping:
            // * <- it's an array (1 byte)
            // 1\r\n <- length of the array is 1 (3 bytes)
            // $ <- First element is a bulk string (1 byte)
            // 4\r\n <- length of the string (3 bytes)
            // ping <- string (4 bytes)
            // \r\n <- terminate message (2 bytes)
            // *1\r\n$4\r\nping\r\n  (14 bytes)
            14 => {
                eprintln!("Enough bytes read! {}", resp_to_debug_str(ping_command));
                socket.write("+PONG\r\n".as_bytes()).await.unwrap();
                socket.flush().await.unwrap();
            }
            _ => {
                eprintln!("Not enough bytes read! {}", resp_to_debug_str(ping_command));
            }
        }
    }
}
