use redis_starter_rust::parse_bulk_string_array;
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
    eprintln!("Connected to client {}", addr);
    let mut command_buf = [0u8; 4096];
    loop {
        match socket.read(&mut command_buf).await {
            Ok(0) => {
                eprintln!("Connection terminated by client {}", addr);
                break;
            }
            Ok(n) => match handle_command(&command_buf[..n]) {
                Ok(resp) => {
                    socket.write(resp.as_bytes()).await.unwrap();
                    socket.flush().await.unwrap();
                }
                Err(e) => {
                    eprintln!("Error while handling command\n{}", e);
                }
            },
            Err(e) => {
                eprintln!("Error while reading data from client {}\n{}", addr, e);
                break;
            }
        }
    }
}

fn handle_command(command_buf: &[u8]) -> Result<String, String> {
    let (command, _) = parse_bulk_string_array(&command_buf)?;

    if command.is_empty() {
        return Err("Empty command".into());
    }

    Ok(gen_response(command[0].as_str(), &command[1..])?)
}

fn gen_response(command: &str, args: &[String]) -> Result<String, String> {
    match command {
        "ECHO" | "echo" => {
            if args.is_empty() {
                return Err("No message to ECHO".to_string());
            }
            let message = &args[0];
            Ok(format!("${}\r\n{}\r\n", message.len(), message))
        }
        "PING" | "ping" => Ok("+PONG\r\n".to_string()),
        c => Err(format!("Unknown command {}", c)),
    }
}
