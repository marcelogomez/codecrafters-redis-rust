use redis_starter_rust::BulkString;
use redis_starter_rust::RESPValue;
use std::collections::HashMap;
use std::convert::TryInto;
#[allow(unused_imports)]
use std::env;
#[allow(unused_imports)]
use std::fs;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::RwLock;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::net::TcpStream;

type Table = Arc<RwLock<HashMap<String, String>>>;

#[tokio::main]
async fn main() {
    // Uncomment this block to pass the first stage
    let listener = TcpListener::bind("0.0.0.0:6379").await.unwrap();
    let mut connections = vec![];
    let table = Arc::new(RwLock::new(HashMap::new()));

    // TODO: Handle accept errors
    while let Ok((socket, addr)) = listener.accept().await {
        let table = Arc::clone(&table);
        connections.push(tokio::task::spawn(async move {
            handle_client(socket, addr, table).await
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

async fn handle_client(mut socket: TcpStream, addr: SocketAddr, table: Table) {
    eprintln!("Connected to client {}", addr);
    let mut command_buf = [0u8; 4096];
    loop {
        match socket.read(&mut command_buf).await {
            Ok(0) => {
                eprintln!("Connection terminated by client {}", addr);
                break;
            }
            Ok(n) => match handle_command(&command_buf[..n], table.clone()) {
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

fn handle_command(command_buf: &[u8], table: Table) -> Result<String, String> {
    let (resp_value, _) = RESPValue::parse(command_buf).map_err(|e| format!("{:?}", e))?;
    // TODO: Make this easier
    let command: Option<Vec<BulkString>> = resp_value.try_into().unwrap();
    let command = command.unwrap_or_default();

    if command.is_empty() {
        return Err("Empty command".into());
    }

    Ok(gen_response(
        command[0].as_ref().unwrap(),
        &command[1..],
        table,
    )?)
}

fn gen_response(command: &String, args: &[BulkString], table: Table) -> Result<String, String> {
    match command.as_str() {
        "ECHO" | "echo" => {
            if args.is_empty() {
                return Err("No message to ECHO".to_string());
            }
            // TODO: Make this easier
            let message = args[0].as_ref().map(|s| s.as_str()).unwrap_or_default();
            Ok(format!("${}\r\n{}\r\n", message.len(), message))
        }
        "SET" | "set" => {
            let mut args = args.into_iter();
            // TODO: Make this easier
            let key = args
                .next()
                .and_then(|s| s.as_deref())
                .ok_or_else(|| "No key specified for set operation".to_string())?;
            let value = args
                .next()
                .and_then(|s| s.as_deref())
                .ok_or_else(|| "No key specified for set operation".to_string())?;

            match table.write() {
                Ok(mut t) => {
                    t.insert(key.to_string(), value.to_string());
                    Ok(format!("+OK\r\n"))
                }
                Err(e) => {
                    eprintln!("Failed to acquire lock for table {}", e);
                    // TODO: Make error handling simpler
                    Ok(format!("$-1\r\n"))
                }
            }
        }
        "GET" | "get" => {
            let mut args = args.into_iter();
            // TODO: Make this easier
            let key = args
                .next()
                .and_then(|s| s.as_deref())
                .ok_or_else(|| "No key specified for get operation".to_string())?;

            match table.read() {
                Ok(t) => match t.get(key) {
                    Some(value) => Ok(format!("${}\r\n{}", value.len(), value)),
                    None => Ok(format!("$-1\r\n")),
                },
                // TODO: Read up on error handling
                Err(e) => Err(format!("Failed to acquire lock for table {}", e)),
            }
        }
        "PING" | "ping" => Ok("+PONG\r\n".to_string()),
        c => Err(format!("Unknown command {}", c)),
    }
}
