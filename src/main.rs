#[allow(unused_imports)]
use std::env;
#[allow(unused_imports)]
use std::fs;
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() {
    // Uncomment this block to pass the first stage
    let mut listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    match listener.accept().await {
        Ok((mut socket, addr)) => {
            println!("Connected to addr {}", addr);
            let mut ping_command = [0u8; 15];
            // The PING command is an array with a single bulk string value that says ping:
            // * <- it's an array (1 byte)
            // 1\r\n <- length of the array is 1 (3 bytes)
            // $ <- First element is a bulk string (1 byte)
            // 4\r\n <- length of the string (3 bytes)
            // ping <- string (4 bytes)
            // \r\n <- terminate message (3 bytes)
            // *1\r\n$4\r\nping\r\n  (15 bytes)
            match socket.read(&mut ping_command).await {
                Ok(15) => {
                },
                Ok(_) => {
                    eprintln!("Not enough bytes read!");
                },
                Err(e) => {
                    eprintln!("Failed to read from socket {:?}", e);
                },
            }
        }
        Err(e) => println!("couldn't accept client: {:?}", e),
    }
}
