use crate::db::Store;

use anyhow::Result;
use resp::{Decoder, Value};
use std::io::BufReader;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::{sleep, Duration},
};

pub struct Follower {
    port: u16,
    store: Store,
}

impl Follower {
    pub fn new(port: u16, store: Store) -> Self {
        Self { port, store }
    }

    pub async fn handle_conn(&self, mut stream: TcpStream) -> Result<()> {
        send_ping(&mut stream).await?;
        check_ping_response(&mut stream).await?;

        send_replconf_port(&mut stream, self.port).await?;
        send_replconf_capa(&mut stream).await?;
        check_replconf_response(&mut stream).await?;

        send_psync(&mut stream).await?;

        loop {
            sleep(Duration::from_millis(10)).await;

            let mut buf = [0; 1024];
            let bytes_read = stream.read(&mut buf).await?;
            let bufreader = BufReader::new(&buf[..bytes_read]);
            let mut decoder = Decoder::new(bufreader);

            let _ = match decoder.decode() {
                Ok(_) => {
                    // let (command, args) = extract_command(&value).unwrap();
                    // match command.to_lowercase().as_str() {
                    //     "ping" => Some(Value::String("PONG".into())),
                    //     "echo" => Some(args.first().unwrap().clone()),
                    //     "set" => handle_set(args, &self.store).map(|v| {
                    //         println!("Sent to followers: {:?}", value);
                    //         v
                    //     }),
                    //     "get" => handle_get(args, &self.store),
                    //     "info" => handle_info(self.role),
                    //     "replconf" => Some(Value::String("OK".into())),
                    //     "psync" => handle_psync(&mut stream, &mut replication).await,
                    //     _ => Some(Value::Error("ERR unknown command".into())),
                    // }
                }
                Err(e) => {
                    eprintln!("Error decoding value: {:?}", e);
                }
            };
        }
    }
}

async fn send_ping(stream: &mut TcpStream) -> Result<()> {
    let ping = Value::Array(vec![Value::Bulk("PING".into())]);
    stream.write_all(&ping.encode()).await?;
    Ok(())
}

async fn check_ping_response(stream: &mut TcpStream) -> Result<()> {
    stream.flush().await?;
    let mut buf = [0; 1024];
    stream.read(&mut buf).await?;
    let response = Decoder::new(BufReader::new(&buf[..])).decode()?;
    if let Value::String(res_str) = response {
        if res_str != "PONG" {
            return Err(anyhow::anyhow!("Unexpected response to PING: {res_str}"));
        }
    } else {
        return Err(anyhow::anyhow!("Unexpected response to PING"));
    }
    Ok(())
}

async fn send_replconf_port(stream: &mut TcpStream, my_port: u16) -> Result<()> {
    let replconf_port = Value::Array(vec![
        Value::Bulk("REPLCONF".into()),
        Value::Bulk("listening-port".into()),
        Value::Bulk(my_port.to_string()),
    ]);
    stream.write_all(&replconf_port.encode()).await?;
    Ok(())
}

async fn send_replconf_capa(stream: &mut TcpStream) -> Result<()> {
    let replconf_capa = Value::Array(vec![
        Value::Bulk("REPLCONF".into()),
        Value::Bulk("capa".into()),
        Value::Bulk("eof".into()),
    ]);
    stream.write_all(&replconf_capa.encode()).await?;
    Ok(())
}

async fn check_replconf_response(stream: &mut TcpStream) -> Result<()> {
    stream.flush().await?;
    let mut buf = [0; 1024];
    stream.read(&mut buf).await?;

    // check that 2 OK's are received
    for _ in 0..2 {
        let response = Decoder::new(BufReader::new(&buf[..])).decode()?;
        match response {
            Value::String(res_str) if res_str == "OK" => continue,
            _ => return Err(anyhow::anyhow!("Unexpected response to REPLCONF")),
        }
    }
    Ok(())
}

async fn send_psync(stream: &mut TcpStream) -> Result<()> {
    let psync = Value::Array(vec![
        Value::Bulk("PSYNC".into()),
        Value::Bulk("?".into()),
        Value::Bulk("-1".to_string()),
    ]);
    stream.write_all(&psync.encode()).await?;
    Ok(())
}
