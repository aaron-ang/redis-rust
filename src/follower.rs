use crate::db::Store;
use crate::server::{self, EMPTY_RDB_B64};

use anyhow::Result;
use base64::prelude::*;
use resp::{Decoder, Value};
use std::io::BufReader;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

pub struct Follower {
    port: u16,
    store: Store,
    offset: usize,
}

impl Follower {
    pub fn new(port: u16, store: Store) -> Self {
        Self {
            port,
            store,
            offset: 0,
        }
    }

    pub async fn handle_conn(&mut self, mut stream: TcpStream) -> Result<()> {
        send_ping(&mut stream).await?;
        check_ping_response(&mut stream).await?;
        send_replconf(&mut stream, self.port).await?;
        check_replconf_response(&mut stream).await?;
        send_psync(&mut stream).await?;
        self.check_psync_response(&mut stream).await?;

        let mut buf = [0; 1024];
        loop {
            let bytes_read = stream.read(&mut buf).await?;
            if bytes_read == 0 {
                eprintln!("Follower disconnected");
                break;
            }
            let mut decoder = Decoder::new(BufReader::new(&buf[..bytes_read]));
            while let Some(value) = decoder.decode().ok() {
                println!("Follower received value: {:?}", value);
                let (command, args) = server::extract_command(&value).unwrap();
                let response = match command.to_lowercase().as_str() {
                    "set" => {
                        server::handle_set(args, &self.store);
                        None
                    }
                    "get" => server::handle_get(args, &self.store),
                    "replconf" => self.handle_replconf(),
                    _ => Some(Value::Error("ERR unknown command".into())),
                };
                if let Some(response) = response {
                    stream.write_all(&response.encode()).await?;
                }
            }
        }
        Ok(())
    }

    async fn check_psync_response(&mut self, stream: &mut TcpStream) -> Result<()> {
        stream.flush().await?;

        let mut buf = [0; 1024];
        let mut bytes_read = stream.read(&mut buf).await?;
        let response = Decoder::new(BufReader::new(&buf[..bytes_read])).decode()?;
        if let Value::String(res_str) = response {
            let sync = res_str.split_ascii_whitespace().collect::<Vec<&str>>();
            if sync.len() < 3 {
                return Err(anyhow::anyhow!("Unexpected response to PSYNC: {res_str}"));
            }
            self.offset = sync.last().unwrap().parse::<usize>()?;
        } else {
            return Err(anyhow::anyhow!("Unexpected response to PSYNC"));
        }

        // verify RDB file
        let mut rdb = BASE64_STANDARD.decode(EMPTY_RDB_B64).unwrap();
        let mut msg_bytes = format!("${}\r\n", rdb.len()).into_bytes();
        msg_bytes.append(&mut rdb);

        bytes_read = stream.read(&mut buf).await?;
        if &buf[..bytes_read] != msg_bytes {
            return Err(anyhow::anyhow!("Unexpected RDB file"));
        }
        Ok(())
    }

    fn handle_replconf(&self) -> Option<Value> {
        Some(Value::Array(vec![
            Value::Bulk("REPLCONF".into()),
            Value::Bulk("ACK".into()),
            Value::Bulk(self.offset.to_string()),
        ]))
    }
}

async fn send_ping(stream: &mut TcpStream) -> Result<()> {
    let ping = Value::Array(vec![Value::Bulk("PING".into())]);
    stream.write_all(&ping.encode()).await?;
    Ok(())
}

async fn check_ping_response(stream: &mut TcpStream) -> Result<()> {
    let mut buf = [0; 1024];
    stream.flush().await?;
    let bytes_read = stream.read(&mut buf).await?;
    let response = Decoder::new(BufReader::new(&buf[..bytes_read])).decode()?;
    match response {
        Value::String(res_str) if res_str == "PONG" => Ok(()),
        _ => Err(anyhow::anyhow!("Unexpected response to PING")),
    }
}

async fn send_replconf(stream: &mut TcpStream, my_port: u16) -> Result<()> {
    let replconf_port = Value::Array(vec![
        Value::Bulk("REPLCONF".into()),
        Value::Bulk("listening-port".into()),
        Value::Bulk(my_port.to_string()),
    ]);
    stream.write_all(&replconf_port.encode()).await?;
    let replconf_capa = Value::Array(vec![
        Value::Bulk("REPLCONF".into()),
        Value::Bulk("capa".into()),
        Value::Bulk("eof".into()),
    ]);
    stream.write_all(&replconf_capa.encode()).await?;
    Ok(())
}

async fn check_replconf_response(stream: &mut TcpStream) -> Result<()> {
    let mut buf = [0; 1024];
    stream.flush().await?;
    let mut num_ok = 0;

    // check that 2 OK's are received
    while num_ok < 2 {
        let bytes_read = stream.read(&mut buf).await?;
        let mut decoder = Decoder::new(BufReader::new(&buf[..bytes_read]));
        while let Some(response) = decoder.decode().ok() {
            match response {
                Value::String(res_str) if res_str == "OK" => num_ok += 1,
                _ => return Err(anyhow::anyhow!("Unexpected response to REPLCONF")),
            }
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
