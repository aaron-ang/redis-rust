use crate::db::Store;
use crate::server::{self, EMPTY_RDB_B64};
use crate::util::Command;

use anyhow::Result;
use base64::prelude::*;
use resp::{Decoder, Value};
use std::io::{BufReader, ErrorKind};
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

        let mut buf = vec![0; 1024];
        loop {
            let bytes_read = stream.read(&mut buf).await?;
            if bytes_read == 0 {
                eprintln!("Follower disconnected");
                break;
            }
            let mut decoder = Decoder::new(BufReader::new(&buf[..bytes_read]));
            loop {
                match decoder.decode() {
                    Ok(value) => {
                        if let Ok((command, args)) = server::extract_command(&value) {
                            match command {
                                Command::PING => {}
                                Command::SET => {
                                    server::handle_set(args, &self.store);
                                }
                                Command::GET => {
                                    server::handle_get(args, &self.store);
                                }
                                Command::REPLCONF => self.handle_replconf(&mut stream).await?,
                                _ => eprintln!("Unknown command: {}", command),
                            }
                            self.offset += value.encode().len();
                        }
                    }
                    Err(e) => match e.kind() {
                        ErrorKind::UnexpectedEof => break,
                        ErrorKind::InvalidInput => continue,
                        _ => eprintln!("Error: {:?}", e),
                    },
                }
            }
        }
        Ok(())
    }

    async fn check_psync_response(&mut self, stream: &mut TcpStream) -> Result<()> {
        let mut psync_buf = vec![0; 56]; // length of PSYNC response
        stream.read_exact(&mut psync_buf).await?;
        let mut decoder = Decoder::new(BufReader::new(&psync_buf[..]));
        if let Value::String(res_str) = decoder.decode().unwrap() {
            let sync = res_str.split_ascii_whitespace().collect::<Vec<&str>>();
            if sync.len() < 3 || sync[0] != "FULLRESYNC" {
                return Err(anyhow::anyhow!("Unexpected response to PSYNC: {}", res_str));
            }
            self.offset = sync.last().unwrap().parse::<usize>()?;
        } else {
            return Err(anyhow::anyhow!("Unexpected response to PSYNC"));
        }

        // verify RDB file
        let mut rdb = BASE64_STANDARD.decode(EMPTY_RDB_B64).unwrap();
        let mut msg_bytes = format!("${}\r\n", rdb.len()).into_bytes();
        msg_bytes.append(&mut rdb);
        let mut rdb_buf = vec![0; msg_bytes.len()];
        stream.read_exact(&mut rdb_buf).await?;

        if rdb_buf != msg_bytes.as_slice() {
            eprintln!("Received RDB: {:?}", String::from_utf8_lossy(&rdb_buf));
            eprintln!(
                "Expected RDB: {:?}",
                String::from_utf8_lossy(msg_bytes.as_slice())
            );
            return Err(anyhow::anyhow!("Unexpected RDB file"));
        }
        Ok(())
    }

    async fn handle_replconf(&self, stream: &mut TcpStream) -> Result<()> {
        let replconf_ack = Value::Array(vec![
            Value::Bulk("REPLCONF".into()),
            Value::Bulk("ACK".into()),
            Value::Bulk(self.offset.to_string()),
        ]);
        stream.write_all(&replconf_ack.encode()).await?;
        Ok(())
    }
}

async fn send_ping(stream: &mut TcpStream) -> Result<()> {
    let ping = Value::Array(vec![Value::Bulk("PING".into())]);
    stream.write_all(&ping.encode()).await?;
    Ok(())
}

async fn check_ping_response(stream: &mut TcpStream) -> Result<()> {
    let mut buf = vec![0; 1024];
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
    let mut buf = vec![0; 1024];
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
