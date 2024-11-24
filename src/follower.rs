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
    leader_stream: TcpStream,
    offset: usize,
}

impl Follower {
    pub fn new(port: u16, store: Store, leader_stream: TcpStream) -> Self {
        Self {
            port,
            store,
            leader_stream,
            offset: 0,
        }
    }

    pub async fn handle_conn(&mut self) -> Result<()> {
        self.connect_to_leader().await?;

        let mut buf = vec![0; 1024];
        loop {
            let bytes_read = self.leader_stream.read(&mut buf).await?;
            if bytes_read == 0 {
                eprintln!("Leader closed connection");
                break;
            }

            let mut decoder = Decoder::new(BufReader::new(&buf[..bytes_read]));
            loop {
                match decoder.decode() {
                    Ok(value) => {
                        let (command, args) = server::extract_command(&value)?;
                        match command {
                            Command::PING => {}
                            Command::SET => {
                                server::handle_set(args, &self.store)?;
                            }
                            Command::GET => {
                                server::handle_get(args, &self.store)?;
                            }
                            Command::REPLCONF => self.handle_replconf().await?,
                            _ => eprintln!("Unknown command: {}", command),
                        }
                        self.offset += value.encode().len();
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

    async fn connect_to_leader(&mut self) -> Result<()> {
        self.send_ping().await?;
        self.send_replconf().await?;
        self.send_psync().await?;
        Ok(())
    }

    async fn send_ping(&mut self) -> Result<()> {
        let ping = Value::Array(vec![Value::Bulk(Command::PING.to_string())]);
        self.leader_stream.write_all(&ping.encode()).await?;

        let mut buf = vec![0; 1024];
        let bytes_read = self.leader_stream.read(&mut buf).await?;
        let response = Decoder::new(BufReader::new(&buf[..bytes_read])).decode()?;
        match response {
            Value::String(res_str) if res_str == "PONG" => Ok(()),
            _ => anyhow::bail!("Unexpected response to PING"),
        }
    }

    async fn send_replconf(&mut self) -> Result<()> {
        let replconf_port = Value::Array(vec![
            Value::Bulk(Command::REPLCONF.to_string()),
            Value::Bulk("listening-port".into()),
            Value::Bulk(self.port.to_string()),
        ]);
        self.leader_stream
            .write_all(&replconf_port.encode())
            .await?;

        let replconf_capa = Value::Array(vec![
            Value::Bulk(Command::REPLCONF.to_string()),
            Value::Bulk("capa".into()),
            Value::Bulk("eof".into()),
        ]);
        self.leader_stream
            .write_all(&replconf_capa.encode())
            .await?;

        let mut buf = vec![0; 1024];
        let mut num_ok = 0;

        // check that 2 OK's are received
        while num_ok < 2 {
            let bytes_read = self.leader_stream.read(&mut buf).await?;
            let mut decoder = Decoder::new(BufReader::new(&buf[..bytes_read]));
            while let Some(response) = decoder.decode().ok() {
                match response {
                    Value::String(res_str) if res_str == "OK" => num_ok += 1,
                    _ => anyhow::bail!("Unexpected response to REPLCONF"),
                }
            }
        }
        Ok(())
    }

    async fn send_psync(&mut self) -> Result<()> {
        let psync = Value::Array(vec![
            Value::Bulk(Command::PSYNC.to_string()),
            Value::Bulk("?".into()),
            Value::Bulk("-1".to_string()),
        ]);
        self.leader_stream.write_all(&psync.encode()).await?;

        let mut psync_buf = vec![0; 56]; // length of PSYNC response
        self.leader_stream.read_exact(&mut psync_buf).await?;

        let mut decoder = Decoder::new(BufReader::new(&psync_buf[..]));
        if let Value::String(res_str) = decoder.decode().unwrap() {
            let sync = res_str.split_ascii_whitespace().collect::<Vec<&str>>();
            if sync.len() < 3 || sync[0] != "FULLRESYNC" {
                anyhow::bail!("Unexpected response to PSYNC: {}", res_str);
            }
            self.offset = sync.last().unwrap().parse::<usize>()?;
        } else {
            anyhow::bail!("Unexpected response to PSYNC");
        }

        // verify RDB file
        let mut rdb = BASE64_STANDARD.decode(EMPTY_RDB_B64).unwrap();
        let mut msg_bytes = format!("${}\r\n", rdb.len()).into_bytes();
        msg_bytes.append(&mut rdb);

        let mut rdb_buf = vec![0; msg_bytes.len()];
        self.leader_stream.read_exact(&mut rdb_buf).await?;

        if rdb_buf != msg_bytes.as_slice() {
            eprintln!("Received RDB: {:?}", String::from_utf8_lossy(&rdb_buf));
            eprintln!(
                "Expected RDB: {:?}",
                String::from_utf8_lossy(msg_bytes.as_slice())
            );
            anyhow::bail!("Unexpected RDB file");
        }
        Ok(())
    }

    async fn handle_replconf(&mut self) -> Result<()> {
        let replconf_ack = Value::Array(vec![
            Value::Bulk(Command::REPLCONF.to_string()),
            Value::Bulk("ACK".into()),
            Value::Bulk(self.offset.to_string()),
        ]);
        self.leader_stream.write_all(&replconf_ack.encode()).await?;
        Ok(())
    }
}
