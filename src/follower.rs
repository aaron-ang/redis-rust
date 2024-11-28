use anyhow::{bail, Result};
use base64::prelude::*;
use resp::{Decoder, Value};
use std::io::{BufReader, ErrorKind};
use std::sync::Arc;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use crate::db::Store;
use crate::server::{self, EMPTY_RDB_B64};
use crate::util::Command;

const BUFFER_SIZE: usize = 1024;
const PSYNC_RESPONSE_LEN: usize = 56;

pub struct Follower {
    port: u16,
    store: Arc<Store>,
    leader_stream: TcpStream,
    offset: usize,
}

impl Follower {
    pub fn new(port: u16, store: Arc<Store>, leader_stream: TcpStream) -> Self {
        Self {
            port,
            store,
            leader_stream,
            offset: 0,
        }
    }

    pub async fn handle_conn(&mut self) -> Result<()> {
        self.connect_to_leader().await?;

        let mut buf = vec![0; BUFFER_SIZE];
        loop {
            let bytes_read = self.leader_stream.read(&mut buf).await?;
            if bytes_read == 0 {
                eprintln!("Leader closed connection");
                break;
            }

            self.process_commands(&buf[..bytes_read]).await?;
        }
        Ok(())
    }

    async fn process_commands(&mut self, data: &[u8]) -> Result<()> {
        let mut decoder = Decoder::new(BufReader::new(data));

        loop {
            match decoder.decode() {
                Ok(value) => {
                    let (command, args) = server::extract_command(&value)?;
                    self.handle_command(command, args).await?;
                    self.offset += value.encode().len();
                }
                Err(e) => match e.kind() {
                    ErrorKind::UnexpectedEof => break,
                    ErrorKind::InvalidInput => continue,
                    _ => eprintln!("Error: {:?}", e),
                },
            }
        }
        Ok(())
    }

    async fn handle_command(&mut self, command: Command, args: &[Value]) -> Result<()> {
        match command {
            Command::PING => {}
            Command::SET => {
                server::handle_set(args, &self.store).await?;
            }
            Command::GET => {
                server::handle_get(args, &self.store).await?;
            }
            Command::REPLCONF => self.handle_replconf().await?,
            Command::XADD => {
                server::handle_xadd(args, &self.store).await?;
            }
            Command::INCR => {
                server::handle_incr(args, &self.store).await?;
            }
            _ => eprintln!("Unknown command: {}", command),
        }
        Ok(())
    }

    async fn connect_to_leader(&mut self) -> Result<()> {
        self.send_ping().await?;
        self.send_replconf().await?;
        self.send_psync().await?;
        Ok(())
    }

    async fn send_command(&mut self, value: Value) -> Result<()> {
        self.leader_stream.write_all(&value.encode()).await?;
        Ok(())
    }

    async fn read_response(&mut self) -> Result<Value> {
        self.read_response_with_len(0).await
    }

    async fn read_response_with_len(&mut self, len: usize) -> Result<Value> {
        let buf_size = if len > 0 { len } else { BUFFER_SIZE };
        let mut buf = vec![0; buf_size];
        let bytes_read = self.leader_stream.read(&mut buf).await?;
        Ok(Decoder::new(BufReader::new(&buf[..bytes_read])).decode()?)
    }

    async fn send_ping(&mut self) -> Result<()> {
        let ping = Value::Array(vec![Value::Bulk(Command::PING.to_string())]);
        self.send_command(ping).await?;

        match self.read_response().await? {
            Value::String(res) if res == "PONG" => Ok(()),
            _ => bail!("Unexpected response to PING"),
        }
    }

    async fn send_replconf(&mut self) -> Result<()> {
        // Send port configuration
        let port_cmd = Value::Array(vec![
            Value::Bulk(Command::REPLCONF.to_string()),
            Value::Bulk("listening-port".into()),
            Value::Bulk(self.port.to_string()),
        ]);
        self.send_command(port_cmd).await?;

        // Send capabilities
        let capa_cmd = Value::Array(vec![
            Value::Bulk(Command::REPLCONF.to_string()),
            Value::Bulk("capa".into()),
            Value::Bulk("eof".into()),
        ]);
        self.send_command(capa_cmd).await?;

        // Verify responses
        for _ in 0..2 {
            match self.read_response().await? {
                Value::String(res) if res == "OK" => continue,
                _ => bail!("Unexpected response to REPLCONF"),
            }
        }
        Ok(())
    }

    async fn send_psync(&mut self) -> Result<()> {
        self.request_sync().await?;
        self.verify_rdb_file().await?;
        Ok(())
    }

    async fn request_sync(&mut self) -> Result<()> {
        let psync = Value::Array(vec![
            Value::Bulk(Command::PSYNC.to_string()),
            Value::Bulk("?".into()),
            Value::Bulk("-1".to_string()),
        ]);
        self.send_command(psync).await?;

        match self.read_response_with_len(PSYNC_RESPONSE_LEN).await? {
            Value::String(res_str) => {
                let sync: Vec<&str> = res_str.split_ascii_whitespace().collect();
                if sync.len() < 3 || sync[0] != "FULLRESYNC" {
                    bail!("Unexpected response to PSYNC: {}", res_str);
                }
                self.offset = sync[2].parse()?;
                Ok(())
            }
            _ => bail!("Invalid PSYNC response format"),
        }
    }

    async fn verify_rdb_file(&mut self) -> Result<()> {
        let rdb = BASE64_STANDARD.decode(EMPTY_RDB_B64)?;
        let mut msg_bytes = format!("${}\r\n", rdb.len()).into_bytes();
        msg_bytes.append(&mut rdb.to_vec());

        let mut rdb_buf = vec![0; msg_bytes.len()];
        self.leader_stream.read_exact(&mut rdb_buf).await?;

        if rdb_buf != msg_bytes {
            bail!("RDB file verification failed");
        }
        Ok(())
    }

    async fn handle_replconf(&mut self) -> Result<()> {
        let replconf_ack = Value::Array(vec![
            Value::Bulk(Command::REPLCONF.to_string()),
            Value::Bulk("ACK".into()),
            Value::Bulk(self.offset.to_string()),
        ]);
        self.send_command(replconf_ack).await
    }
}
