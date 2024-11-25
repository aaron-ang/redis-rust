use crate::config::Config;
use crate::db::{RecordType, Store};
use crate::util::{Command, ReplicaType};

use anyhow::{bail, Result};
use base64::prelude::*;
use resp::{Decoder, Value};
use std::{collections::HashMap, io::BufReader, str::FromStr, time::SystemTime};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::broadcast::error::RecvError,
    time::{sleep, timeout, Duration, Instant},
};

pub const REPL_ID: &str = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
pub const REPL_OFFSET: usize = 0;
pub const EMPTY_RDB_B64: &str = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";

pub struct Server {
    config: Config,
    replication: bool,
    stream: TcpStream,
}

impl Server {
    pub fn new(config: Config, stream: TcpStream) -> Self {
        Server {
            config,
            replication: false,
            stream,
        }
    }

    pub async fn handle_conn(&mut self) -> Result<()> {
        let mut buf = vec![0; 1024];

        loop {
            let bytes_read = self.stream.read(&mut buf).await?;
            if bytes_read == 0 {
                eprintln!("Client disconnected");
                break;
            }
            let mut decoder = Decoder::new(BufReader::new(&buf[..bytes_read]));
            while let Some(cmd_line) = decoder.decode().ok() {
                if let Some(response) = self.execute(&cmd_line).await? {
                    self.stream.write_all(&response.encode()).await?;
                }
            }

            if self.replication {
                let mut rx = self.config.tx.subscribe();
                loop {
                    match rx.recv().await {
                        Ok(v) => {
                            self.stream.write_all(&v.encode()).await?;
                        }
                        Err(RecvError::Closed) => {
                            eprintln!("Channel closed");
                            break;
                        }
                        Err(RecvError::Lagged(n)) => {
                            eprintln!("Lagged by {n} messages");
                        }
                    }

                    if let Ok(bytes_read) =
                        timeout(Duration::from_millis(100), self.stream.read(&mut buf)).await
                    {
                        let mut decoder = Decoder::new(BufReader::new(&buf[..bytes_read?]));
                        while decoder.decode().ok().is_some() {
                            self.config.rep_state.incr_num_ack().await;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn execute(&mut self, cmd_line: &Value) -> Result<Option<Value>> {
        let (command, args) = extract_command(cmd_line)?;
        let response = match command {
            Command::PING => Some(Value::String("PONG".into())),
            Command::ECHO => args.first().cloned(),
            Command::SET => {
                let resp = handle_set(args, &self.config.store).await?;
                if self.config.role == ReplicaType::Leader {
                    let _ = self.config.tx.send(cmd_line.clone());
                }
                Some(resp)
            }
            Command::GET => handle_get(args, &self.config.store).await?,
            Command::INFO => Some(self.handle_info()),
            Command::REPLCONF => Some(Value::String("OK".into())),
            Command::PSYNC => {
                self.handle_psync().await?;
                None
            }
            Command::WAIT => self.handle_wait(args).await?,
            Command::CONFIG => self.handle_config(args)?,
            Command::KEYS => Some(self.handle_keys(args).await?),
            Command::TYPE => Some(self.handle_type(args).await?),
            Command::XADD => Some(self.handle_xadd(args).await?),
        };

        self.config
            .rep_state
            .set_prev_client_cmd(Some(command))
            .await;

        Ok(response)
    }

    fn handle_info(&self) -> Value {
        let value = format!(
            "role:{}\r\nmaster_replid:{REPL_ID}\r\nmaster_repl_offset:{REPL_OFFSET}",
            self.config.role
        );
        Value::Bulk(value)
    }

    async fn handle_psync(&mut self) -> Result<()> {
        let msg = Value::String(format!("FULLRESYNC {REPL_ID} {REPL_OFFSET}"));
        self.stream.write_all(&msg.encode()).await?;

        let rdb = BASE64_STANDARD.decode(EMPTY_RDB_B64)?;
        self.stream
            .write_all(format!("${}\r\n", rdb.len()).as_bytes())
            .await?;
        self.stream.write_all(&rdb).await?;
        self.stream.flush().await?;
        self.replication = true;
        Ok(())
    }

    async fn handle_wait(&mut self, args: &[Value]) -> Result<Option<Value>> {
        if args.len() < 2 {
            return Ok(None);
        }
        let limit = unpack_bulk_string(&args[0])?.parse::<usize>()?;
        let timeout_ms = unpack_bulk_string(&args[1])?.parse::<u64>()?;

        if limit == 0 {
            return Ok(Some(Value::Integer(self.num_followers() as i64)));
        }

        let responded = if self.config.rep_state.get_prev_client_cmd().await != Some(Command::SET) {
            sleep(Duration::from_millis(timeout_ms)).await;
            self.num_followers()
        } else {
            let repl_getack = Value::Array(vec![
                Value::Bulk("REPLCONF".into()),
                Value::Bulk("GETACK".into()),
                Value::Bulk("*".into()),
            ]);
            self.config.tx.send(repl_getack)?;

            let end_time = Instant::now() + Duration::from_millis(timeout_ms);
            let num_ack = loop {
                let curr_acks = self.config.rep_state.get_num_ack().await;
                if Instant::now() >= end_time || curr_acks >= limit {
                    break curr_acks;
                }
                sleep(Duration::from_millis(100)).await;
            };

            self.config.rep_state.reset().await;
            num_ack
        };

        Ok(Some(Value::Integer(responded as i64)))
    }

    fn handle_config(&self, args: &[Value]) -> Result<Option<Value>> {
        if args.len() < 2 {
            return Ok(None);
        }

        let cmd = unpack_bulk_string(&args[0])?;
        let cmd = Command::from_str(cmd)?;
        let res = match cmd {
            Command::GET => {
                let name = unpack_bulk_string(&args[1])?;
                let value = match name.to_lowercase().as_str() {
                    "dir" => Value::Bulk(
                        self.config
                            .dir
                            .clone()
                            .into_os_string()
                            .into_string()
                            .unwrap_or_default(),
                    ),
                    "dbfilename" => Value::Bulk(self.config.dbfilename.clone()),
                    _ => bail!("Invalid CONFIG option"),
                };
                Some(Value::Array(vec![Value::Bulk(name.to_string()), value]))
            }
            _ => None,
        };
        Ok(res)
    }

    async fn handle_keys(&self, args: &[Value]) -> Result<Value> {
        if args.len() != 1 {
            bail!("Wrong number of arguments for KEYS command");
        }

        let pattern = unpack_bulk_string(&args[0])?;
        let keys = self.config.store.keys(pattern).await?;
        Ok(Value::Array(keys.into_iter().map(Value::Bulk).collect()))
    }

    async fn handle_type(&self, args: &[Value]) -> Result<Value> {
        if args.len() != 1 {
            bail!("Wrong number of arguments for TYPE command");
        }

        let key = unpack_bulk_string(&args[0])?;
        let value = self.config.store.get(key).await;
        let res = match value {
            Some(RecordType::String(_)) => Value::Bulk("string".into()),
            Some(RecordType::Stream(_)) => Value::Bulk("stream".into()),
            None => Value::Bulk("none".into()),
        };
        Ok(res)
    }

    async fn handle_xadd(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 3 {
            bail!("XADD command requires at least 2 arguments");
        }

        let key = unpack_bulk_string(&args[0])?;
        let entry_id = unpack_bulk_string(&args[1])?;

        let values = args[2..]
            .chunks(2)
            .map(|pair| {
                if let [field, value] = pair {
                    Ok((
                        unpack_bulk_string(field)?.to_string(),
                        unpack_bulk_string(value)?.to_string(),
                    ))
                } else {
                    bail!("Incorrect key-value pairs format.")
                }
            })
            .collect::<Result<HashMap<_, _>>>()?;

        let stream_entry_id = self
            .config
            .store
            .add_stream_entry(key, entry_id, values)
            .await?;

        Ok(Value::Bulk(stream_entry_id.to_string()))
    }

    fn num_followers(&self) -> usize {
        self.config.tx.receiver_count()
    }
}

pub fn extract_command(value: &Value) -> Result<(Command, &[Value])> {
    if let Value::Array(a) = value {
        let command_str = unpack_bulk_string(&a[0])?;
        let command = Command::from_str(command_str)?;
        let args = &a[1..];
        Ok((command, args))
    } else {
        bail!("Expected array value")
    }
}

fn unpack_bulk_string(value: &Value) -> Result<&str> {
    if let Value::Bulk(ref s) = value {
        Ok(s)
    } else {
        bail!("Expected bulk string")
    }
}

pub async fn handle_set(args: &[Value], store: &Store) -> Result<Value> {
    if args.len() < 2 {
        bail!("SET command requires at least 2 arguments");
    }

    let key = unpack_bulk_string(&args[0])?;
    let value = unpack_bulk_string(&args[1])?;
    let mut expiry: Option<SystemTime> = None;

    if let Some(Value::Bulk(option)) = args.get(2) {
        match option.to_lowercase().as_str() {
            "px" => {
                let ms = match args.get(3) {
                    Some(Value::Bulk(arg)) => arg.parse::<u64>().unwrap_or(0),
                    _ => bail!("Invalid PX option"),
                };
                expiry = Some(SystemTime::now() + Duration::from_millis(ms));
            }
            _ => bail!("Invalid SET option"),
        }
    }

    store.set(key.to_string(), value.into(), expiry).await;
    Ok(Value::String("OK".into()))
}

pub async fn handle_get(args: &[Value], store: &Store) -> Result<Option<Value>> {
    if args.is_empty() {
        bail!("GET command requires at least 1 argument");
    }

    let key = unpack_bulk_string(&args[0])?;
    match store.get(key).await {
        Some(RecordType::String(s)) => Ok(Some(Value::Bulk(s.to_string()))),
        Some(RecordType::Stream(_)) => todo!(),
        None => Ok(Some(Value::Null)),
    }
}
