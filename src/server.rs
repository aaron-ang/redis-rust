use crate::config::Config;
use crate::db::Store;
use crate::util::{Command, ReplicaType};

use anyhow::Result;
use base64::prelude::*;
use resp::{Decoder, Value};
use std::{io::BufReader, str::FromStr, time::SystemTime};
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
                        Err(e) => match e {
                            RecvError::Closed => {
                                eprintln!("Channel closed");
                                break;
                            }
                            RecvError::Lagged(n) => {
                                eprintln!("Lagged by {n} messages");
                            }
                        },
                    }
                    let mut buf = vec![0; 1024];
                    if let Ok(bytes_read) =
                        timeout(Duration::from_millis(100), self.stream.read(&mut buf)).await
                    {
                        let mut decoder = Decoder::new(BufReader::new(&buf[..bytes_read?]));
                        while let Some(_) = decoder.decode().ok() {
                            self.config.rep_state.incr_num_ack().await;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn execute(&mut self, cmd_line: &Value) -> Result<Option<Value>> {
        let (command, args) = extract_command(&cmd_line)?;
        let response = match command {
            Command::PING => Some(Value::String("PONG".into())),
            Command::ECHO => {
                if args.is_empty() {
                    None
                } else {
                    Some(args[0].clone())
                }
            }
            Command::SET => {
                let resp = handle_set(args, &self.config.store)?;
                if self.config.role == ReplicaType::Leader {
                    let _ = self.config.tx.send(cmd_line.clone());
                }
                Some(resp)
            }
            Command::GET => handle_get(args, &self.config.store)?,
            Command::INFO => handle_info(&self.config.role),
            Command::REPLCONF => Some(Value::String("OK".into())),
            Command::PSYNC => {
                self.handle_psync().await?;
                None
            }
            Command::WAIT => self.handle_wait(&args).await?,
            Command::CONFIG => self.handle_config(args)?,
            Command::KEYS => self.handle_keys(args)?,
            Command::TYPE => self.handle_type(args)?,
        };

        self.config
            .rep_state
            .set_prev_client_cmd(Some(command))
            .await;

        Ok(response)
    }

    async fn handle_psync(&mut self) -> Result<()> {
        let msg = Value::String(format!("FULLRESYNC {REPL_ID} {REPL_OFFSET}"));
        self.stream.write_all(&msg.encode()).await?;

        let rdb = BASE64_STANDARD.decode(EMPTY_RDB_B64)?;
        self.stream
            .write(format!("${}\r\n", rdb.len()).as_bytes())
            .await?;
        self.stream.write_all(&rdb).await?;
        self.stream.flush().await?;
        self.replication = true;
        Ok(())
    }

    async fn handle_wait(&mut self, args: &Vec<Value>) -> Result<Option<Value>> {
        if args.len() < 2 {
            return Ok(None);
        }
        let limit = unpack_bulk_string(&args[0])?.parse::<usize>()?;
        let timeout = unpack_bulk_string(&args[1])?.parse::<u64>()?;

        if limit <= 0 {
            return Ok(Some(Value::Integer(self.num_followers() as i64)));
        }

        let responded = if self.config.rep_state.get_prev_client_cmd().await != Some(Command::SET) {
            // If last command wasn't SET, just sleep for timeout duration
            sleep(Duration::from_millis(timeout)).await;
            self.num_followers()
        } else {
            // Get acknowledgements from followers
            let repl_getack = Value::Array(vec![
                Value::Bulk("REPLCONF".into()),
                Value::Bulk("GETACK".into()),
                Value::Bulk("*".into()),
            ]);
            self.config.tx.send(repl_getack)?;

            // Wait until either timeout or we get enough acknowledgements
            let end = Instant::now() + Duration::from_millis(timeout);
            let num_ack = loop {
                let curr_acks = self.config.rep_state.get_num_ack().await;
                if Instant::now() >= end || curr_acks >= limit {
                    break curr_acks;
                }
                sleep(Duration::from_millis(100)).await;
            };

            // Reset state
            self.config.rep_state.reset().await;
            num_ack
        };

        Ok(Some(Value::Integer(responded as i64)))
    }

    fn handle_config(&self, args: Vec<Value>) -> Result<Option<Value>> {
        if args.len() < 2 {
            return Ok(None);
        }

        let cmd = unpack_bulk_string(&args[0])?;
        let cmd = Command::from_str(&cmd)?;
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
                    _ => anyhow::bail!("Invalid CONFIG option"),
                };
                Some(Value::Array(vec![Value::Bulk(name), value]))
            }
            _ => None,
        };
        Ok(res)
    }

    fn handle_keys(&self, args: Vec<Value>) -> Result<Option<Value>> {
        if args.len() != 1 {
            anyhow::bail!("Wrong number of arguments for KEYS command");
        }

        let pattern = unpack_bulk_string(&args[0])?;
        let keys = self.config.store.keys(&pattern)?;
        let res = Some(Value::Array(keys.into_iter().map(Value::Bulk).collect()));
        Ok(res)
    }

    fn handle_type(&self, args: Vec<Value>) -> Result<Option<Value>> {
        if args.len() != 1 {
            anyhow::bail!("Wrong number of arguments for TYPE command");
        }

        let key = unpack_bulk_string(&args[0])?;
        let value = self.config.store.get(&key);
        let res = match value {
            Some(_) => Some(Value::Bulk("string".into())),
            None => Some(Value::Bulk("none".into())),
        };
        Ok(res)
    }

    fn num_followers(&self) -> usize {
        self.config.tx.receiver_count()
    }
}

pub fn extract_command(value: &Value) -> Result<(Command, Vec<Value>)> {
    if let Value::Array(a) = value {
        let command_str = unpack_bulk_string(&a[0])?;
        let command = Command::from_str(&command_str)?;
        let args = a[1..].to_vec();
        Ok((command, args))
    } else {
        anyhow::bail!("Expected array value")
    }
}

fn unpack_bulk_string(value: &Value) -> Result<String> {
    if let Value::Bulk(s) = value {
        Ok(s.clone())
    } else {
        anyhow::bail!("Expected bulk string")
    }
}

pub fn handle_set(args: Vec<Value>, store: &Store) -> Result<Value> {
    if args.len() < 2 {
        anyhow::bail!("SET command requires at least 2 arguments");
    }

    let key = unpack_bulk_string(&args[0])?;
    let value = unpack_bulk_string(&args[1])?;
    let mut expiry: Option<SystemTime> = None;

    if let Some(Value::Bulk(option)) = args.get(2) {
        match option.to_lowercase().as_str() {
            "px" => {
                let ms = match args.get(3) {
                    Some(Value::Bulk(arg)) => arg.parse::<u64>().unwrap_or(0),
                    _ => anyhow::bail!("Invalid PX option"),
                };
                expiry = Some(SystemTime::now() + Duration::from_millis(ms));
            }
            _ => anyhow::bail!("Invalid SET option"),
        }
    }

    store.set(key, value, expiry);
    Ok(Value::String("OK".into()))
}

pub fn handle_get(args: Vec<Value>, store: &Store) -> Result<Option<Value>> {
    if args.is_empty() {
        anyhow::bail!("GET command requires at least 1 argument");
    }
    let key = unpack_bulk_string(&args[0])?;
    Ok(store
        .get(&key)
        .map(|v| Value::Bulk(v))
        .or(Some(Value::Null)))
}

fn handle_info(role: &ReplicaType) -> Option<Value> {
    let value =
        format!("role:{role}\r\nmaster_replid:{REPL_ID}\r\nmaster_repl_offset:{REPL_OFFSET}");
    Some(Value::Bulk(value))
}
