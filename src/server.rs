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

use crate::config::Config;
use crate::db::{RecordType, Store};
use crate::stream::StreamValue;
use crate::util::{Command, RedisError, ReplicaType, XReadBlockType};

pub const REPL_ID: &str = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
pub const REPL_OFFSET: usize = 0;
pub const EMPTY_RDB_B64: &str = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";

pub struct Server {
    config: Config,
    replication: bool,
    stream: TcpStream,
    queued_commands: Option<Vec<Value>>,
}

impl Server {
    pub fn new(config: Config, stream: TcpStream) -> Self {
        Server {
            config,
            replication: false,
            stream,
            queued_commands: None,
        }
    }

    pub async fn handle_conn(&mut self) -> Result<()> {
        let mut buf = vec![0; 1024];

        while let Ok(bytes_read) = self.stream.read(&mut buf).await {
            if bytes_read == 0 {
                eprintln!("Client disconnected");
                break;
            }

            let mut decoder = Decoder::new(BufReader::new(&buf[..bytes_read]));
            while let Ok(cmd_line) = decoder.decode() {
                match self.process(&cmd_line).await {
                    Ok(Some(response)) => {
                        self.stream.write_all(&response.encode()).await?;
                    }
                    Ok(None) => (),
                    Err(e) => {
                        eprintln!("Error processing command: {e}");
                        self.stream
                            .write_all(&Value::Error(e.to_string()).encode())
                            .await?;
                    }
                }
            }

            if self.replication {
                self.handle_replication(&mut buf).await?;
            }
        }
        Ok(())
    }

    async fn handle_replication(&mut self, buf: &mut [u8]) -> Result<()> {
        let mut rx = self.config.tx.subscribe();
        loop {
            match rx.recv().await {
                Ok(v) => self.stream.write_all(&v.encode()).await?,
                Err(RecvError::Closed) => {
                    eprintln!("Channel closed");
                    break;
                }
                Err(RecvError::Lagged(n)) => {
                    eprintln!("Lagged by {n} messages");
                    continue;
                }
            }

            // Collect acks
            if let Ok(bytes_read) = timeout(Duration::from_millis(100), self.stream.read(buf)).await
            {
                let mut decoder = Decoder::new(BufReader::new(&buf[..bytes_read?]));
                while decoder.decode().ok().is_some() {
                    self.config.rep_state.incr_num_ack().await;
                }
            }
        }
        Ok(())
    }

    pub async fn process(&mut self, cmd_line: &Value) -> Result<Option<Value>> {
        let (command, args) = extract_command(cmd_line)?;

        if let Some(queued_commands) = &mut self.queued_commands {
            let response = match command {
                Command::Exec => {
                    let commands = self.queued_commands.take().unwrap();
                    let mut responses = Vec::with_capacity(commands.len());
                    for cmd in commands {
                        match Box::pin(self.process(&cmd)).await {
                            Ok(Some(result)) => responses.push(result),
                            Ok(None) => (),
                            Err(e) => {
                                eprintln!("Error processing command: {e}");
                                responses.push(Value::Error(e.to_string()));
                            }
                        }
                    }
                    Some(Value::Array(responses))
                }
                Command::Discard => {
                    self.queued_commands = None;
                    Some(Value::String("OK".into()))
                }
                _ => {
                    queued_commands.push(cmd_line.clone());
                    Some(Value::String("QUEUED".into()))
                }
            };
            return Ok(response);
        }

        let response = match command {
            Command::BLPop => Some(handle_blpop(args, &self.config.store).await?),
            Command::Config => Some(self.handle_config(args)?),
            Command::Discard => bail!(RedisError::CommandWithoutMulti(command)),
            Command::Echo => Some(handle_echo(args)?),
            Command::Exec => bail!(RedisError::CommandWithoutMulti(command)),
            Command::Get => Some(handle_get(args, &self.config.store).await?),
            Command::Incr => Some(handle_incr(args, &self.config.store).await?),
            Command::Info => Some(self.handle_info()),
            Command::Keys => Some(self.handle_keys(args).await?),
            Command::LLen => Some(self.handle_llen(args).await?),
            Command::LPop => Some(handle_lpop(args, &self.config.store).await?),
            Command::LPush => Some(handle_lpush(args, &self.config.store).await?),
            Command::LRange => Some(self.handle_lrange(args).await?),
            Command::Multi => {
                self.queued_commands = Some(Vec::new());
                Some(Value::String("OK".into()))
            }
            Command::Ping => Some(Value::String("PONG".into())),
            Command::PSync => {
                if let Err(e) = self.handle_psync().await {
                    eprintln!("Error handling PSYNC: {e:?}");
                }
                None
            }
            Command::ReplConf => Some(Value::String("OK".into())),
            Command::RPush => Some(handle_rpush(args, &self.config.store).await?),
            Command::Set => Some(handle_set(args, &self.config.store).await?),
            Command::Type => Some(self.handle_type(args).await?),
            Command::Wait => Some(self.handle_wait(args).await?),
            Command::XAdd => Some(handle_xadd(args, &self.config.store).await?),
            Command::XRange => Some(self.handle_xrange(args).await?),
            Command::XRead => Some(self.handle_xread(args).await?),
        };

        if command.is_write() && self.config.role == ReplicaType::Leader {
            let _ = self.config.tx.send(cmd_line.clone());
        }

        self.config
            .rep_state
            .set_prev_client_cmd(Some(command))
            .await;

        Ok(response)
    }

    // CONFIG GET parameter
    fn handle_config(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            bail!(RedisError::InvalidArgument);
        }
        let cmd = unpack_bulk_string(&args[0])?;
        let cmd = Command::from_str(cmd)?;
        let res = match cmd {
            Command::Get => {
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
                    option => bail!("Unsupported CONFIG option: {}", option),
                };
                Value::Array(vec![Value::Bulk(name.to_string()), value])
            }
            cmd => bail!("Unsupported CONFIG subcommand: {}", cmd),
        };
        Ok(res)
    }

    // INFO
    fn handle_info(&self) -> Value {
        let value = format!(
            "role:{}\r\nmaster_replid:{REPL_ID}\r\nmaster_repl_offset:{REPL_OFFSET}",
            self.config.role
        );
        Value::Bulk(value)
    }

    // KEYS pattern
    async fn handle_keys(&self, args: &[Value]) -> Result<Value> {
        if args.len() != 1 {
            bail!(RedisError::InvalidArgument);
        }
        let pattern = unpack_bulk_string(&args[0])?;
        let keys = self.config.store.keys(pattern).await?;
        Ok(Value::Array(keys.into_iter().map(Value::Bulk).collect()))
    }

    // LLEN key
    async fn handle_llen(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            bail!(RedisError::InvalidArgument);
        }
        let key = unpack_bulk_string(&args[0])?;
        Ok(Value::Integer(self.config.store.llen(key).await?))
    }

    // LRANGE key start stop
    async fn handle_lrange(&self, args: &[Value]) -> Result<Value> {
        if args.len() != 3 {
            bail!(RedisError::InvalidArgument);
        }
        let key = unpack_bulk_string(&args[0])?;
        let start = unpack_bulk_string(&args[1])?.parse::<i64>()?;
        let end = unpack_bulk_string(&args[2])?.parse::<i64>()?;

        let values = self.config.store.lrange(key, start, end).await?;
        Ok(Value::Array(values.into_iter().map(Value::Bulk).collect()))
    }

    // PSYNC replicationid offset
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

    // TYPE key
    async fn handle_type(&self, args: &[Value]) -> Result<Value> {
        if args.len() != 1 {
            bail!(RedisError::InvalidArgument);
        }
        let key = unpack_bulk_string(&args[0])?;
        let value = self.config.store.get(key).await;
        let res = match value {
            Some(RecordType::String(_)) => Value::Bulk("string".into()),
            Some(RecordType::Stream(_)) => Value::Bulk("stream".into()),
            Some(RecordType::List(_)) => Value::Bulk("list".into()),
            None => Value::Bulk("none".into()),
        };
        Ok(res)
    }

    // WAIT numreplicas timeout
    async fn handle_wait(&mut self, args: &[Value]) -> Result<Value> {
        if args.len() != 2 {
            bail!(RedisError::InvalidArgument);
        }
        let num_replicas = unpack_bulk_string(&args[0])?.parse::<usize>()?;
        let timeout_ms = unpack_bulk_string(&args[1])?.parse::<u64>()?;

        if num_replicas == 0 {
            return Ok(Value::Integer(self.count_active_replicas() as i64));
        }

        let responded = if self.config.rep_state.get_prev_client_cmd().await != Some(Command::Set) {
            sleep(Duration::from_millis(timeout_ms)).await;
            self.count_active_replicas()
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
                if Instant::now() >= end_time || curr_acks >= num_replicas {
                    break curr_acks;
                }
                sleep(Duration::from_millis(50)).await;
            };

            self.config.rep_state.reset().await;
            num_ack
        };

        Ok(Value::Integer(responded as i64))
    }

    fn count_active_replicas(&self) -> usize {
        self.config.tx.receiver_count()
    }

    // XRANGE key start end
    async fn handle_xrange(&self, args: &[Value]) -> Result<Value> {
        if args.len() != 3 {
            bail!(RedisError::InvalidArgument);
        }

        let key = unpack_bulk_string(&args[0])?;
        let start = unpack_bulk_string(&args[1])?;
        let end = unpack_bulk_string(&args[2])?;

        let stream_entries = self
            .config
            .store
            .get_range_stream_entries(key, start, end)
            .await?;

        if stream_entries.is_empty() {
            Ok(Value::Null)
        } else {
            let values = build_stream_entry_list(stream_entries);
            Ok(Value::Array(values))
        }
    }

    // XREAD [BLOCK milliseconds] STREAMS key [key ...] id [id ...]
    async fn handle_xread(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 3 {
            bail!(RedisError::InvalidArgument);
        }

        let mut args_iter = args.iter();
        let mut block_option = XReadBlockType::NoWait;

        loop {
            match args_iter.next() {
                Some(Value::Bulk(b)) if b.to_lowercase() == "block" => {
                    if let Some(Value::Bulk(ms)) = args_iter.next() {
                        block_option = ms.parse()?;
                    } else {
                        bail!(RedisError::InvalidArgument);
                    }
                }
                Some(Value::Bulk(b)) if b.to_lowercase() == "streams" => break,
                _ => bail!(RedisError::InvalidArgument),
            }
        }

        let stream_args = args_iter.collect::<Vec<_>>();
        if stream_args.len() % 2 != 0 {
            bail!(RedisError::InvalidArgument);
        }

        let mid = stream_args.len() / 2;
        let stream_keys = &stream_args[..mid];
        let start_ids = &stream_args[mid..];

        let streams: Vec<(&str, &str)> = stream_keys
            .iter()
            .zip(start_ids)
            .map(|(key, id)| Ok((unpack_bulk_string(key)?, unpack_bulk_string(id)?)))
            .collect::<Result<_>>()?;

        let entries = self
            .config
            .store
            .get_bulk_stream_entries(&streams, block_option)
            .await?;

        if entries.is_empty() {
            Ok(Value::Null)
        } else {
            // Follow same stream order as input
            let values = stream_keys
                .iter()
                .filter_map(|key| {
                    unpack_bulk_string(key).ok().and_then(|key_str| {
                        entries.get(key_str).map(|stream| {
                            Value::Array(vec![
                                Value::Bulk(key_str.to_string()),
                                Value::Array(build_stream_entry_list(stream.clone())),
                            ])
                        })
                    })
                })
                .collect();
            Ok(Value::Array(values))
        }
    }
}

fn build_stream_entry_list(entries: StreamValue) -> Vec<Value> {
    entries
        .iter()
        .map(|(id, fields)| {
            Value::Array(vec![
                Value::Bulk(id.to_string()),
                Value::Array(
                    fields
                        .iter()
                        .flat_map(|(field, value)| {
                            vec![
                                Value::Bulk(field.to_string()),
                                Value::Bulk(value.to_string()),
                            ]
                        })
                        .collect(),
                ),
            ])
        })
        .collect()
}

pub fn extract_command(value: &Value) -> Result<(Command, &[Value])> {
    if let Value::Array(args) = value {
        let command_str = unpack_bulk_string(&args[0])?;
        let command = Command::from_str(command_str)?;
        Ok((command, &args[1..]))
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

// ECHO message
fn handle_echo(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        bail!(RedisError::InvalidArgument);
    } else {
        Ok(args[0].clone())
    }
}

// SET key value [PX milliseconds]
pub async fn handle_set(args: &[Value], store: &Store) -> Result<Value> {
    if args.len() < 2 {
        bail!(RedisError::InvalidArgument);
    }

    let key = unpack_bulk_string(&args[0])?;
    let value = unpack_bulk_string(&args[1])?;
    let expiry = if let Some(Value::Bulk(option)) = args.get(2) {
        match option.to_lowercase().as_str() {
            "px" => {
                let ms = match args.get(3) {
                    Some(Value::Bulk(arg)) => match arg.parse::<u64>() {
                        Ok(ms) => ms,
                        Err(_) => bail!(RedisError::InvalidInteger),
                    },
                    _ => bail!(RedisError::InvalidArgument),
                };
                Some(SystemTime::now() + Duration::from_millis(ms))
            }
            option => bail!("Unsupported SET option: {}", option),
        }
    } else {
        None
    };

    store.set(key.to_string(), value.into(), expiry).await;
    Ok(Value::String("OK".into()))
}

// GET key
pub async fn handle_get(args: &[Value], store: &Store) -> Result<Value> {
    if args.is_empty() {
        bail!(RedisError::InvalidArgument);
    }
    let key = unpack_bulk_string(&args[0])?;
    match store.get(key).await {
        Some(RecordType::String(s)) => Ok(Value::Bulk(s.to_string())),
        None => Ok(Value::Null),
        _ => unimplemented!(),
    }
}

// XADD key <* | id> field value [field value ...]
pub async fn handle_xadd(args: &[Value], store: &Store) -> Result<Value> {
    if args.len() < 2 {
        bail!(RedisError::InvalidArgument);
    }

    let key = unpack_bulk_string(&args[0])?;
    let entry_id = unpack_bulk_string(&args[1])?;
    let values = args[2..]
        .chunks(2)
        .map(|pair| match pair {
            [field, value] => Ok((
                unpack_bulk_string(field)?.to_string(),
                unpack_bulk_string(value)?.to_string(),
            )),
            _ => bail!(RedisError::InvalidArgument),
        })
        .collect::<Result<HashMap<_, _>>>()?;

    if values.is_empty() {
        bail!(RedisError::InvalidArgument);
    }

    match store.add_stream_entry(key, entry_id, values).await {
        Ok(stream_entry_id) => Ok(Value::Bulk(stream_entry_id.to_string())),
        Err(e) => Ok(Value::Error(e.to_string())),
    }
}

// INC key
pub async fn handle_incr(args: &[Value], store: &Store) -> Result<Value> {
    if args.is_empty() {
        bail!(RedisError::InvalidArgument);
    }
    let key = unpack_bulk_string(&args[0])?;
    let value = store.incr(key).await?;
    Ok(Value::Integer(value))
}

// RPUSH key element [element ...]
pub async fn handle_rpush(args: &[Value], store: &Store) -> Result<Value> {
    if args.len() < 2 {
        bail!(RedisError::InvalidArgument);
    }
    let key = unpack_bulk_string(&args[0])?;
    let elements = args[1..]
        .iter()
        .map(|v| unpack_bulk_string(v))
        .collect::<Result<Vec<_>>>()?;
    let count = store.rpush(key, &elements).await?;
    Ok(Value::Integer(count))
}

// LPOP key [count]
pub async fn handle_lpop(args: &[Value], store: &Store) -> Result<Value> {
    if args.is_empty() {
        bail!(RedisError::InvalidArgument);
    }
    let key = unpack_bulk_string(&args[0])?;
    let count = match args.get(1) {
        Some(Value::Bulk(c)) => {
            let parsed = c.parse::<i64>()?;
            if parsed < 0 {
                bail!(RedisError::InvalidInteger);
            }
            Some(parsed as usize)
        }
        Some(Value::Integer(c)) => {
            if *c < 0 {
                bail!(RedisError::InvalidInteger);
            }
            Some(*c as usize)
        }
        None => None,
        _ => bail!(RedisError::InvalidArgument),
    };
    let elements = store.lpop(key, count.unwrap_or(1)).await?;

    if elements.is_empty() {
        Ok(Value::Null)
    } else if count.is_none() {
        Ok(Value::Bulk(elements[0].clone()))
    } else {
        Ok(Value::Array(
            elements.into_iter().map(Value::Bulk).collect(),
        ))
    }
}

// LPUSH key element [element ...]
pub async fn handle_lpush(args: &[Value], store: &Store) -> Result<Value> {
    if args.len() < 2 {
        bail!(RedisError::InvalidArgument);
    }
    let key = unpack_bulk_string(&args[0])?;
    let elements = args[1..]
        .iter()
        .map(|v| unpack_bulk_string(v))
        .collect::<Result<Vec<_>>>()?;
    let count = store.lpush(key, &elements).await?;
    Ok(Value::Integer(count))
}

// BLPOP key [key ...] timeout
pub async fn handle_blpop(args: &[Value], store: &Store) -> Result<Value> {
    if args.len() < 2 {
        bail!(RedisError::InvalidArgument);
    }

    let timeout = match args.last() {
        Some(Value::Bulk(t)) => t.parse::<f64>()?,
        _ => bail!(RedisError::InvalidArgument),
    };
    let keys = args[..args.len() - 1]
        .iter()
        .map(|v| unpack_bulk_string(v))
        .collect::<Result<Vec<_>>>()?;
    let elements = store.blpop(&keys, timeout).await?;

    if let Some((key, value)) = elements {
        Ok(Value::Array(vec![Value::Bulk(key), Value::Bulk(value)]))
    } else {
        Ok(Value::Null)
    }
}
