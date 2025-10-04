use std::{collections::HashMap, io::BufReader, str::FromStr, time::SystemTime};

use anyhow::{bail, Result};
use base64::prelude::*;
use resp::{Decoder, Value};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::mpsc,
    task::JoinHandle,
    time::{sleep, timeout, Duration, Instant},
};

use crate::config::Config;
use crate::db::{RecordType, Store};
use crate::replication::ReplicaType;
use crate::stream::StreamValue;
use crate::types::{Command, RedisError, XReadBlockType};

enum ClientMode {
    Normal,
    Transaction {
        queued_commands: Vec<Value>,
    },
    Subscribed {
        sub_tx: mpsc::Sender<(String, String)>,
        forwarders: HashMap<String, JoinHandle<()>>,
    },
}

pub struct Connection {
    stream: TcpStream,
    buffer: Vec<u8>,
}

impl Connection {
    fn new(stream: TcpStream) -> Self {
        Connection {
            stream,
            buffer: vec![0; 1024],
        }
    }

    async fn read_value(&mut self) -> Result<Option<Value>> {
        let bytes_read = self.stream.read(&mut self.buffer).await?;
        if bytes_read == 0 {
            return Ok(None);
        }
        let mut decoder = Decoder::new(BufReader::new(&self.buffer[..bytes_read]));
        let value = decoder.decode()?;
        Ok(Some(value))
    }

    pub async fn read_value_with_timeout(&mut self, duration: Duration) -> Result<Option<Value>> {
        match timeout(duration, self.read_value()).await {
            Err(_) => Ok(None), // Timeout
            Ok(Ok(value)) => Ok(value),
            Ok(Err(e)) => Err(e),
        }
    }

    async fn write_all(&mut self, bytes: &[u8]) -> Result<()> {
        self.stream.write_all(bytes).await?;
        Ok(())
    }

    pub async fn write_value(&mut self, value: &Value) -> Result<()> {
        self.stream.write_all(&value.encode()).await?;
        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        self.stream.flush().await?;
        Ok(())
    }
}

pub const REPL_ID: &str = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
pub const REPL_OFFSET: usize = 0;
pub const EMPTY_RDB_B64: &str = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";

pub struct Server {
    config: Config,
    conn: Connection,
    mode: ClientMode,
}

impl Server {
    pub fn new(config: Config, stream: TcpStream) -> Self {
        Server {
            config,
            conn: Connection::new(stream),
            mode: ClientMode::Normal,
        }
    }

    pub async fn handle_conn(&mut self) -> Result<()> {
        loop {
            let value = match self.conn.read_value().await {
                Ok(Some(value)) => value,
                Ok(None) => break,
                Err(e) => {
                    eprintln!("Error reading from stream: {e}");
                    break;
                }
            };

            match self.process(&value).await {
                Ok(Some(response)) => self.conn.write_value(&response).await?,
                Ok(None) => (),
                Err(e) => {
                    eprintln!("Error processing command: {e}");
                    self.conn.write_value(&Value::Error(e.to_string())).await?;
                }
            }
        }
        Ok(())
    }

    pub async fn process(&mut self, cmd_line: &Value) -> Result<Option<Value>> {
        let (command, args) = extract_command(cmd_line)?;

        if matches!(self.mode, ClientMode::Subscribed { .. })
            && !matches!(
                command,
                Command::Subscribe | Command::Unsubscribe | Command::Ping
            )
        {
            bail!(RedisError::CommandWithoutSubscribe(command));
        }

        // Transaction mode handling
        if matches!(self.mode, ClientMode::Transaction { .. }) {
            let response = match command {
                Command::Exec => {
                    // Take queued commands and exit transaction mode
                    let commands = match std::mem::replace(&mut self.mode, ClientMode::Normal) {
                        ClientMode::Transaction { queued_commands } => queued_commands,
                        other => {
                            // Should not happen, but restore state just in case
                            self.mode = other;
                            Vec::new()
                        }
                    };
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
                    self.mode = ClientMode::Normal;
                    Some(Value::String("OK".into()))
                }
                _ => {
                    if let ClientMode::Transaction { queued_commands } = &mut self.mode {
                        queued_commands.push(cmd_line.clone());
                    }
                    Some(Value::String("QUEUED".into()))
                }
            };
            return Ok(response);
        }

        let response = match command {
            Command::BLPop => Some(handle_blpop(args, &self.config.store).await?),
            Command::Command => Some(Value::Array(vec![])),
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
                self.mode = ClientMode::Transaction {
                    queued_commands: Vec::new(),
                };
                Some(Value::String("OK".into()))
            }
            Command::Ping => Some(self.handle_ping(args)),
            Command::Publish => Some(self.handle_publish(args).await?),
            Command::PSync => {
                if let Err(e) = self.handle_psync().await {
                    eprintln!("Error handling PSYNC: {e:?}");
                }
                None
            }
            Command::ReplConf => Some(Value::String("OK".into())),
            Command::RPush => Some(handle_rpush(args, &self.config.store).await?),
            Command::Set => Some(handle_set(args, &self.config.store).await?),
            Command::Subscribe => {
                self.handle_subscribe(args).await?;
                None
            }
            Command::Type => Some(self.handle_type(args).await?),
            Command::Unsubscribe => {
                if !matches!(self.mode, ClientMode::Subscribed { .. }) {
                    Some(Value::Array(vec![
                        Value::Bulk("unsubscribe".into()),
                        Value::Null,
                        Value::Integer(0),
                    ]))
                } else {
                    self.handle_unsubscribe(args).await?;
                    None
                }
            }
            Command::Wait => Some(self.handle_wait(args).await?),
            Command::XAdd => Some(handle_xadd(args, &self.config.store).await?),
            Command::XRange => Some(self.handle_xrange(args).await?),
            Command::XRead => Some(self.handle_xread(args).await?),
            Command::ZAdd => Some(handle_zadd(args, &self.config.store).await?),
        };

        if command.is_write() && self.config.role == ReplicaType::Leader {
            self.config.replication.publish(cmd_line.clone());
            self.config.replication.incr_num_commands();
        }

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

    // PING [message]
    fn handle_ping(&self, args: &[Value]) -> Value {
        let message = args.first().and_then(|v| match v {
            Value::Bulk(msg) => Some(msg),
            _ => None,
        });
        if matches!(self.mode, ClientMode::Subscribed { .. }) {
            let pong = Value::Bulk("pong".into());
            let msg = Value::Bulk(message.cloned().unwrap_or_default());
            Value::Array(vec![pong, msg])
        } else if let Some(msg) = message {
            Value::String(msg.clone())
        } else {
            Value::String("PONG".to_string())
        }
    }

    // PUBLISH channel message
    async fn handle_publish(&self, args: &[Value]) -> Result<Value> {
        if args.len() != 2 {
            bail!(RedisError::InvalidArgument);
        }
        let channel = unpack_bulk_string(&args[0])?;
        let message = unpack_bulk_string(&args[1])?;
        let subscribed = self.config.pubsub.publish(channel, message);
        Ok(Value::Integer(subscribed as i64))
    }

    // PSYNC replicationid offset
    async fn handle_psync(&mut self) -> Result<()> {
        let msg = Value::String(format!("FULLRESYNC {REPL_ID} {REPL_OFFSET}"));
        let rdb = BASE64_STANDARD.decode(EMPTY_RDB_B64)?;
        let rdb_msg = format!("${}\r\n", rdb.len());

        self.conn.write_value(&msg).await?;
        self.conn.write_all(rdb_msg.as_bytes()).await?;
        self.conn.write_all(&rdb).await?;
        self.conn.flush().await?;
        self.config.replication.run_replica(&mut self.conn).await
    }

    // SUBSCRIBE channel [channel ...]
    async fn handle_subscribe(&mut self, args: &[Value]) -> Result<()> {
        if args.is_empty() {
            bail!(RedisError::InvalidArgument);
        }

        let channels = args
            .iter()
            .map(|v| unpack_bulk_string(v))
            .collect::<Result<Vec<_>>>()?;

        // Ensure we're in Subscribed mode, creating the fan-in channel if needed.
        let maybe_rx = match &mut self.mode {
            ClientMode::Subscribed { .. } => None,
            _ => {
                let (tx, rx) = mpsc::channel(64);
                self.mode = ClientMode::Subscribed {
                    sub_tx: tx,
                    forwarders: HashMap::new(),
                };
                Some(rx)
            }
        };

        // Add new subscriptions and spawn forwarders
        if let ClientMode::Subscribed { sub_tx, forwarders } = &mut self.mode {
            for channel in channels {
                if forwarders.contains_key(channel) {
                    continue;
                }

                let mut broadcast_rx = self.config.pubsub.subscribe(channel);
                let tx = sub_tx.clone();
                let channel_name = channel.to_string();
                let handle = tokio::spawn(async move {
                    while let Ok(msg) = broadcast_rx.recv().await {
                        if tx.send((channel_name.clone(), msg)).await.is_err() {
                            break;
                        }
                    }
                });
                forwarders.insert(channel.to_string(), handle);

                let response = Value::Array(vec![
                    Value::Bulk("subscribe".into()),
                    Value::Bulk(channel.into()),
                    Value::Integer(forwarders.len() as i64),
                ]);
                self.conn.write_value(&response).await?;
            }
        }

        // If we created the fan-in channel in this call,
        // enter the subscribed-mode loop and drive I/O until all subscriptions are gone.
        if let Some(mut rx) = maybe_rx {
            while matches!(self.mode, ClientMode::Subscribed { .. }) {
                tokio::select! {
                    maybe_msg = rx.recv() => {
                        if let Some((channel, msg)) = maybe_msg {
                            let response = Value::Array(vec![
                                Value::Bulk("message".into()),
                                Value::Bulk(channel),
                                Value::Bulk(msg),
                            ]);
                            self.conn.write_value(&response).await?;
                        } else {
                            // All senders dropped; exit subscribed mode
                            break;
                        }
                    }
                    read = self.conn.read_value() => {
                        match read {
                            Ok(Some(value)) => {
                                match Box::pin(self.process(&value)).await {
                                    Ok(Some(response)) => self.conn.write_value(&response).await?,
                                    Ok(None) => (),
                                    Err(e) => {
                                        eprintln!("Error processing command: {e}");
                                        self.conn.write_value(&Value::Error(e.to_string())).await?;
                                    }
                                }
                            }
                            Ok(None) => break,
                            Err(e) => {
                                eprintln!("Error reading from stream: {e}");
                                break;
                            }
                        }
                    }
                }
            }
        }

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
            Some(RecordType::SortedSet(_)) => Value::Bulk("zset".into()),
            None => Value::Bulk("none".into()),
        };
        Ok(res)
    }

    // UNSUBSCRIBE [channel [channel ...]]
    async fn handle_unsubscribe(&mut self, args: &[Value]) -> Result<()> {
        let channels = args
            .iter()
            .map(|v| unpack_bulk_string(v))
            .collect::<Result<Vec<_>>>()?;

        let forwarders = match &mut self.mode {
            ClientMode::Subscribed { forwarders, .. } => forwarders,
            _ => return Ok(()),
        };

        let target_channels: Vec<String> = if channels.is_empty() {
            forwarders.keys().cloned().collect()
        } else {
            channels.iter().map(|c| c.to_string()).collect()
        };

        for channel in target_channels {
            if let Some(handle) = forwarders.remove(&channel) {
                handle.abort();
            }
            let response = Value::Array(vec![
                Value::Bulk("unsubscribe".into()),
                Value::Bulk(channel.into()),
                Value::Integer(forwarders.len() as i64),
            ]);
            self.conn.write_value(&response).await?;
        }

        if forwarders.is_empty() {
            self.mode = ClientMode::Normal;
        }

        Ok(())
    }

    // WAIT numreplicas timeout
    async fn handle_wait(&mut self, args: &[Value]) -> Result<Value> {
        if args.len() != 2 {
            bail!(RedisError::InvalidArgument);
        }
        let num_replicas = unpack_bulk_string(&args[0])?.parse::<usize>()?;
        let timeout_ms = unpack_bulk_string(&args[1])?.parse::<u64>()?;

        if self.config.replication.get_num_commands() == 0 {
            return Ok(Value::Integer(self.config.replication.num_replicas() as i64));
        }

        let repl_getack = Value::Array(vec![
            Value::Bulk("REPLCONF".into()),
            Value::Bulk("GETACK".into()),
            Value::Bulk("*".into()),
        ]);
        self.config.replication.publish(repl_getack);

        let end_time = Instant::now() + Duration::from_millis(timeout_ms);
        let num_ack = loop {
            let curr_acks = self.config.replication.get_num_ack();
            if Instant::now() >= end_time || curr_acks >= num_replicas {
                break curr_acks;
            }
            sleep(Duration::from_millis(50)).await;
        };

        self.config.replication.reset();
        Ok(Value::Integer(num_ack as i64))
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
            Ok(Value::NullArray)
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
        Ok(Value::NullArray)
    }
}

// ZADD key score member [score member ...]
pub async fn handle_zadd(args: &[Value], store: &Store) -> Result<Value> {
    if args.len() < 3 || (args.len() - 1) % 2 != 0 {
        bail!(RedisError::InvalidArgument);
    }
    let key = unpack_bulk_string(&args[0])?;
    let mut added = 0;

    for i in (1..args.len()).step_by(2) {
        let score = unpack_bulk_string(&args[i])?.parse::<f64>()?;
        let member = unpack_bulk_string(&args[i + 1])?;
        if store.zadd(key, member, score).await? {
            added += 1;
        }
    }

    Ok(Value::Integer(added))
}
