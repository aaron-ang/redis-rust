use std::{
    collections::HashMap,
    io::{BufReader, ErrorKind},
    str::FromStr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::SystemTime,
};

use anyhow::{bail, Result};
use base64::prelude::*;
use resp::{Decoder, Value};
use sha2::{Digest, Sha256};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::{sleep, Duration, Instant},
};
use tokio_stream::{wrappers::BroadcastStream, StreamExt, StreamMap};

use crate::config::Config;
use crate::data::{Store, StreamValue};
use crate::geo::{encode, is_valid_coordinate};
use crate::types::{Command, QuotedArgs, RedisError, XReadBlockType};

use super::args::{unpack_bulk_string, Args};
use super::replication::ReplicaType;
use super::resp::{encode_into, resp_encoded_len};

pub const BUFFER_SIZE: usize = 1024;
pub const REPL_OFFSET: usize = 0;
pub const REPL_ID: &str = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
pub const EMPTY_RDB_B64: &str = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";

enum ClientMode {
    Normal,
    Transaction {
        queued_commands: Vec<Value>,
    },
    Subscribed {
        subscriptions: StreamMap<String, BroadcastStream<String>>,
    },
}

pub struct Connection {
    stream: TcpStream,
    buffer: Vec<u8>,
    read_buf: Vec<u8>,
}

impl Connection {
    fn new(stream: TcpStream) -> Self {
        Connection {
            stream,
            buffer: Vec::with_capacity(BUFFER_SIZE),
            read_buf: vec![0; BUFFER_SIZE],
        }
    }

    pub async fn read_value(&mut self) -> Result<Value> {
        loop {
            if !self.buffer.is_empty() {
                let mut decoder = Decoder::new(BufReader::new(self.buffer.as_slice()));
                match decoder.decode() {
                    Ok(value) => {
                        let consumed = resp_encoded_len(&value);
                        self.buffer.drain(..consumed);
                        return Ok(value);
                    }
                    Err(e) if e.kind() == ErrorKind::UnexpectedEof => {
                        // Need more data, continue reading
                    }
                    Err(e) => return Err(e.into()),
                }
            }

            let bytes_read = self.stream.read(&mut self.read_buf).await?;
            if bytes_read == 0 {
                bail!("Connection closed")
            }

            self.buffer.extend_from_slice(&self.read_buf[..bytes_read]);
        }
    }

    async fn read_values(&mut self) -> Result<Vec<Value>> {
        let mut values = Vec::new();

        loop {
            if !self.buffer.is_empty() {
                let mut decoder = Decoder::new(BufReader::new(self.buffer.as_slice()));
                let mut total_consumed = 0;

                while let Ok(value) = decoder.decode() {
                    total_consumed += resp_encoded_len(&value);
                    values.push(value);
                }

                if total_consumed > 0 {
                    self.buffer.drain(..total_consumed);
                }

                if !values.is_empty() {
                    return Ok(values);
                }
            }

            let bytes_read = self.stream.read(&mut self.read_buf).await?;
            if bytes_read == 0 {
                if values.is_empty() {
                    bail!("Connection closed")
                }
                return Ok(values);
            }

            self.buffer.extend_from_slice(&self.read_buf[..bytes_read]);
        }
    }

    async fn write_all(&mut self, bytes: &[u8]) -> Result<()> {
        self.stream.write_all(bytes).await?;
        Ok(())
    }

    pub async fn write_value(&mut self, value: &Value) -> Result<()> {
        let mut buf = Vec::with_capacity(resp_encoded_len(value));
        encode_into(value, &mut buf);
        self.stream.write_all(&buf).await?;
        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        self.stream.flush().await?;
        Ok(())
    }
}

pub struct Server {
    config: Config,
    conn: Connection,
    mode: ClientMode,
    authenticated: bool,
    watched_keys: Vec<String>,
    dirty: Arc<AtomicBool>,
    response_buf: Vec<u8>,
}

impl Server {
    pub fn new(config: Config, stream: TcpStream) -> Self {
        let authenticated = config
            .acl_users
            .get("default")
            .is_some_and(|user| user.flags.contains("nopass"));
        Server {
            config,
            conn: Connection::new(stream),
            mode: ClientMode::Normal,
            authenticated,
            watched_keys: Vec::new(),
            dirty: Arc::new(AtomicBool::new(false)),
            response_buf: Vec::with_capacity(512),
        }
    }

    fn clear_watches(&mut self) {
        for key in self.watched_keys.drain(..) {
            self.config.store.remove_watcher(&key, &self.dirty);
        }
        self.dirty.store(false, Ordering::Release);
    }

    /// # Errors
    /// Returns an error if the TCP connection fails.
    pub async fn handle_conn(&mut self) -> Result<()> {
        loop {
            if let ClientMode::Subscribed { subscriptions } = &mut self.mode {
                // Subscribed mode: select between incoming messages and client commands
                tokio::select! {
                    Some((channel, msg)) = subscriptions.next() => {
                        if let Ok(msg) = msg {
                            let response = Value::Array(vec![
                                Value::Bulk("message".into()),
                                Value::Bulk(channel),
                                Value::Bulk(msg),
                            ]);
                            self.conn.write_value(&response).await?;
                        }
                    }
                    read = self.conn.read_value() => {
                        match read {
                            Ok(value) => {
                                match self.process(&value).await {
                                    Ok(Some(response)) => self.conn.write_value(&response).await?,
                                    Ok(None) => (),
                                    Err(e) => {
                                        eprintln!("Error processing command: {e}");
                                        self.conn.write_value(&Value::Error(e.to_string())).await?;
                                    }
                                }
                            }
                            Err(e) => {
                                eprintln!("Error reading from stream: {e}");
                                break;
                            }
                        }
                    }
                }
            } else {
                // Normal / Transaction mode: batch read + process
                let values = match self.conn.read_values().await {
                    Ok(values) => values,
                    Err(e) => {
                        eprintln!("Error reading from stream: {e}");
                        break;
                    }
                };

                self.response_buf.clear();
                for value in values {
                    match self.process(&value).await {
                        Ok(Some(response)) => encode_into(&response, &mut self.response_buf),
                        Ok(None) => (),
                        Err(e) => {
                            eprintln!("Error processing command: {e}");
                            encode_into(&Value::Error(e.to_string()), &mut self.response_buf);
                        }
                    }
                }

                if !self.response_buf.is_empty() {
                    self.conn.write_all(&self.response_buf).await?;
                }
            }
        }
        Ok(())
    }

    /// # Errors
    /// Returns a `RedisError` for invalid commands, auth failures, or command-specific errors.
    pub async fn process(&mut self, cmd_line: &Value) -> Result<Option<Value>> {
        let (command, args) = extract_command(cmd_line)?;

        if !self.authenticated && command != Command::Auth {
            bail!("NOAUTH Authentication required.")
        }

        if matches!(self.mode, ClientMode::Subscribed { .. })
            && !matches!(
                command,
                Command::Subscribe | Command::Unsubscribe | Command::Ping
            )
        {
            bail!(RedisError::CommandWithoutSubscribe(command))
        }

        if matches!(self.mode, ClientMode::Transaction { .. }) {
            return self.process_transaction(command, cmd_line).await;
        }

        let response = match command {
            Command::Acl => Some(self.handle_acl(args)?),
            Command::Auth => Some(self.handle_auth(args)?),
            Command::BLPop => Some(self.handle_blpop(args).await?),
            Command::Cmd => Some(Value::Array(vec![])),
            Command::Config => Some(self.handle_config(args)?),
            Command::DbSize => Some(self.handle_db_size()),
            Command::Discard | Command::Exec => bail!(RedisError::CommandWithoutMulti(command)),
            Command::Echo => Some(handle_echo(args)?),
            Command::FlushAll => Some(self.handle_flushall()),
            Command::GeoAdd => Some(self.handle_geoadd(args)?),
            Command::GeoDist => Some(self.handle_geodist(args)?),
            Command::GeoPos => Some(self.handle_geopos(args)?),
            Command::GeoSearch => Some(self.handle_geosearch(args)?),
            Command::Get => Some(self.handle_get(args)?),
            Command::Incr => Some(self.handle_incr(args)?),
            Command::Info => Some(self.handle_info()),
            Command::Keys => Some(self.handle_keys(args)?),
            Command::LLen => Some(self.handle_llen(args)?),
            Command::LPop => Some(self.handle_lpop(args)?),
            Command::LPush => Some(self.handle_lpush(args)?),
            Command::LRange => Some(self.handle_lrange(args)?),
            Command::Multi => Some(self.handle_multi()),
            Command::Ping => Some(self.handle_ping(args)),
            Command::Publish => Some(self.handle_publish(args)?),
            Command::PSync => self.handle_psync().await?,
            Command::ReplConf => Some(Value::String("OK".into())),
            Command::RPush => Some(self.handle_rpush(args)?),
            Command::Set => Some(self.handle_set(args)?),
            Command::Subscribe => self.handle_subscribe(args).await?,
            Command::Type => Some(self.handle_type(args)?),
            Command::Unsubscribe => self.handle_unsubscribe(args).await?,
            Command::Unwatch => Some(self.handle_unwatch()),
            Command::Wait => Some(self.handle_wait(args).await?),
            Command::Watch => Some(self.handle_watch(args)?),
            Command::XAdd => Some(self.handle_xadd(args)?),
            Command::XRange => Some(self.handle_xrange(args)?),
            Command::XRead => Some(self.handle_xread(args).await?),
            Command::ZAdd => Some(self.handle_zadd(args)?),
            Command::ZCard => Some(self.handle_zcard(args)?),
            Command::ZRange => Some(self.handle_zrange(args)?),
            Command::ZRank => Some(self.handle_zrank(args)?),
            Command::ZRem => Some(self.handle_zrem(args)?),
            Command::ZScore => Some(self.handle_zscore(args)?),
        };

        if command.is_write() {
            if self.config.role == ReplicaType::Leader && self.config.replication.has_replicas() {
                self.config.replication.publish(cmd_line);
                self.config.replication.incr_num_commands();
            }
            if let Some(aof) = &self.config.aof {
                aof.append(cmd_line)?;
            }
        }

        Ok(response)
    }

    async fn process_transaction(
        &mut self,
        command: Command,
        cmd_line: &Value,
    ) -> Result<Option<Value>> {
        let response = match command {
            Command::Exec => {
                let ClientMode::Transaction { queued_commands } = &mut self.mode else {
                    bail!("Expected transaction mode")
                };
                let watch_violated = self.dirty.load(Ordering::Acquire);
                let commands = std::mem::take(queued_commands);
                self.mode = ClientMode::Normal;
                self.clear_watches();

                if watch_violated {
                    Some(Value::NullArray)
                } else {
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
            }
            Command::Discard => {
                self.mode = ClientMode::Normal;
                self.clear_watches();
                Some(Value::String("OK".into()))
            }
            Command::Watch => bail!(RedisError::WatchInsideMulti),
            _ => {
                if let ClientMode::Transaction { queued_commands } = &mut self.mode {
                    queued_commands.push(cmd_line.clone());
                }
                Some(Value::String("QUEUED".into()))
            }
        };
        Ok(response)
    }

    // ACL subcommand
    fn handle_acl(&self, args: &[Value]) -> Result<Value> {
        let subcommand = args.bulk(0)?;
        match subcommand.to_lowercase().as_str() {
            "whoami" => Ok(Value::Bulk("default".into())),
            "getuser" => {
                let username = args.bulk(1)?;
                if let Some(user) = self.config.acl_users.get(username) {
                    let flags: Vec<Value> =
                        user.flags.names().map(|s| Value::Bulk(s.into())).collect();
                    let passwords: Vec<Value> = user
                        .passwords
                        .iter()
                        .map(|s| Value::Bulk(s.clone()))
                        .collect();
                    Ok(Value::Array(vec![
                        Value::Bulk("flags".into()),
                        Value::Array(flags),
                        Value::Bulk("passwords".into()),
                        Value::Array(passwords),
                    ]))
                } else {
                    Ok(Value::Null)
                }
            }
            "setuser" => {
                let username = args.bulk(1)?.to_string();
                let mut user = self
                    .config
                    .acl_users
                    .get_mut(&username)
                    .ok_or_else(|| anyhow::anyhow!("ERR User '{username}' does not exist"))?;
                for rule in args.iter().skip(2) {
                    let rule_str = unpack_bulk_string(rule)?;
                    if let Some(password) = rule_str.strip_prefix('>') {
                        let hash = Sha256::digest(password.as_bytes());
                        let hex_hash = format!("{hash:x}");
                        user.passwords.push(hex_hash);
                        user.flags.set("nopass", false);
                    }
                }
                Ok(Value::String("OK".into()))
            }
            _ => bail!(RedisError::UnknownCommand(
                format!("ACL {subcommand}"),
                QuotedArgs(vec![subcommand.to_string()])
            )),
        }
    }

    // AUTH [username] password
    fn handle_auth(&mut self, args: &[Value]) -> Result<Value> {
        args.min_len(2)?;
        let username = args.bulk(0)?;
        let password = args.bulk(1)?;
        let user = self.config.acl_users.get(username).ok_or_else(|| {
            anyhow::anyhow!("WRONGPASS invalid username-password pair or user is disabled.")
        })?;
        let password_ok = if user.flags.contains("nopass") {
            true
        } else {
            let hash = Sha256::digest(password.as_bytes());
            let hex_hash = format!("{hash:x}");
            user.passwords.iter().any(|p| p == &hex_hash)
        };
        if password_ok {
            self.authenticated = true;
            Ok(Value::String("OK".into()))
        } else {
            bail!("WRONGPASS invalid username-password pair or user is disabled.")
        }
    }

    // CONFIG GET parameter
    fn handle_config(&self, args: &[Value]) -> Result<Value> {
        args.min_len(2)?;
        let cmd = Command::from_str(args.bulk(0)?)?;
        let res = match cmd {
            Command::Get => {
                let name = args.bulk(1)?;
                let lower = name.to_lowercase();
                let value = match lower.as_str() {
                    "dir" => Some(Value::Bulk(
                        self.config
                            .dir
                            .clone()
                            .into_os_string()
                            .into_string()
                            .unwrap_or_default(),
                    )),
                    "dbfilename" => Some(Value::Bulk(self.config.dbfilename.clone())),
                    "appendonly" => Some(Value::Bulk(
                        if self.config.appendonly { "yes" } else { "no" }.to_string(),
                    )),
                    "appenddirname" => Some(Value::Bulk(self.config.appenddirname.clone())),
                    "appendfilename" => Some(Value::Bulk(self.config.appendfilename.clone())),
                    "appendfsync" => Some(Value::Bulk(self.config.appendfsync.clone())),
                    _ => None,
                };
                match value {
                    Some(v) => Value::Array(vec![Value::Bulk(lower), v]),
                    None => Value::Array(vec![]),
                }
            }
            cmd => bail!("Unsupported CONFIG subcommand: {cmd}"),
        };
        Ok(res)
    }

    // DBSIZE
    fn handle_db_size(&self) -> Value {
        Value::Integer(i64::try_from(self.config.store.db_size()).unwrap_or(i64::MAX))
    }

    // FLUSHALL
    fn handle_flushall(&self) -> Value {
        self.config.store.flushall();
        Value::String("OK".into())
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
    fn handle_keys(&self, args: &[Value]) -> Result<Value> {
        args.exact_len(1)?;
        let keys = self.config.store.keys(args.bulk(0)?)?;
        Ok(Value::Array(keys.into_iter().map(Value::Bulk).collect()))
    }

    // LLEN key
    fn handle_llen(&self, args: &[Value]) -> Result<Value> {
        Ok(Value::Integer(self.config.store.llen(args.bulk(0)?)?))
    }

    // LRANGE key start stop
    fn handle_lrange(&self, args: &[Value]) -> Result<Value> {
        args.exact_len(3)?;
        let key = args.bulk(0)?;
        let start = args.bulk(1)?.parse::<i64>()?;
        let end = args.bulk(2)?.parse::<i64>()?;

        let values = self.config.store.lrange(key, start, end)?;
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
    fn handle_publish(&self, args: &[Value]) -> Result<Value> {
        args.exact_len(2)?;
        let subscribed = self.config.pubsub.publish(args.bulk(0)?, args.bulk(1)?);
        Ok(Value::Integer(
            i64::try_from(subscribed).unwrap_or(i64::MAX),
        ))
    }

    // PSYNC replicationid offset
    async fn handle_psync(&mut self) -> Result<Option<Value>> {
        let msg = Value::String(format!("FULLRESYNC {REPL_ID} {REPL_OFFSET}"));
        let rdb = BASE64_STANDARD.decode(EMPTY_RDB_B64)?;
        let rdb_msg = format!("${}\r\n", rdb.len());

        self.conn.write_value(&msg).await?;
        self.conn.write_all(rdb_msg.as_bytes()).await?;
        self.conn.write_all(&rdb).await?;
        self.conn.flush().await?;
        self.config.replication.run_replica(&mut self.conn).await?;
        Ok(None)
    }

    // SUBSCRIBE channel [channel ...]
    async fn handle_subscribe(&mut self, args: &[Value]) -> Result<Option<Value>> {
        args.min_len(1)?;
        let channels = args.bulks_from(0)?;

        // Ensure we're in Subscribed mode
        if !matches!(self.mode, ClientMode::Subscribed { .. }) {
            self.mode = ClientMode::Subscribed {
                subscriptions: StreamMap::new(),
            };
        }
        let ClientMode::Subscribed { subscriptions } = &mut self.mode else {
            unreachable!()
        };

        for channel in channels {
            if subscriptions.contains_key(channel) {
                continue;
            }

            let broadcast_rx = self.config.pubsub.subscribe(channel);
            subscriptions.insert(channel.to_string(), BroadcastStream::new(broadcast_rx));

            let response = Value::Array(vec![
                Value::Bulk("subscribe".into()),
                Value::Bulk(channel.into()),
                Value::Integer(i64::try_from(subscriptions.len()).unwrap_or(i64::MAX)),
            ]);
            self.conn.write_value(&response).await?;
        }

        Ok(None)
    }

    // TYPE key
    fn handle_type(&self, args: &[Value]) -> Result<Value> {
        args.exact_len(1)?;
        Ok(Value::String(self.config.store.type_(args.bulk(0)?)))
    }

    // UNSUBSCRIBE [channel [channel ...]]
    async fn handle_unsubscribe(&mut self, args: &[Value]) -> Result<Option<Value>> {
        let ClientMode::Subscribed { subscriptions } = &mut self.mode else {
            return Ok(Some(Value::Array(vec![
                Value::Bulk("unsubscribe".into()),
                Value::Null,
                Value::Integer(0),
            ])));
        };

        let channels = args.bulks_from(0)?;

        let target_channels: Vec<String> = if channels.is_empty() {
            subscriptions.keys().cloned().collect()
        } else {
            channels
                .iter()
                .map(std::string::ToString::to_string)
                .collect()
        };

        for channel in target_channels {
            subscriptions.remove(&channel);
            let response = Value::Array(vec![
                Value::Bulk("unsubscribe".into()),
                Value::Bulk(channel),
                Value::Integer(i64::try_from(subscriptions.len()).unwrap_or(i64::MAX)),
            ]);
            self.conn.write_value(&response).await?;
        }

        if subscriptions.is_empty() {
            self.mode = ClientMode::Normal;
        }

        Ok(None)
    }

    // MULTI
    fn handle_multi(&mut self) -> Value {
        self.mode = ClientMode::Transaction {
            queued_commands: Vec::new(),
        };
        Value::String("OK".into())
    }

    // UNWATCH
    fn handle_unwatch(&mut self) -> Value {
        self.clear_watches();
        Value::String("OK".into())
    }

    // WATCH key [key ...]
    fn handle_watch(&mut self, args: &[Value]) -> Result<Value> {
        for key in args.bulks_from(0)? {
            self.config.store.add_watcher(key, Arc::clone(&self.dirty));
            self.watched_keys.push(key.to_string());
        }
        Ok(Value::String("OK".into()))
    }

    // WAIT numreplicas timeout
    async fn handle_wait(&mut self, args: &[Value]) -> Result<Value> {
        if self.config.replication.get_num_commands() == 0 {
            return Ok(Value::Integer(
                i64::try_from(self.config.replication.num_replicas()).unwrap_or(i64::MAX),
            ));
        }

        args.exact_len(2)?;
        let num_replicas: usize = args.bulk(0)?.parse()?;
        let timeout_ms: u64 = args.bulk(1)?.parse()?;

        // Ask all replicas to send ACKs.
        self.config.replication.publish(&Value::Array(vec![
            Value::Bulk("REPLCONF".into()),
            Value::Bulk("GETACK".into()),
            Value::Bulk("*".into()),
        ]));

        let deadline = Instant::now() + Duration::from_millis(timeout_ms);
        let mut num_ack;

        loop {
            num_ack = self.config.replication.get_num_ack();
            if num_ack >= num_replicas || Instant::now() >= deadline {
                break;
            }
            sleep(Duration::from_millis(50)).await;
        }

        self.config.replication.reset();

        Ok(Value::Integer(i64::try_from(num_ack).unwrap_or(i64::MAX)))
    }

    // XRANGE key start end
    fn handle_xrange(&self, args: &[Value]) -> Result<Value> {
        args.exact_len(3)?;
        let key = args.bulk(0)?;
        let start = args.bulk(1)?;
        let end = args.bulk(2)?;

        let stream_entries = self
            .config
            .store
            .get_range_stream_entries(key, start, end)?;

        if stream_entries.is_empty() {
            Ok(Value::Null)
        } else {
            let values = build_stream_entry_list(&stream_entries);
            Ok(Value::Array(values))
        }
    }

    // XREAD [BLOCK milliseconds] STREAMS key [key ...] id [id ...]
    async fn handle_xread(&self, args: &[Value]) -> Result<Value> {
        args.min_len(3)?;

        let mut args_iter = args.iter();
        let mut block_option = XReadBlockType::NoWait;

        loop {
            match args_iter.next() {
                Some(Value::Bulk(b)) if b.to_lowercase() == "block" => {
                    if let Some(Value::Bulk(ms)) = args_iter.next() {
                        block_option = ms.parse()?;
                    } else {
                        bail!(RedisError::SyntaxError)
                    }
                }
                Some(Value::Bulk(b)) if b.to_lowercase() == "streams" => break,
                _ => bail!(RedisError::SyntaxError),
            }
        }

        let stream_args = args_iter.collect::<Vec<_>>();
        if stream_args.len() % 2 != 0 {
            bail!(RedisError::InvalidArgument)
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
            let values = streams
                .into_iter()
                .filter_map(|(key, _)| {
                    entries.get(key).map(|stream| {
                        Value::Array(vec![
                            Value::Bulk(key.to_string()),
                            Value::Array(build_stream_entry_list(stream)),
                        ])
                    })
                })
                .collect();
            Ok(Value::Array(values))
        }
    }

    // ZCARD key
    fn handle_zcard(&self, args: &[Value]) -> Result<Value> {
        args.exact_len(1)?;
        Ok(Value::Integer(self.config.store.zcard(args.bulk(0)?)?))
    }

    // ZRANGE key start stop
    fn handle_zrange(&self, args: &[Value]) -> Result<Value> {
        args.exact_len(3)?;
        let key = args.bulk(0)?;
        let start = args.bulk(1)?.parse::<i64>()?;
        let stop = args.bulk(2)?.parse::<i64>()?;

        Ok(Value::Array(
            self.config
                .store
                .zrange(key, start, stop)?
                .into_iter()
                .map(Value::Bulk)
                .collect(),
        ))
    }

    // ZRANK key member
    fn handle_zrank(&self, args: &[Value]) -> Result<Value> {
        args.exact_len(2)?;
        match self.config.store.zrank(args.bulk(0)?, args.bulk(1)?)? {
            Some(rank) => Ok(Value::Integer(rank)),
            None => Ok(Value::Null),
        }
    }

    // ZSCORE key member
    fn handle_zscore(&self, args: &[Value]) -> Result<Value> {
        args.exact_len(2)?;
        match self.config.store.zscore(args.bulk(0)?, args.bulk(1)?)? {
            Some(score) => Ok(Value::Bulk(score.to_string())),
            None => Ok(Value::Null),
        }
    }

    // GEODIST key member1 member2
    fn handle_geodist(&self, args: &[Value]) -> Result<Value> {
        args.min_len(3)?;
        let distance = self
            .config
            .store
            .geodist(args.bulk(0)?, args.bulk(1)?, args.bulk(2)?)?;
        match distance {
            Some(d) => Ok(Value::Bulk(d.to_string())),
            None => Ok(Value::Null),
        }
    }

    // GEOPOS key [member [member ...]]
    fn handle_geopos(&self, args: &[Value]) -> Result<Value> {
        let key = args.bulk(0)?;
        let members = args.bulks_from(1)?;
        let positions = self.config.store.geopos(key, &members)?;

        let result = positions
            .into_iter()
            .map(|pos| match pos {
                Some((lat, lon)) => Value::Array(vec![
                    Value::Bulk(lon.to_string()),
                    Value::Bulk(lat.to_string()),
                ]),
                None => Value::NullArray,
            })
            .collect();

        Ok(Value::Array(result))
    }

    // GEOSEARCH key <FROMLONLAT longitude latitude> <BYRADIUS radius <M | KM |FT | MI>>
    fn handle_geosearch(&self, args: &[Value]) -> Result<Value> {
        args.exact_len(7)?;
        let key = args.bulk(0)?;
        let from_lon = args.bulk(2)?.parse::<f64>()?;
        let from_lat = args.bulk(3)?.parse::<f64>()?;
        let radius = args.bulk(5)?.parse::<f64>()?;
        let unit = args.bulk(6)?;

        let radius_m = match unit.to_uppercase().as_str() {
            "M" => radius,
            "KM" => radius * 1000.0,
            "MI" => radius * 1609.344,
            "FT" => radius * 0.3048,
            _ => bail!(RedisError::SyntaxError),
        };

        let results = self
            .config
            .store
            .geosearch(key, from_lon, from_lat, radius_m)?;

        Ok(Value::Array(results.into_iter().map(Value::Bulk).collect()))
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.clear_watches();
    }
}

fn build_stream_entry_list(entries: &StreamValue) -> Vec<Value> {
    entries
        .iter()
        .map(|(id, fields)| {
            let field_values = fields
                .iter()
                .flat_map(|(field, value)| {
                    vec![Value::Bulk(field.clone()), Value::Bulk(value.clone())]
                })
                .collect();
            Value::Array(vec![
                Value::Bulk(id.to_string()),
                Value::Array(field_values),
            ])
        })
        .collect()
}

pub fn extract_command(value: &Value) -> Result<(Command, &[Value])> {
    let Value::Array(args) = value else {
        bail!("Expected array value")
    };
    let command_str = unpack_bulk_string(&args[0])?;
    if let Ok(command) = Command::from_str(command_str) {
        Ok((command, &args[1..]))
    } else {
        let arg_str_vec = QuotedArgs(
            args[1..]
                .iter()
                .filter_map(|v| v.to_encoded_string().ok())
                .collect::<Vec<_>>(),
        );
        bail!(RedisError::UnknownCommand(
            command_str.to_string(),
            arg_str_vec
        ))
    }
}

// ECHO message
fn handle_echo(args: &[Value]) -> Result<Value> {
    args.min_len(1)?;
    Ok(args[0].clone())
}

/// Command handlers shared between `Server` and `Follower`.
pub(crate) trait StoreCommands {
    fn store(&self) -> &Store;

    // SET key value [PX milliseconds]
    fn handle_set(&self, args: &[Value]) -> Result<Value> {
        args.min_len(2)?;
        let key = args.bulk(0)?;
        let value = args.bulk(1)?;

        let expiry = match args.get(2) {
            Some(Value::Bulk(option)) if option.eq_ignore_ascii_case("px") => {
                let ms = match args.get(3) {
                    Some(Value::Bulk(arg)) => {
                        arg.parse::<u64>().map_err(|_| RedisError::InvalidInteger)?
                    }
                    _ => bail!(RedisError::SyntaxError),
                };
                Some(SystemTime::now() + Duration::from_millis(ms))
            }
            Some(_) => bail!(RedisError::SyntaxError),
            None => None,
        };

        self.store().set(key.to_string(), value.into(), expiry);
        Ok(Value::String("OK".into()))
    }

    // GET key
    fn handle_get(&self, args: &[Value]) -> Result<Value> {
        let key = args.bulk(0)?;
        match self.store().get(key) {
            Some(s) => Ok(Value::Bulk(s.to_string())),
            None => Ok(Value::Null),
        }
    }

    // XADD key <* | id> field value [field value ...]
    fn handle_xadd(&self, args: &[Value]) -> Result<Value> {
        args.min_len(2)?;
        let key = args.bulk(0)?;
        let entry_id = args.bulk(1)?;
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
            bail!(RedisError::InvalidArgument)
        }

        match self.store().add_stream_entry(key, entry_id, values) {
            Ok(stream_entry_id) => Ok(Value::Bulk(stream_entry_id.to_string())),
            Err(e) => Ok(Value::Error(e.to_string())),
        }
    }

    // INCR key
    fn handle_incr(&self, args: &[Value]) -> Result<Value> {
        let key = args.bulk(0)?;
        let value = self.store().incr(key)?;
        Ok(Value::Integer(value))
    }

    // RPUSH key element [element ...]
    fn handle_rpush(&self, args: &[Value]) -> Result<Value> {
        args.min_len(2)?;
        let key = args.bulk(0)?;
        let elements = args.bulks_from(1)?;
        let count = self.store().rpush(key, &elements)?;
        Ok(Value::Integer(count))
    }

    // LPOP key [count]
    fn handle_lpop(&self, args: &[Value]) -> Result<Value> {
        let key = args.bulk(0)?;
        let count = if let Some(arg) = args.get(1) {
            match arg {
                Value::Bulk(c) => {
                    let parsed = c.parse::<i64>()?;
                    if parsed < 0 {
                        bail!(RedisError::InvalidInteger)
                    }
                    Some(usize::try_from(parsed).unwrap_or(usize::MAX))
                }
                Value::Integer(c) => {
                    if *c < 0 {
                        bail!(RedisError::InvalidInteger)
                    }
                    Some(usize::try_from(*c).unwrap_or(usize::MAX))
                }
                _ => bail!(RedisError::SyntaxError),
            }
        } else {
            None
        };

        let elements = self.store().lpop(key, count.unwrap_or(1))?;

        match (elements.is_empty(), count) {
            (true, _) => Ok(Value::Null),
            (false, None) => Ok(Value::Bulk(elements[0].clone())),
            (false, Some(_)) => Ok(Value::Array(
                elements.into_iter().map(Value::Bulk).collect(),
            )),
        }
    }

    // LPUSH key element [element ...]
    fn handle_lpush(&self, args: &[Value]) -> Result<Value> {
        args.min_len(2)?;
        let key = args.bulk(0)?;
        let elements = args.bulks_from(1)?;
        let count = self.store().lpush(key, &elements)?;
        Ok(Value::Integer(count))
    }

    // BLPOP key [key ...] timeout
    async fn handle_blpop(&self, args: &[Value]) -> Result<Value> {
        args.min_len(2)?;
        let timeout = args.bulk(args.len() - 1)?.parse::<f64>()?;
        let keys = args[..args.len() - 1]
            .iter()
            .map(unpack_bulk_string)
            .collect::<Result<Vec<_>>>()?;
        let elements = self.store().blpop(&keys, timeout).await?;

        if let Some((key, value)) = elements {
            Ok(Value::Array(vec![Value::Bulk(key), Value::Bulk(value)]))
        } else {
            Ok(Value::NullArray)
        }
    }

    // ZADD key score member [score member ...]
    #[allow(clippy::similar_names)]
    fn handle_zadd(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 3 || !(args.len() - 1).is_multiple_of(2) {
            bail!(RedisError::InvalidArgument);
        }
        let key = args.bulk(0)?;
        let mut added = 0;

        for i in (1..args.len()).step_by(2) {
            let score = args.bulk(i)?.parse::<f64>()?;
            let member = args.bulk(i + 1)?;
            if self.store().zadd(key, member, score)? {
                added += 1;
            }
        }

        Ok(Value::Integer(added))
    }

    // ZREM key member [member ...]
    fn handle_zrem(&self, args: &[Value]) -> Result<Value> {
        args.min_len(2)?;
        let key = args.bulk(0)?;
        let members = args.bulks_from(1)?;
        let removed = self.store().zrem(key, &members)?;
        Ok(Value::Integer(removed))
    }

    // GEOADD key longitude latitude member [longitude latitude member ...]
    #[allow(clippy::similar_names, clippy::cast_precision_loss)]
    fn handle_geoadd(&self, args: &[Value]) -> Result<Value> {
        let arg_len = args.len();
        if arg_len < 4 || !(arg_len - 1).is_multiple_of(3) {
            bail!(RedisError::InvalidArgument)
        }

        let key = args.bulk(0)?;
        let mut parsed_entries = Vec::with_capacity((arg_len - 1) / 3);
        for chunk in args[1..].chunks(3) {
            let longitude = unpack_bulk_string(&chunk[0])?.parse::<f64>()?;
            let latitude = unpack_bulk_string(&chunk[1])?.parse::<f64>()?;
            let member = unpack_bulk_string(&chunk[2])?;
            if !is_valid_coordinate(latitude, longitude) {
                bail!(RedisError::InvalidCoordinates(longitude, latitude))
            }
            parsed_entries.push((longitude, latitude, member));
        }

        let mut added = 0;
        for (longitude, latitude, member) in parsed_entries {
            let score = encode(latitude, longitude);
            if self.store().zadd(key, member, score as f64)? {
                added += 1;
            }
        }

        Ok(Value::Integer(added))
    }

    /// Dispatch a single write command against the store. Used for AOF replay
    /// and replication streams where only mutation effects matter.
    async fn dispatch_write(&self, cmd: &Value) -> Result<()> {
        let (command, args) = extract_command(cmd)?;
        match command {
            Command::Set => {
                self.handle_set(args)?;
            }
            Command::Incr => {
                self.handle_incr(args)?;
            }
            Command::LPush => {
                self.handle_lpush(args)?;
            }
            Command::RPush => {
                self.handle_rpush(args)?;
            }
            Command::LPop => {
                self.handle_lpop(args)?;
            }
            Command::BLPop => {
                self.handle_blpop(args).await?;
            }
            Command::XAdd => {
                self.handle_xadd(args)?;
            }
            Command::ZAdd => {
                self.handle_zadd(args)?;
            }
            Command::ZRem => {
                self.handle_zrem(args)?;
            }
            Command::GeoAdd => {
                self.handle_geoadd(args)?;
            }
            _ => bail!("unsupported command in AOF replay: {command}"),
        }
        Ok(())
    }
}

impl StoreCommands for Server {
    fn store(&self) -> &Store {
        &self.config.store
    }
}
