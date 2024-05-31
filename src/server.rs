use crate::db::Store;
use crate::util::{ReplicaType, ReplicationState};

use anyhow::Result;
use base64::prelude::*;
use resp::{Decoder, Value};
use std::{io::BufReader, sync::Arc, time::SystemTime};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::{
        broadcast::{error::RecvError, Sender},
        Mutex,
    },
    time::{sleep, timeout, Duration, Instant},
};

pub const REPL_ID: &str = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
pub const REPL_OFFSET: usize = 0;
pub const EMPTY_RDB_B64: &str = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";

pub struct Server {
    store: Store,
    role: ReplicaType,
    rep_state: Arc<Mutex<ReplicationState>>,
}

impl Server {
    pub fn new(store: Store, role: ReplicaType, rep_state: Arc<Mutex<ReplicationState>>) -> Self {
        Self {
            store,
            role,
            rep_state,
        }
    }

    pub async fn handle_conn(
        &mut self,
        mut stream: TcpStream,
        tx: Arc<Sender<Value>>,
    ) -> Result<()> {
        let mut replication = false;
        let mut buf = vec![0; 1024];

        loop {
            let bytes_read = stream.read(&mut buf).await?;
            if bytes_read == 0 {
                eprintln!("Client disconnected");
                break;
            }
            let mut decoder = Decoder::new(BufReader::new(&buf[..bytes_read]));
            while let Some(value) = decoder.decode().ok() {
                let (command, args) = extract_command(&value);
                let response = match command.to_lowercase().as_str() {
                    "ping" => Some(Value::String("PONG".into())),
                    "echo" => Some(args.first().unwrap().clone()),
                    "set" => {
                        let r = handle_set(args, &self.store);
                        if r.is_some() && self.role == ReplicaType::Leader {
                            tx.send(value.clone()).unwrap();
                            r
                        } else {
                            None
                        }
                    }
                    "get" => handle_get(args, &self.store),
                    "info" => handle_info(self.role),
                    "replconf" => Some(Value::String("OK".into())),
                    "psync" => handle_psync(&mut stream, &mut replication).await,
                    "wait" => self.handle_wait(&args, &tx).await,
                    c => {
                        eprintln!("Unknown command: {}", c);
                        None
                    }
                };
                if let Some(r) = response {
                    stream.write_all(&r.encode()).await?;
                }
                self.rep_state.lock().await.set_prev_client_cmd(command);
            }

            if replication {
                let mut rx = tx.subscribe();
                loop {
                    match rx.recv().await {
                        Ok(v) => {
                            stream.write_all(&v.encode()).await?;
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
                        timeout(Duration::from_millis(100), stream.read(&mut buf)).await
                    {
                        let mut decoder = Decoder::new(BufReader::new(&buf[..bytes_read?]));
                        let mut rep_state = self.rep_state.lock().await;
                        while let Some(_) = decoder.decode().ok() {
                            rep_state.incr_num_ack();
                        }
                    }
                }
            }
        }
        Ok(())
    }

    async fn handle_wait(&mut self, args: &Vec<Value>, tx: &Arc<Sender<Value>>) -> Option<Value> {
        if args.len() < 2 {
            return None;
        }
        let limit = unpack_bulk_string(&args[0])
            .unwrap()
            .parse::<usize>()
            .unwrap();
        let timeout = unpack_bulk_string(&args[1])
            .unwrap()
            .parse::<u64>()
            .unwrap();

        if limit <= 0 {
            return Some(Value::Integer((tx.receiver_count() - 1) as i64)); // ignore first receiver
        }

        if self.rep_state.lock().await.get_prev_client_cmd() != "set" {
            sleep(Duration::from_millis(timeout)).await;
            Some(Value::Integer((tx.receiver_count() - 1) as i64)) // ignore first receiver
        } else {
            let repl_getack = Value::Array(vec![
                Value::Bulk("REPLCONF".into()),
                Value::Bulk("GETACK".into()),
                Value::Bulk("*".into()),
            ]);
            tx.send(repl_getack).unwrap();
            let end = Instant::now() + Duration::from_millis(timeout);
            let num_ack = loop {
                let rep_state = self.rep_state.lock().await;
                let num_ack = rep_state.get_num_ack();
                if Instant::now() >= end || num_ack >= limit {
                    break num_ack;
                }
                drop(rep_state);
                sleep(Duration::from_millis(100)).await;
            };
            let mut rep_state = self.rep_state.lock().await;
            rep_state.reset_num_ack();
            rep_state.set_prev_client_cmd("".into());
            Some(Value::Integer(num_ack as i64)) // ignore first receiver
        }
    }
}

pub fn extract_command(value: &Value) -> (String, Vec<Value>) {
    if let Value::Array(a) = value {
        let command = unpack_bulk_string(a.first().unwrap()).unwrap();
        let args = a.iter().skip(1).cloned().collect();
        (command, args)
    } else {
        (String::new(), vec![])
    }
}

fn unpack_bulk_string(value: &Value) -> Result<String> {
    if let Value::Bulk(s) = value {
        Ok(s.into())
    } else {
        Err(anyhow::anyhow!("Expected command to be bulk string"))
    }
}

pub fn handle_set(args: Vec<Value>, store: &Store) -> Option<Value> {
    if args.len() < 2 {
        return None;
    }

    let mut iter = args.into_iter();
    let key = unpack_bulk_string(&iter.next().unwrap()).unwrap();
    let value = unpack_bulk_string(&iter.next().unwrap()).unwrap();
    let mut expiry: Option<SystemTime> = None;

    if let Some(Value::Bulk(option)) = iter.next() {
        match option.to_lowercase().as_str() {
            "px" => {
                let ms = match iter.next() {
                    Some(Value::Bulk(arg)) => arg.parse::<u64>().unwrap_or(0),
                    _ => return None,
                };
                expiry = Some(SystemTime::now() + Duration::from_millis(ms));
            }
            _ => return None,
        }
    }

    store.set(key, value, expiry);
    Some(Value::String("OK".into()))
}

pub fn handle_get(args: Vec<Value>, store: &Store) -> Option<Value> {
    if args.len() < 1 {
        return Some(Value::Null);
    }
    let key = unpack_bulk_string(&args[0]).unwrap();
    store
        .get(&key)
        .map(|v| Value::Bulk(v))
        .or(Some(Value::Null))
}

fn handle_info(role: ReplicaType) -> Option<Value> {
    let value =
        format!("role:{role}\r\nmaster_replid:{REPL_ID}\r\nmaster_repl_offset:{REPL_OFFSET}");
    Some(Value::Bulk(value))
}

async fn handle_psync(stream: &mut TcpStream, replication: &mut bool) -> Option<Value> {
    let msg = Value::String(format!("FULLRESYNC {REPL_ID} {REPL_OFFSET}"));
    stream.write_all(&msg.encode()).await.unwrap();

    let rdb = BASE64_STANDARD.decode(EMPTY_RDB_B64).unwrap();
    stream
        .write(format!("${}\r\n", rdb.len()).as_bytes())
        .await
        .unwrap();
    stream.write_all(&rdb).await.unwrap();
    stream.flush().await.unwrap();
    *replication = true;
    None
}
