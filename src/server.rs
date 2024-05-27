use crate::db::Store;
use crate::ReplicaType;

use anyhow::Result;
use base64::prelude::*;
use resp::{Decoder, Value};
use std::{io::BufReader, sync::Arc, time::SystemTime};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::broadcast::Sender,
    time::{sleep, Duration},
};

const REPL_ID: &str = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
const REPL_OFFSET: usize = 0;
const EMPTY_RDB_B64: &str = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";

pub struct Server {
    store: Store,
    role: ReplicaType,
}

impl Server {
    pub fn new(store: Store, role: ReplicaType) -> Self {
        Self { store, role }
    }

    pub async fn handle_conn(&self, mut stream: TcpStream, tx: Arc<Sender<Value>>) -> Result<()> {
        let mut replication = false;

        let tx = tx.clone();

        loop {
            sleep(Duration::from_millis(10)).await;

            let mut buf = [0; 1024];
            let bytes_read = stream.read(&mut buf).await?;
            if bytes_read == 0 {
                eprintln!("Client disconnected");
                break;
            }
            let bufreader = BufReader::new(&buf[..bytes_read]);
            let mut decoder = Decoder::new(bufreader);

            let response = match decoder.decode() {
                Ok(value) => {
                    let (command, args) = extract_command(&value).unwrap();
                    match command.to_lowercase().as_str() {
                        "ping" => Some(Value::String("PONG".into())),
                        "echo" => Some(args.first().unwrap().clone()),
                        "set" => handle_set(args, &self.store).map(|v| {
                            tx.send(value.clone()).unwrap();
                            println!("Sent to followers: {:?}", value);
                            v
                        }),
                        "get" => handle_get(args, &self.store),
                        "info" => handle_info(self.role),
                        "replconf" => Some(Value::String("OK".into())),
                        "psync" => handle_psync(&mut stream, &mut replication).await,
                        c => {
                            eprintln!("Unknown command: {}", c);
                            None
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Error decoding value: {:?}", e);
                    None
                }
            };

            if let Some(r) = response {
                stream.write_all(&r.encode()).await?;
            }

            if replication {
                let mut rx = tx.subscribe();
                while let Ok(f) = rx.recv().await {
                    stream.write_all(&f.encode()).await?;
                }
            }
        }
        Ok(())
    }
}

fn unpack_bulk_string(value: &Value) -> Result<String> {
    if let Value::Bulk(s) = value {
        Ok(s.into())
    } else {
        Err(anyhow::anyhow!("Expected command to be bulk string"))
    }
}
fn extract_command(value: &Value) -> Result<(String, Vec<Value>)> {
    if let Value::Array(a) = value {
        let command = unpack_bulk_string(a.first().unwrap())?;
        let args = a.iter().skip(1).cloned().collect();
        Ok((command, args))
    } else {
        Err(anyhow::anyhow!("Unexpected command format"))
    }
}

fn handle_set(args: Vec<Value>, store: &Store) -> Option<Value> {
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

fn handle_get(args: Vec<Value>, store: &Store) -> Option<Value> {
    if args.len() < 1 {
        return None;
    }
    let key = unpack_bulk_string(&args[0]).unwrap();
    store
        .get(&key)
        .map(|v| Value::Bulk(v))
        .or_else(|| Some(Value::Null))
}

fn handle_info(role: ReplicaType) -> Option<Value> {
    let value =
        format!("role:{role}\r\nmaster_replid:{REPL_ID}\r\nmaster_repl_offset:{REPL_OFFSET}");
    Some(Value::Bulk(value))
}

async fn handle_psync(stream: &mut TcpStream, replication: &mut bool) -> Option<Value> {
    let msg = Value::String(format!("FULLRESYNC {REPL_ID} {REPL_OFFSET}"));
    stream.write_all(&msg.encode()).await.unwrap();

    let rdb = BASE64_STANDARD.decode(EMPTY_RDB_B64.as_bytes()).unwrap();
    stream
        .write(format!("${}\r\n", rdb.len()).as_bytes())
        .await
        .unwrap();
    stream.write_all(&rdb).await.unwrap();
    *replication = true;
    None
}
