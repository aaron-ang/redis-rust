use anyhow::Result;
use clap::Parser;
use log::{error, info};
use resp::{Decoder, Value};
use std::{
    io::{BufReader, ErrorKind, Write},
    net::{TcpListener, TcpStream},
    thread,
    time::{Duration, SystemTime},
};

mod db;
use db::Store;

static PORT: u16 = 6379;
static REP_ID: &str = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
static REPL_OFFSET: usize = 0;

#[derive(Parser, Debug, Clone)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value_t = PORT)]
    port: u16,
    #[arg(short, long)]
    replicaof: Option<String>,
}

fn main() -> Result<()> {
    let cmd_args = Args::parse();
    let port = cmd_args.port;
    let role = if let Some((host, port)) = parse_replica(&cmd_args) {
        let mut stream = TcpStream::connect(format!("{}:{}", host, port))?;
        let reply = Value::Array(vec![Value::Bulk("PING".to_string())]);
        stream.write_all(&reply.encode())?;
        "slave"
    } else {
        "master"
    };

    let listener = TcpListener::bind(format!("127.0.0.1:{port}"))?;
    let store = Store::new();

    for stream in listener.incoming() {
        let store_clone = store.clone();
        match stream {
            Ok(stream) => {
                info!("Accepted new connection");
                thread::spawn(move || {
                    handle_client(stream, store_clone, role);
                });
            }
            Err(e) => {
                error!("Error: {}", e);
            }
        }
    }

    Ok(())
}

fn parse_replica(cmd_args: &Args) -> Option<(String, u16)> {
    if let Some(replicaof) = &cmd_args.replicaof {
        let parts: Vec<&str> = replicaof.split_ascii_whitespace().collect();
        if parts.len() == 2 {
            let host = parts[0].to_string();
            let port = parts[1].parse::<u16>().unwrap();
            return Some((host, port));
        }
    }
    None
}

fn handle_client(mut stream: TcpStream, store: Store, role: &str) {
    loop {
        let bufreader = BufReader::new(&stream);
        let mut decoder = Decoder::new(bufreader);

        let result = match decoder.decode() {
            Ok(value) => {
                let (command, args) = extract_command(&value).unwrap();
                match command.to_lowercase().as_str() {
                    "ping" => Ok(Value::String("PONG".to_string())),
                    "echo" => Ok(args.first().unwrap().clone()),
                    "set" => handle_set(args, &store),
                    "get" => handle_get(args, &store),
                    "info" => {
                        let value = format!(
                            "role:{}\r\nmaster_replid:{}\r\nmaster_repl_offset:{}",
                            role, REP_ID, REPL_OFFSET
                        );
                        Ok(Value::Bulk(value))
                    }
                    c => Err(anyhow::anyhow!("Unknown command: {}", c)),
                }
            }
            Err(e) => {
                if e.kind() == ErrorKind::UnexpectedEof {
                    error!("client disconnected");
                    return;
                }
                Err(e.into())
            }
        };

        match result {
            Ok(value) => {
                stream.write_all(&value.encode()).unwrap();
            }
            Err(e) => {
                error!("error: {}", e);
            }
        }
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

fn unpack_bulk_string(value: &Value) -> Result<String> {
    if let Value::Bulk(s) = value {
        Ok(s.to_string())
    } else {
        Err(anyhow::anyhow!("Expected command to be bulk string"))
    }
}

fn handle_set(args: Vec<Value>, store: &Store) -> Result<Value> {
    if args.len() < 2 {
        return Ok(Value::Error(
            "wrong number of arguments for 'set' command".to_string(),
        ));
    }
    let mut iter = args.into_iter();
    let key = unpack_bulk_string(&iter.next().unwrap()).unwrap();
    let value = unpack_bulk_string(&iter.next().unwrap()).unwrap();
    let mut expiry: Option<SystemTime> = None;

    if let Some(Value::Bulk(option)) = iter.next() {
        match option.to_lowercase().as_str() {
            "px" => {
                let ms = match iter.next() {
                    Some(Value::Bulk(arg)) => arg.parse::<u64>().unwrap(),
                    _ => {
                        return Ok(Value::Error("argument is not a bulk string".to_string()));
                    }
                };
                expiry = Some(SystemTime::now() + Duration::from_millis(ms));
            }
            _ => {
                return Ok(Value::Error("option not supported".to_string()));
            }
        }
    }

    store.write(key, value, expiry).unwrap();
    Ok(Value::String("OK".to_string()))
}

fn handle_get(args: Vec<Value>, store: &Store) -> Result<Value> {
    if args.len() < 1 {
        return Ok(Value::Error(
            "wrong number of arguments for 'get' command".to_string(),
        ));
    }
    let key = unpack_bulk_string(&args[0]).unwrap();
    match store.read(&key) {
        Ok(value) => Ok(Value::Bulk(value)),
        Err(e) => {
            error!("error: {}", e);
            Ok(Value::Null)
        }
    }
}
