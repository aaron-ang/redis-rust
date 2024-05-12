use anyhow::Result;
use clap::Parser;
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

#[derive(Parser, Debug, Clone)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value_t = PORT)]
    port: u16,
    #[arg(short, long, num_args = 2)]
    replicaof: Option<Vec<String>>,
}

fn main() {
    let cmd_args = Args::parse();
    let port = cmd_args.port;
    let listener = TcpListener::bind(format!("127.0.0.1:{port}")).unwrap();
    let store = Store::new();

    for stream in listener.incoming() {
        let store_clone = store.clone();
        let cmd_args_clone = cmd_args.clone();
        match stream {
            Ok(stream) => {
                println!("accepted new connection");
                thread::spawn(move || {
                    handle_client(stream, store_clone, cmd_args_clone);
                });
            }
            Err(e) => {
                println!("error: {e}");
            }
        }
    }
}

fn handle_client(mut stream: TcpStream, store: Store, cmd_args: Args) {
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
                    "info" => handle_info(&cmd_args),
                    c => Err(anyhow::anyhow!("Unknown command: {c}")),
                }
            }
            Err(e) => {
                if e.kind() == ErrorKind::UnexpectedEof {
                    println!("client disconnected");
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
                println!("error: {e}");
            }
        }
    }
}

fn extract_command(value: &Value) -> Result<(String, Vec<Value>)> {
    match value {
        Value::Array(a) => {
            let command = unpack_bulk_string(a.first().unwrap())?;
            let args = a.iter().skip(1).cloned().collect();
            Ok((command, args))
        }
        _ => Err(anyhow::anyhow!("Unexpected command format")),
    }
}

fn unpack_bulk_string(value: &Value) -> Result<String> {
    match value {
        Value::Bulk(s) => Ok(s.to_string()),
        _ => Err(anyhow::anyhow!("Expected command to be bulk string")),
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

    if let Some(v) = iter.next() {
        match v {
            Value::Bulk(option) => match option.to_lowercase().as_str() {
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
            },
            _ => {
                return Ok(Value::Error("option is not a bulk string".to_string()));
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
            println!("error: {e}");
            Ok(Value::Null)
        }
    }
}

fn handle_info(cmd_args: &Args) -> Result<Value> {
    let role = match cmd_args.replicaof {
        Some(_) => "slave",
        None => "master",
    };
    Ok(Value::Bulk(format!("role:{role}")))
}
