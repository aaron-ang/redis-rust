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
static REPL_ID: &str = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
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
    let listening_port = cmd_args.port;
    let role = if let Some((host, master_port)) = parse_replica(&cmd_args) {
        let stream = TcpStream::connect(format!("{host}:{master_port}"))?;
        thread::spawn(move || handshake(stream, listening_port));
        "slave"
    } else {
        "master"
    };

    let listener = TcpListener::bind(format!("127.0.0.1:{listening_port}"))?;
    let store = Store::new();

    for stream in listener.incoming() {
        let store_clone = store.clone();
        match stream {
            Ok(stream) => {
                info!("Accepted new connection");
                thread::spawn(move || {
                    handle_conn(stream, store_clone, role);
                });
            }
            Err(e) => {
                error!("Error: {e}");
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

fn handshake(mut stream: TcpStream, port: u16) -> Result<()> {
    send_ping(&mut stream)?;
    check_ping_response(&mut stream)?;

    send_replconf_port(&mut stream, port)?;
    send_replconf_capa(&mut stream)?;
    check_replconf_response(&mut stream)?;

    send_psync(&mut stream)?;

    Ok(())
}

fn send_ping(stream: &mut TcpStream) -> Result<()> {
    let ping = Value::Array(vec![Value::Bulk("PING".into())]);
    stream.write_all(&ping.encode())?;
    Ok(())
}

fn check_ping_response(stream: &mut TcpStream) -> Result<()> {
    stream.flush()?;
    let response = Decoder::new(BufReader::new(stream)).decode()?;
    if let Value::String(res_str) = response {
        if res_str != "PONG" {
            return Err(anyhow::anyhow!("Unexpected response to PING: {res_str}"));
        }
    } else {
        return Err(anyhow::anyhow!("Unexpected response to PING"));
    }
    Ok(())
}

fn send_replconf_port(stream: &mut TcpStream, port: u16) -> Result<()> {
    let replconf_port = Value::Array(vec![
        Value::Bulk("REPLCONF".into()),
        Value::Bulk("listening-port".into()),
        Value::Bulk(port.to_string()),
    ]);
    stream.write_all(&replconf_port.encode())?;
    Ok(())
}

fn send_replconf_capa(stream: &mut TcpStream) -> Result<()> {
    let replconf_capa = Value::Array(vec![
        Value::Bulk("REPLCONF".into()),
        Value::Bulk("capa".into()),
        Value::Bulk("eof".into()),
    ]);
    stream.write_all(&replconf_capa.encode())?;
    Ok(())
}

fn check_replconf_response(stream: &mut TcpStream) -> Result<()> {
    stream.flush()?;
    // check that 2 OK's are received
    for _ in 0..2 {
        let response = Decoder::new(BufReader::new(&mut *stream)).decode()?;
        match response {
            Value::String(res_str) if res_str == "OK" => continue,
            _ => return Err(anyhow::anyhow!("Unexpected response to REPLCONF")),
        }
    }
    Ok(())
}

fn send_psync(stream: &mut TcpStream) -> Result<()> {
    let psync = Value::Array(vec![
        Value::Bulk("PSYNC".into()),
        Value::Bulk("?".into()),
        Value::Bulk("-1".to_string()),
    ]);
    stream.write_all(&psync.encode())?;
    Ok(())
}

fn handle_conn(mut stream: TcpStream, store: Store, role: &str) {
    loop {
        let bufreader = BufReader::new(&stream);
        let mut decoder = Decoder::new(bufreader);

        let result = match decoder.decode() {
            Ok(value) => {
                let (command, args) = extract_command(&value).unwrap();
                match command.to_lowercase().as_str() {
                    "ping" => Ok(Value::String("PONG".into())),
                    "echo" => Ok(args.first().unwrap().clone()),
                    "set" => handle_set(args, &store),
                    "get" => handle_get(args, &store),
                    "info" => handle_info(role),
                    "replconf" => Ok(Value::String("OK".into())),
                    "psync" => Ok(Value::String(format!("FULLRESYNC {REPL_ID} 0"))),
                    c => Err(anyhow::anyhow!("Unknown command: {c}")),
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
                error!("error: {e}");
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
        Ok(s.into())
    } else {
        Err(anyhow::anyhow!("Expected command to be bulk string"))
    }
}

fn handle_set(args: Vec<Value>, store: &Store) -> Result<Value> {
    if args.len() < 2 {
        return Ok(Value::Error(
            "wrong number of arguments for 'set' command".into(),
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
                        return Ok(Value::Error("argument is not a bulk string".into()));
                    }
                };
                expiry = Some(SystemTime::now() + Duration::from_millis(ms));
            }
            _ => {
                return Ok(Value::Error("option not supported".into()));
            }
        }
    }

    store.write(key, value, expiry).unwrap();
    Ok(Value::String("OK".into()))
}

fn handle_get(args: Vec<Value>, store: &Store) -> Result<Value> {
    if args.len() < 1 {
        return Ok(Value::Error(
            "wrong number of arguments for 'get' command".into(),
        ));
    }
    let key = unpack_bulk_string(&args[0]).unwrap();
    match store.read(&key) {
        Ok(value) => Ok(Value::Bulk(value)),
        Err(e) => {
            error!("error: {e}");
            Ok(Value::Null)
        }
    }
}

fn handle_info(role: &str) -> Result<Value> {
    let value =
        format!("role:{role}\r\nmaster_replid:{REPL_ID}\r\nmaster_repl_offset:{REPL_OFFSET}");
    Ok(Value::Bulk(value))
}
