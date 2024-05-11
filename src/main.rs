use anyhow::Result;
use resp::{Decoder, Value};
use std::{
    io::{BufReader, ErrorKind, Write},
    net::{TcpListener, TcpStream},
    thread,
};

mod db;
use db::Store;

static PORT: u16 = 6379;

fn main() {
    let listener = TcpListener::bind(format!("127.0.0.1:{PORT}")).unwrap();
    let store = Store::new();

    for stream in listener.incoming() {
        let store_clone = store.clone();
        match stream {
            Ok(stream) => {
                println!("accepted new connection");
                thread::spawn(move || {
                    handle_client(stream, store_clone);
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn handle_client(mut stream: TcpStream, store: Store) {
    loop {
        let bufreader = BufReader::new(&stream);
        let mut decoder = Decoder::new(bufreader);

        let result = match decoder.decode() {
            Ok(value) => {
                let (command, args) = extract_command(&value).unwrap();
                match command.to_lowercase().as_str() {
                    "ping" => Ok(Value::String("PONG".to_string())),
                    "echo" => Ok(args.first().unwrap().clone()),
                    "set" => Ok(handle_set(args, &store)),
                    "get" => Ok(handle_get(args, &store)),
                    c => Err(anyhow::anyhow!("Unknown command: {}", c)),
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
                println!("error: {}", e);
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

fn handle_set(args: Vec<Value>, store: &Store) -> Value {
    if args.len() < 2 {
        return Value::Error("wrong number of arguments for 'set' command".to_string());
    }
    let key = unpack_bulk_string(&args[0]).unwrap();
    let value = unpack_bulk_string(&args[1]).unwrap();
    store.write(key, value).unwrap();
    Value::String("OK".to_string())
}

fn handle_get(args: Vec<Value>, store: &Store) -> Value {
    if args.len() < 1 {
        return Value::Error("wrong number of arguments for 'get' command".to_string());
    }
    let key = unpack_bulk_string(&args[0]).unwrap();
    match store.read(&key) {
        Ok(value) => Value::Bulk(value),
        Err(_) => Value::Null,
    }
}
