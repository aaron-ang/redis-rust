use anyhow::Result;
use resp::{Decoder, Value};
use std::{
    io::{BufReader, ErrorKind, Write},
    net::{TcpListener, TcpStream},
    thread,
};

static PORT: u16 = 6379;

fn main() {
    let listener = TcpListener::bind(format!("127.0.0.1:{PORT}")).unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("accepted new connection");
                thread::spawn(move || {
                    handle_client(stream);
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn handle_client(mut stream: TcpStream) {
    loop {
        let bufreader = BufReader::new(&stream);
        let mut decoder = Decoder::new(bufreader);
        let mut result: Option<Value> = None;

        match decoder.decode() {
            Ok(value) => {
                let (command, args) = extract_command(value).unwrap();
                result = match command.to_lowercase().as_str() {
                    "ping" => Some(Value::String("PONG".to_string())),
                    "echo" => Some(args.first().unwrap().clone()),
                    c => panic!("Cannot handle command {}", c),
                }
            }
            Err(e) => {
                if e.kind() == ErrorKind::UnexpectedEof {
                    println!("client disconnected");
                    return;
                }
                eprintln!("error: {}", e);
            }
        }

        if let Some(r) = result {
            stream.write_all(&r.encode()).unwrap();
        }
    }
}

fn extract_command(value: Value) -> Result<(String, Vec<Value>)> {
    match value {
        Value::Array(a) => {
            let command = unpack_bulk_string(a.first().unwrap().clone())?;
            let args = a.into_iter().skip(1).collect();
            Ok((command, args))
        }
        _ => Err(anyhow::anyhow!("Unexpected command format")),
    }
}

fn unpack_bulk_string(value: Value) -> Result<String> {
    match value {
        Value::Bulk(s) => Ok(s),
        _ => Err(anyhow::anyhow!("Expected command to be bulk string")),
    }
}
