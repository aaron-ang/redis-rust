use std::{
    io::{Read, Write},
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
    let mut buf = [0; 512];
    loop {
        match stream.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => {
                let command = String::from_utf8(buf[0..n].to_vec());
                if command.is_ok() {
                    let command = command.unwrap();
                    if command.contains("PING") {
                        println!("responding with PONG\r\n");
                        stream.write_all(b"+PONG\r\n").unwrap();
                    }
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
        buf = [0; 512];
    }
}
