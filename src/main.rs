use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
};

static PORT: u16 = 6379;

fn main() {
    let listener = TcpListener::bind(format!("127.0.0.1:{PORT}")).unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("accepted new connection");
                handle_connection(stream);
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn handle_connection(mut stream: TcpStream) {
    let mut buf = [0; 512];
    match stream.read(&mut buf) {
        Ok(_n) => {
            let command = String::from_utf8(buf[0.._n].to_vec());
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
}
