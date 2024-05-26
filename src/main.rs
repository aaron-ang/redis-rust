mod db;
mod follower;
mod server;
mod util;

use anyhow::Result;
use clap::Parser;
use tokio::net::{TcpListener, TcpStream};

use db::Store;
use follower::Follower;
use server::Server;
use util::Replica;

static PORT: u16 = 6379;

#[derive(Parser, Debug, Clone)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value_t = PORT)]
    port: u16,
    #[arg(short, long)]
    replicaof: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cmd_args = Args::parse();
    let listening_port = cmd_args.port;
    let store = Store::new();

    let role = if let Some((host, leader_port)) = parse_replica(&cmd_args) {
        let leader_stream = TcpStream::connect(format!("{host}:{leader_port}")).await?;
        let follower = Follower::new(listening_port, store.clone());
        let _ = tokio::spawn(async move { follower.handle_conn(leader_stream).await });
        Replica::Follower
    } else {
        Replica::Leader
    };

    let listener = TcpListener::bind(format!("127.0.0.1:{listening_port}")).await?;
    loop {
        let (stream, _addr) = listener.accept().await?;
        println!("Accepted new connection");

        let store = store.clone();
        tokio::spawn(async move {
            let server = Server::new(store, role);
            server.handle_conn(stream).await;
        });
    }
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
