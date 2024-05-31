mod db;
mod follower;
mod server;
mod util;

use anyhow::Result;
use clap::Parser;
use resp::Value;
use std::sync::Arc;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{
        broadcast::{self, Sender},
        Mutex,
    },
};

use db::Store;
use follower::Follower;
use server::Server;
use util::{ReplicaType, ReplicationState};

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
    let (tx, _rx): (Sender<Value>, _) = broadcast::channel(16);
    let tx = Arc::new(tx);

    let role = if let Some((host, leader_port)) = parse_replica(&cmd_args) {
        let leader_stream = TcpStream::connect(format!("{host}:{leader_port}")).await?;
        let mut follower = Follower::new(listening_port, store.clone());
        tokio::spawn(async move { follower.handle_conn(leader_stream).await.unwrap() });
        ReplicaType::Follower
    } else {
        ReplicaType::Leader
    };

    let rep_state = Arc::new(Mutex::new(ReplicationState::new()));
    let listener = TcpListener::bind(format!("127.0.0.1:{listening_port}")).await?;
    loop {
        let (stream, _addr) = listener.accept().await?;
        println!("Accepted new connection");

        let store = store.clone();
        let rep_state = rep_state.clone();
        let tx = tx.clone();
        tokio::spawn(async move {
            let mut server = Server::new(store, role, rep_state);
            server.handle_conn(stream, tx).await.unwrap()
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
