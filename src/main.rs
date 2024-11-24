use anyhow::Result;
use clap::Parser;
use std::{
    net::{Ipv4Addr, SocketAddrV4},
    path::PathBuf,
};
use tokio::net::{TcpListener, TcpStream};

use redis_starter_rust::*;

const PORT: u16 = 6379;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(long, default_value_t = PORT)]
    port: u16,

    #[arg(long)]
    replicaof: Option<String>,

    #[arg(long)]
    dir: Option<PathBuf>,

    #[arg(long)]
    dbfilename: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = config_init(Args::parse()).await?;
    let addr = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), config.port);
    let listener = TcpListener::bind(addr).await?;

    loop {
        let (stream, addr) = listener.accept().await?;
        let config = config.clone();
        println!("Accepted new connection from {addr}");
        tokio::spawn(async move {
            let mut server = Server::new(config, stream);
            if let Err(e) = server.handle_conn().await {
                eprintln!("Error handling connection: {:?}", e);
            }
        });
    }
}

fn parse_replica(cmd_args: &Args) -> Option<(String, u16)> {
    cmd_args.replicaof.as_ref().and_then(|replicaof| {
        let mut parts = replicaof.split_whitespace();
        match (parts.next(), parts.next()) {
            (Some(host), Some(port_str)) => port_str
                .parse::<u16>()
                .ok()
                .map(|port| (host.to_string(), port)),
            _ => None,
        }
    })
}

async fn config_init(cmd_args: Args) -> Result<Config> {
    let store = Store::new();
    let role = if let Some((host, leader_port)) = parse_replica(&cmd_args) {
        let leader_stream = TcpStream::connect(format!("{host}:{leader_port}")).await?;
        let mut follower = Follower::new(cmd_args.port, store.clone(), leader_stream);
        tokio::spawn(async move {
            if let Err(e) = follower.handle_conn().await {
                eprintln!("Error handling connection from leader: {:?}", e);
            }
        });
        ReplicaType::Follower
    } else {
        ReplicaType::Leader
    };

    Ok(Config::new(
        cmd_args.port,
        cmd_args.dir,
        cmd_args.dbfilename,
        store,
        role,
    ))
}
