use anyhow::Result;
use clap::Parser;
use std::{
    net::{Ipv4Addr, SocketAddrV4},
    path::PathBuf,
    sync::Arc,
};
use tokio::net::{TcpListener, TcpStream};

use redis_starter_rust::*;

const PORT: u16 = 6379;
const LOCALHOST: Ipv4Addr = Ipv4Addr::new(127, 0, 0, 1);

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
    let config = setup_config().await?;
    start_server(config).await
}

async fn setup_config() -> Result<Config> {
    let args = Args::parse();
    let store = Arc::new(Store::from_path(&args.dir, &args.dbfilename)?);
    let role = setup_replication(&args, store.clone()).await?;

    Ok(Config::new(
        args.port,
        args.dir,
        args.dbfilename,
        store,
        role,
    ))
}

async fn setup_replication(args: &Args, store: Arc<Store>) -> Result<ReplicaType> {
    match parse_replica(args) {
        Some((host, leader_port)) => {
            spawn_follower(&host, leader_port, args.port, store).await?;
            Ok(ReplicaType::Follower)
        }
        None => Ok(ReplicaType::Leader),
    }
}

async fn spawn_follower(
    host: &str,
    leader_port: u16,
    follower_port: u16,
    store: Arc<Store>,
) -> Result<()> {
    let leader_stream = TcpStream::connect(format!("{host}:{leader_port}")).await?;
    let mut follower = Follower::new(follower_port, store, leader_stream);

    tokio::spawn(async move {
        if let Err(e) = follower.handle_conn().await {
            eprintln!("Error handling connection from leader: {:?}", e);
        }
    });

    Ok(())
}

async fn start_server(config: Config) -> Result<()> {
    let addr = SocketAddrV4::new(LOCALHOST, config.port);
    let listener = TcpListener::bind(addr).await?;

    println!("Server listening on {addr}");

    loop {
        let (stream, addr) = listener.accept().await?;
        let config = config.clone();
        println!("Accepted new connection from {addr}");

        tokio::spawn(async move {
            if let Err(e) = Server::new(config, stream).handle_conn().await {
                eprintln!("Error handling connection: {:?}", e);
            }
        });
    }
}

fn parse_replica(args: &Args) -> Option<(String, u16)> {
    args.replicaof.as_ref().and_then(|replicaof| {
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
