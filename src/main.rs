use std::{
    net::{Ipv4Addr, SocketAddrV4},
    path::PathBuf,
    sync::Arc,
};

use anyhow::{bail, Result};
use clap::Parser;
use tokio::net::{TcpListener, TcpStream};

use codecrafters_redis::*;

const PORT: u16 = 6379;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(long, default_value_t = PORT)]
    port: u16, // which tcp port to bind to (default 6379)

    #[arg(long)]
    replicaof: Option<String>, // if set this node acts as a replica of another

    #[arg(long)]
    dir: Option<PathBuf>, // directory for database persistence

    #[arg(long)]
    dbfilename: Option<String>, // the name of the database file
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = setup_config().await?; // reads the args > builds the store > determines the
    // role( leader, or follower)


    // starting the tcp server
    let addr = SocketAddrV4::new(Ipv4Addr::LOCALHOST, config.port);
    let listener = TcpListener::bind(addr).await?;

    println!("Server listening on {addr}");
    // this likely starts some bg task to sync data from the leader
    if let ReplicaType::Follower = config.role {
        if let Some(replicaof) = &config.replicaof {
            spawn_follower_connection(replicaof, config.clone()).await?;
        }
    }

    loop {
        let (stream, addr) = listener.accept().await?;
        let config = config.clone();
        println!("Accepted new connection from {addr}");

        tokio::spawn(async move {
            if let Err(e) = Server::new(config, stream).handle_conn().await {
                eprintln!("Error handling connection: {e:?}");
            }
        });
    }
}

async fn setup_config() -> Result<Config> {
    let args = Args::parse();
    let store = Arc::new(Store::from_path(&args.dir, &args.dbfilename)?);
    let role = determine_role(&args);

    Ok(Config::new(
        args.port,
        args.dir,
        args.dbfilename,
        store,
        role,
        args.replicaof,
    ))
}

fn determine_role(args: &Args) -> ReplicaType {
    if args.replicaof.is_some() {
        ReplicaType::Follower
    } else {
        ReplicaType::Leader
    }
}

async fn spawn_follower_connection(replicaof: &str, config: Config) -> Result<()> {
    let (host, leader_port) = parse_replica_string(replicaof)?;
    let leader_stream = TcpStream::connect(format!("{host}:{leader_port}")).await?;
    let mut follower = Follower::new(config.port, config.store, leader_stream);

    tokio::spawn(async move {
        if let Err(e) = follower.handle_conn().await {
            eprintln!("Error handling connection from leader: {e:?}");
        }
    });

    Ok(())
}

fn parse_replica_string(replicaof: &str) -> Result<(String, u16)> {
    let mut parts = replicaof.split_whitespace();
    match (parts.next(), parts.next()) {
        (Some(host), Some(port_str)) => port_str
            .parse::<u16>()
            .map_err(|e| anyhow::anyhow!("Invalid port. {e}"))
            .map(|port|(host.to_string(), port)),
            bail!("Invalid replicaof format"),
}
}
