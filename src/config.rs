use resp::Value;
use std::{path::PathBuf, sync::Arc};
use tokio::sync::broadcast;

use crate::util::{ReplicaType, ReplicationState};
use crate::{PubSub, Store};

const DEFAULT_DIR: &str = ".";
const DEFAULT_DBFILE: &str = "dump.rdb";
const BROADCAST_CHANNEL_SIZE: usize = 16;

#[derive(Clone)]
pub struct Config {
    pub port: u16,
    pub dir: PathBuf,
    pub dbfilename: String,
    pub role: ReplicaType,
    pub replicaof: Option<String>,
    pub store: Arc<Store>,
    pub replicas: Arc<broadcast::Sender<Value>>,
    pub rep_state: Arc<ReplicationState>,
    pub pubsub: Arc<PubSub>,
}

impl Config {
    pub fn new(
        port: u16,
        dir: Option<PathBuf>,
        dbfilename: Option<String>,
        store: Arc<Store>,
        role: ReplicaType,
        replicaof: Option<String>,
    ) -> Self {
        let (tx, _rx) = broadcast::channel(BROADCAST_CHANNEL_SIZE);

        Config {
            port,
            dir: dir.unwrap_or_else(|| PathBuf::from(DEFAULT_DIR)),
            dbfilename: dbfilename.unwrap_or_else(|| DEFAULT_DBFILE.to_string()),
            store,
            role,
            replicaof,
            replicas: Arc::new(tx),
            rep_state: Arc::new(ReplicationState::new()),
            pubsub: Arc::new(PubSub::default()),
        }
    }
}
