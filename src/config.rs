use std::{path::PathBuf, sync::Arc};

use crate::replication::{ReplicaType, ReplicationHub};
use crate::{PubSub, Store};

const DEFAULT_DIR: &str = ".";
const DEFAULT_DBFILE: &str = "dump.rdb";

#[derive(Clone)]
pub struct Config {
    pub port: u16,
    pub dir: PathBuf,
    pub dbfilename: String,
    pub role: ReplicaType,
    pub replicaof: Option<String>,
    pub store: Arc<Store>,
    pub replication: Arc<ReplicationHub>,
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
        Config {
            port,
            dir: dir.unwrap_or_else(|| PathBuf::from(DEFAULT_DIR)),
            dbfilename: dbfilename.unwrap_or_else(|| DEFAULT_DBFILE.to_string()),
            store,
            role,
            replicaof,
            replication: Arc::new(ReplicationHub::default()),
            pubsub: Arc::new(PubSub::default()),
        }
    }
}
