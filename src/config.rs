use resp::Value;
use std::{path::PathBuf, sync::Arc};
use tokio::sync::broadcast::{self, Sender};

use crate::util::{ReplicaType, ReplicationState};
use crate::Store;

#[derive(Clone)]
pub struct Config {
    pub port: u16,
    pub dir: PathBuf,
    pub dbfilename: String,
    pub role: ReplicaType,
    pub store: Arc<Store>,
    pub tx: Arc<Sender<Value>>,
    pub rep_state: Arc<ReplicationState>,
}

impl Config {
    pub fn new(
        port: u16,
        dir: Option<PathBuf>,
        dbfilename: Option<String>,
        store: Arc<Store>,
        role: ReplicaType,
    ) -> Self {
        let (tx, _rx): (Sender<Value>, _) = broadcast::channel(16);
        let tx = Arc::new(tx);
        let rep_state = Arc::new(ReplicationState::new());
        Self {
            port,
            dir: dir.unwrap_or_else(|| PathBuf::from(".")),
            dbfilename: dbfilename.unwrap_or_else(|| "dump.rdb".to_string()),
            store,
            role,
            tx,
            rep_state,
        }
    }
}
