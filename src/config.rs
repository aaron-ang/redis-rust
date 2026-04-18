use std::{path::PathBuf, sync::Arc};

use ahash::RandomState;
use dashmap::DashMap;

use crate::data::Store;
use crate::network::{PubSub, ReplicaType, ReplicationHub};

const DEFAULT_DBFILE: &str = "dump.rdb";
const DEFAULT_APPENDDIRNAME: &str = "appendonlydir";
const DEFAULT_APPENDFILENAME: &str = "appendonly.aof";
const DEFAULT_APPENDFSYNC: &str = "everysec";

/// Flag names by index; index i uses bit (1 << i). Index 0 reserved (empty).
const FLAG_NAMES: &[&str] = &["", "nopass"];

#[derive(Clone, Copy, Default)]
pub struct AclFlags(u8);

impl AclFlags {
    pub fn nopass() -> Self {
        let i = FLAG_NAMES.iter().position(|&n| n == "nopass").unwrap();
        AclFlags(1 << i)
    }

    #[must_use]
    pub fn contains(self, name: &str) -> bool {
        FLAG_NAMES
            .iter()
            .position(|&n| n == name)
            .is_some_and(|i| self.0 & (1 << i) != 0)
    }

    pub fn set(&mut self, name: &str, on: bool) {
        if let Some(i) = FLAG_NAMES.iter().position(|&n| n == name) {
            let bit = 1 << i;
            if on {
                self.0 |= bit;
            } else {
                self.0 &= !bit;
            }
        }
    }

    pub fn names(self) -> impl Iterator<Item = &'static str> {
        let bits = self.0;
        (1..FLAG_NAMES.len())
            .filter(move |&i| bits & (1 << i) != 0)
            .map(|i| FLAG_NAMES[i])
    }
}

#[derive(Clone, Default)]
pub struct AclUser {
    pub flags: AclFlags,
    pub passwords: Vec<String>,
}

#[derive(Clone)]
pub struct Config {
    pub port: u16,
    pub dir: PathBuf,
    pub dbfilename: String,
    pub appendonly: bool,
    pub appenddirname: String,
    pub appendfilename: String,
    pub appendfsync: String,
    pub role: ReplicaType,
    pub replicaof: Option<String>,
    pub store: Arc<Store>,
    pub replication: Arc<ReplicationHub>,
    pub pubsub: Arc<PubSub>,
    pub acl_users: Arc<DashMap<String, AclUser, RandomState>>,
}

impl Config {
    #[must_use]
    pub fn new(
        port: u16,
        dir: Option<PathBuf>,
        dbfilename: Option<String>,
        appendonly: Option<bool>,
        appenddirname: Option<String>,
        appendfilename: Option<String>,
        appendfsync: Option<String>,
        store: Arc<Store>,
        role: ReplicaType,
        replicaof: Option<String>,
    ) -> Self {
        let acl_users = Arc::new(DashMap::with_hasher(RandomState::default()));
        acl_users.insert(
            "default".to_string(),
            AclUser {
                flags: AclFlags::nopass(),
                passwords: vec![],
            },
        );
        Config {
            port,
            dir: dir
                .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))),
            dbfilename: dbfilename.unwrap_or_else(|| DEFAULT_DBFILE.to_string()),
            appendonly: appendonly.unwrap_or(false),
            appenddirname: appenddirname.unwrap_or_else(|| DEFAULT_APPENDDIRNAME.to_string()),
            appendfilename: appendfilename.unwrap_or_else(|| DEFAULT_APPENDFILENAME.to_string()),
            appendfsync: appendfsync.unwrap_or_else(|| DEFAULT_APPENDFSYNC.to_string()),
            store,
            role,
            replicaof,
            replication: Arc::new(ReplicationHub::default()),
            pubsub: Arc::new(PubSub::default()),
            acl_users,
        }
    }
}
