use anyhow::Result;
use glob::Pattern;
use std::{collections::HashMap, path::PathBuf, sync::RwLock, time::SystemTime};
use tokio::fs;

use crate::util::Instance;

#[derive(Clone)]
pub struct RedisData {
    value: String,
    expiry: Option<SystemTime>,
}

impl RedisData {
    pub fn new(value: String, expiry: Option<SystemTime>) -> Self {
        RedisData { value, expiry }
    }

    fn is_expired(&self) -> bool {
        if let Some(expiry) = self.expiry {
            expiry < SystemTime::now()
        } else {
            false
        }
    }
}

pub struct Store {
    entries: RwLock<HashMap<String, RedisData>>,
}

impl Clone for Store {
    fn clone(&self) -> Self {
        let storage = self.entries.read().unwrap();
        Store {
            entries: RwLock::new(storage.clone()),
        }
    }
}

impl Store {
    pub async fn from_path(dir: &Option<PathBuf>, dbfilename: &Option<String>) -> Result<Self> {
        match (dir, dbfilename) {
            (Some(dir), Some(filename)) => {
                let path = dir.join(filename);
                if !path.exists() {
                    return Ok(Self::new_with_entries(HashMap::new()));
                }

                let file = fs::File::open(&path).await?;
                let instance = Instance::new(file).await?;
                // get first database
                let db = instance
                    .dbs
                    .values()
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("No database found"))?;

                Ok(db.clone())
            }
            _ => Ok(Self::new_with_entries(HashMap::new())),
        }
    }

    pub fn new_with_entries(entries: HashMap<String, RedisData>) -> Self {
        Store {
            entries: RwLock::new(entries),
        }
    }

    pub fn get(&self, key: &str) -> Option<String> {
        let storage = self.entries.read().unwrap();
        if let Some(data) = storage.get(key) {
            if data.is_expired() {
                drop(storage); // release read lock before acquiring write lock
                self.entries.write().unwrap().remove(key);
                return None;
            }
            return Some(data.value.clone());
        }
        None
    }

    pub fn set(&self, key: String, value: String, expiry: Option<SystemTime>) {
        let mut storage = self.entries.write().unwrap();
        storage.insert(key, RedisData { value, expiry });
    }

    pub fn keys(&self, pattern: &str) -> Result<Vec<String>> {
        let pattern = Pattern::new(pattern)?;
        let mut expired = Vec::new();
        let mut keys = Vec::new();

        {
            let storage = self.entries.read().unwrap();
            for (key, data) in storage.iter() {
                if data.is_expired() {
                    expired.push(key.clone());
                } else if pattern.matches(key) {
                    keys.push(key.clone());
                }
            }
        }

        let mut storage = self.entries.write().unwrap();
        for key in expired {
            storage.remove(&key);
        }

        Ok(keys)
    }
}
