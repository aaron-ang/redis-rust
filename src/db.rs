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
        if let (Some(dir), Some(filename)) = (dir, dbfilename) {
            let path = dir.join(filename);
            if path.exists() {
                let file = fs::File::open(&path).await?;
                let instance = Instance::new(file).await?;
                return instance.get_db(0).map(|db| db.clone());
            }
        }
        Self::empty()
    }

    fn empty() -> Result<Self> {
        Ok(Store::new_with_entries(HashMap::new()))
    }

    pub fn new_with_entries(entries: HashMap<String, RedisData>) -> Self {
        Store {
            entries: RwLock::new(entries),
        }
    }

    pub fn get(&self, key: &str) -> Option<String> {
        let storage = self.entries.read().unwrap();
        let data = storage.get(key)?;
        if data.is_expired() {
            drop(storage);
            self.entries.write().unwrap().remove(key);
            None
        } else {
            Some(data.value.clone())
        }
    }

    pub fn set(&self, key: String, value: String, expiry: Option<SystemTime>) {
        self.entries
            .write()
            .unwrap()
            .insert(key, RedisData::new(value, expiry));
    }

    pub fn keys(&self, pattern: &str) -> Result<Vec<String>> {
        let pattern = Pattern::new(pattern)?;
        let mut expired = Vec::new();
        let mut matched = Vec::new();

        {
            let storage = self.entries.read().unwrap();
            for (key, data) in storage.iter() {
                if data.is_expired() {
                    expired.push(key.clone());
                } else if pattern.matches(key) {
                    matched.push(key.clone());
                }
            }
        }

        let mut storage = self.entries.write().unwrap();
        for key in expired {
            storage.remove(&key);
        }

        Ok(matched)
    }
}
