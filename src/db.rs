use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
    time::SystemTime,
};

struct RedisData {
    value: String,
    expiry: Option<SystemTime>,
}

#[derive(Clone)]
pub struct Store {
    storage: Arc<RwLock<HashMap<String, RedisData>>>,
}

impl Store {
    pub fn new() -> Self {
        Store {
            storage: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn get(&self, key: &str) -> Option<String> {
        let storage_read = self.storage.read().unwrap();
        if let Some(data) = storage_read.get(key) {
            if let Some(expiry) = data.expiry {
                if expiry < SystemTime::now() {
                    drop(storage_read); // drop the read lock before acquiring the write lock
                    let mut storage_write = self.storage.write().unwrap();
                    storage_write.remove(key);
                    return None;
                }
            }
            return Some(data.value.clone());
        }
        None
    }

    pub fn set(&self, key: String, value: String, expiry: Option<SystemTime>) {
        let mut storage = self.storage.write().unwrap();
        storage.insert(key, RedisData { value, expiry });
    }
}
