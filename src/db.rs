use anyhow::Result;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::SystemTime,
};

struct RedisData {
    value: String,
    expiry: Option<SystemTime>,
}

#[derive(Clone)]
pub struct Store {
    storage: Arc<Mutex<HashMap<String, RedisData>>>,
}

impl Store {
    pub fn new() -> Self {
        Store {
            storage: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn read(&self, key: &String) -> Result<String> {
        let mut storage = self.storage.lock().unwrap();
        match storage.get(key) {
            None => Err(anyhow::anyhow!("Key not found")),
            Some(data) => {
                if let Some(expiry) = data.expiry {
                    if expiry < SystemTime::now() {
                        storage.remove(key);
                        return Err(anyhow::anyhow!("Key expired"));
                    }
                }
                Ok(data.value.clone())
            }
        }
    }

    pub fn write(&self, key: String, value: String, expiry: Option<SystemTime>) -> Result<()> {
        let mut storage = self.storage.lock().unwrap();
        storage.insert(key, RedisData { value, expiry });
        Ok(())
    }
}
