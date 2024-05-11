use anyhow::Result;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
#[derive(Clone)]
pub struct Store {
    storage: Arc<Mutex<HashMap<String, String>>>,
}

impl Store {
    pub fn new() -> Self {
        Store {
            storage: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn read(&self, key: &String) -> Result<String> {
        let storage = self.storage.lock().unwrap();
        match storage.get(key) {
            None => Err(anyhow::anyhow!("Key not found")),
            Some(value) => Ok(value.clone()),
        }
    }

    pub fn write(&self, key: String, value: String) -> Result<()> {
        let mut storage = self.storage.lock().unwrap();
        storage.insert(key, value);
        Ok(())
    }
}
