use anyhow::{bail, Result};
use glob::Pattern;
use std::{collections::HashMap, fs, path::PathBuf, sync::Arc, time::SystemTime};
use tokio::sync::RwLock;

use crate::stream::{StreamEntryId, StreamRecord};
use crate::util::{Instance, StringRecord};

#[derive(Clone)]
pub enum RecordType {
    String(StringRecord),
    Stream(StreamRecord),
}

#[derive(Clone)]
pub struct RedisData {
    record: RecordType,
    expiry: Option<SystemTime>,
}

impl RedisData {
    pub fn new(record: RecordType, expiry: Option<SystemTime>) -> Self {
        RedisData { record, expiry }
    }

    fn new_stream(key: String) -> Self {
        RedisData {
            record: RecordType::Stream(StreamRecord::new(key)),
            expiry: None,
        }
    }

    fn is_expired(&self) -> bool {
        if let Some(expiry) = self.expiry {
            expiry < SystemTime::now()
        } else {
            false
        }
    }
}

#[derive(Clone)]
pub struct Store {
    entries: Arc<RwLock<HashMap<String, RedisData>>>,
}

impl Store {
    pub fn from_path(dir: &Option<PathBuf>, dbfilename: &Option<String>) -> Result<Self> {
        if let (Some(dir), Some(filename)) = (dir, dbfilename) {
            let path = dir.join(filename);
            if path.exists() {
                let file = fs::File::open(&path)?;
                let instance = Instance::new(file)?;
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
            entries: Arc::new(RwLock::new(entries)),
        }
    }

    pub async fn get(&self, key: &str) -> Option<RecordType> {
        let storage = self.entries.read().await;
        let data = storage.get(key)?;
        if data.is_expired() {
            drop(storage);
            self.entries.write().await.remove(key);
            None
        } else {
            Some(data.record.clone())
        }
    }

    pub async fn set(&self, key: String, value: StringRecord, expiry: Option<SystemTime>) {
        self.entries
            .write()
            .await
            .insert(key, RedisData::new(RecordType::String(value), expiry));
    }

    pub async fn keys(&self, pattern: &str) -> Result<Vec<String>> {
        let pattern = Pattern::new(pattern)?;
        let mut expired = Vec::new();
        let mut matched = Vec::new();

        {
            let storage = self.entries.read().await;
            for (key, data) in storage.iter() {
                if data.is_expired() {
                    expired.push(key.clone());
                } else if pattern.matches(key) {
                    matched.push(key.clone());
                }
            }
        }

        let mut storage = self.entries.write().await;
        for key in expired {
            storage.remove(&key);
        }

        Ok(matched)
    }

    pub async fn add_stream_entry(
        &self,
        key: &str,
        entry_id: &str,
        values: HashMap<String, String>,
    ) -> Result<StreamEntryId> {
        let mut storage = self.entries.write().await;
        let stream_data = storage
            .entry(key.to_string())
            .or_insert_with(|| RedisData::new_stream(key.to_string()));

        if stream_data.is_expired() || matches!(stream_data.record, RecordType::String(_)) {
            *stream_data = RedisData::new_stream(key.to_string());
        }

        if let RecordType::Stream(stream) = &mut stream_data.record {
            stream.xadd(entry_id, values)
        } else {
            unreachable!("Stream data is not a Stream record");
        }
    }

    pub async fn get_stream_entries(
        &self,
        key: &str,
        start: &str,
        end: &str,
    ) -> Result<Vec<(StreamEntryId, HashMap<String, String>)>> {
        let storage = self.entries.read().await;
        let stream_data = storage
            .get(key)
            .ok_or_else(|| anyhow::anyhow!("ERR no such key"))?;

        let start = StreamEntryId::parse_start_range(start)?;
        let end = StreamEntryId::parse_end_range(end)?;

        if let RecordType::Stream(stream) = &stream_data.record {
            Ok(stream.xrange(start, end))
        } else {
            bail!("ERR Operation against a key holding the wrong kind of value");
        }
    }
}
