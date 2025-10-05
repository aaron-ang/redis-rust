use std::{
    collections::{HashMap, VecDeque},
    fs,
    path::PathBuf,
    sync::Arc,
    time::SystemTime,
};

use anyhow::{bail, Result};
use futures::future::select_all;
use glob::Pattern;
use tokio::{
    sync::{mpsc, oneshot, RwLock},
    time::{self, Duration, Instant},
};

use crate::sorted_set::SortedSetRecord;
use crate::stream::{StreamEntryId, StreamRecord, StreamValue};
use crate::types::{Instance, RedisError, StringRecord, XReadBlockType};

pub enum RecordType {
    String(StringRecord),
    Stream(StreamRecord),
    List(VecDeque<String>),
    SortedSet(SortedSetRecord),
}

pub struct RedisData {
    record: RecordType,
    expiry: Option<SystemTime>,
}

impl RedisData {
    pub fn new(record: RecordType, expiry: Option<SystemTime>) -> Self {
        RedisData { record, expiry }
    }

    fn new_stream(key: &str) -> Self {
        RedisData {
            record: RecordType::Stream(StreamRecord::new(key)),
            expiry: None,
        }
    }

    fn new_sorted_set() -> Self {
        RedisData {
            record: RecordType::SortedSet(SortedSetRecord::new()),
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
    list_waiters: Arc<RwLock<HashMap<String, VecDeque<oneshot::Sender<()>>>>>,
}

impl Store {
    pub fn from_path(dir: &Option<PathBuf>, dbfilename: &Option<String>) -> Result<Self> {
        if let (Some(dir), Some(filename)) = (dir, dbfilename) {
            let path = dir.join(filename);
            if path.exists() {
                let file = fs::File::open(&path)?;
                let instance = Instance::new(file)?;
                return instance.get_db(0).cloned();
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
            list_waiters: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    async fn notify_list_waiters(&self, key: &str) {
        let mut waiters = self.list_waiters.write().await;
        if let Some(wait_queue) = waiters.get_mut(key) {
            // Notify the first waiter
            while let Some(sender) = wait_queue.pop_front() {
                if sender.send(()).is_ok() {
                    return;
                }
            }
        }
    }

    pub async fn get(&self, key: &str) -> Option<RecordType> {
        let storage = self.entries.read().await;
        let data = storage.get(key)?;

        if data.is_expired() {
            drop(storage);
            self.entries.write().await.remove(key);
            return None;
        }

        if let RecordType::String(string_rec) = &data.record {
            Some(RecordType::String(string_rec.clone()))
        } else {
            None
        }
    }

    pub async fn set(&self, key: String, value: StringRecord, expiry: Option<SystemTime>) {
        self.entries
            .write()
            .await
            .insert(key, RedisData::new(RecordType::String(value), expiry));
    }

    pub async fn incr(&self, key: &str) -> Result<i64> {
        let mut storage = self.entries.write().await;
        let data = storage
            .entry(key.to_string())
            .or_insert_with(|| RedisData::new(RecordType::String(StringRecord::Integer(0)), None));

        if let RecordType::String(string_rec) = &mut data.record {
            string_rec.incr()
        } else {
            bail!(RedisError::WrongType)
        }
    }

    pub async fn rpush(&self, key: &str, elements: &[&str]) -> Result<i64> {
        let len = {
            let mut storage = self.entries.write().await;
            let data = storage
                .entry(key.to_string())
                .or_insert_with(|| RedisData::new(RecordType::List(VecDeque::new()), None));

            if let RecordType::List(list) = &mut data.record {
                for element in elements {
                    list.push_back(element.to_string());
                }
                list.len() as i64
            } else {
                bail!(RedisError::WrongType)
            }
        };
        self.notify_list_waiters(key).await;
        Ok(len)
    }

    pub async fn blpop(&self, keys: &[&str], timeout: f64) -> Result<Option<(String, String)>> {
        let deadline = if timeout > 0.0 {
            Some(Instant::now() + Duration::from_secs_f64(timeout))
        } else {
            None
        };

        loop {
            // 1. Non-blocking check for data, respecting key order.
            {
                let mut storage = self.entries.write().await;
                for &key in keys {
                    if let Some(data) = storage.get_mut(key) {
                        if let RecordType::List(list) = &mut data.record {
                            if let Some(val) = list.pop_front() {
                                return Ok(Some((key.to_string(), val)));
                            }
                        } else {
                            bail!(RedisError::WrongType);
                        }
                    }
                }
            }

            // 2. Register a waiter for each key
            let mut receivers = Vec::new();
            {
                let mut waiters = self.list_waiters.write().await;
                for &key in keys {
                    let (tx, rx) = oneshot::channel();
                    waiters.entry(key.to_string()).or_default().push_back(tx);
                    receivers.push(rx);
                }
            }

            // 3. Wait for a notification or timeout
            let wait_all = select_all(receivers);
            if let Some(deadline) = deadline {
                let now = Instant::now();
                if now >= deadline {
                    return Ok(None);
                }
                let remaining_duration = deadline - now;
                if time::timeout(remaining_duration, wait_all).await.is_err() {
                    return Ok(None);
                }
            } else {
                let _ = wait_all.await;
            }

            // 4. Loop back to check for data again
        }
    }

    pub async fn lpop(&self, key: &str, count: usize) -> Result<Vec<String>> {
        let mut storage = self.entries.write().await;
        let Some(data) = storage.get_mut(key) else {
            return Ok(Vec::new());
        };
        if let RecordType::List(list) = &mut data.record {
            Ok((0..count).filter_map(|_| list.pop_front()).collect())
        } else {
            bail!(RedisError::WrongType)
        }
    }

    pub async fn lpush(&self, key: &str, elements: &[&str]) -> Result<i64> {
        let len = {
            let mut storage = self.entries.write().await;
            let data = storage
                .entry(key.to_string())
                .or_insert_with(|| RedisData::new(RecordType::List(VecDeque::new()), None));

            if let RecordType::List(list) = &mut data.record {
                for element in elements {
                    list.push_front(element.to_string());
                }
                list.len() as i64
            } else {
                bail!(RedisError::WrongType)
            }
        };
        self.notify_list_waiters(key).await;
        Ok(len)
    }

    pub async fn lrange(&self, key: &str, start: i64, end: i64) -> Result<Vec<String>> {
        let storage = self.entries.read().await;
        let Some(data) = storage.get(key) else {
            return Ok(Vec::new());
        };
        if let RecordType::List(list) = &data.record {
            let len = list.len() as i64;
            let start = if start < 0 { len + start } else { start };
            let end = if end < 0 { len + end } else { end };
            let start = start.max(0) as usize;
            let end = end.min(len - 1) as usize;
            Ok(list
                .iter()
                .skip(start)
                .take(end - start + 1)
                .cloned()
                .collect())
        } else {
            bail!(RedisError::WrongType)
        }
    }

    pub async fn llen(&self, key: &str) -> Result<i64> {
        let storage = self.entries.read().await;
        let Some(data) = storage.get(key) else {
            return Ok(0);
        };
        if let RecordType::List(list) = &data.record {
            Ok(list.len() as i64)
        } else {
            bail!(RedisError::WrongType)
        }
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

    pub async fn type_(&self, key: &str) -> String {
        let storage = self.entries.read().await;
        let Some(data) = storage.get(key) else {
            return "none".into();
        };
        let type_ = match &data.record {
            RecordType::String(_) => "string",
            RecordType::Stream(_) => "stream",
            RecordType::List(_) => "list",
            RecordType::SortedSet(_) => "zset",
        };
        type_.into()
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
            .or_insert_with(|| RedisData::new_stream(key));

        if stream_data.is_expired() || !matches!(stream_data.record, RecordType::Stream(_)) {
            *stream_data = RedisData::new_stream(key);
        }

        if let RecordType::Stream(stream) = &mut stream_data.record {
            stream.xadd(entry_id, values).await
        } else {
            bail!(RedisError::WrongType);
        }
    }

    pub async fn get_range_stream_entries(
        &self,
        key: &str,
        start: &str,
        end: &str,
    ) -> Result<StreamValue> {
        let start = StreamEntryId::parse_start_range(start)?;
        let end = StreamEntryId::parse_end_range(end)?;

        let storage = self.entries.read().await;
        let stream_data = storage
            .get(key)
            .ok_or_else(|| anyhow::anyhow!(RedisError::KeyNotFound))?;

        if let RecordType::Stream(stream) = &stream_data.record {
            Ok(stream.xrange(start, end, false))
        } else {
            bail!(RedisError::WrongType);
        }
    }

    pub async fn get_bulk_stream_entries(
        &self,
        streams: &[(&str, &str)],
        block_option: XReadBlockType,
    ) -> Result<HashMap<String, StreamValue>> {
        let mut res = HashMap::new();
        let (tx, mut rx) = mpsc::channel(streams.len());

        {
            let mut storage = self.entries.write().await;
            for &(stream_key, start_id) in streams {
                let Some(stream_data) = storage.get_mut(stream_key) else {
                    continue;
                };
                let RecordType::Stream(stream) = &mut stream_data.record else {
                    bail!(RedisError::WrongType);
                };
                match start_id {
                    "$" => stream.subscribe(stream.last_entry_id(), tx.clone()),
                    _ => {
                        let start = StreamEntryId::parse_start_range(start_id)?;
                        let range_entries = stream.xrange(start, StreamEntryId::MAX, true);
                        if range_entries.is_empty() {
                            stream.subscribe(start, tx.clone());
                        } else {
                            res.insert(stream_key.to_owned(), range_entries);
                        }
                    }
                }
            }
        }

        if block_option == XReadBlockType::NoWait || !res.is_empty() {
            return Ok(res);
        }

        let await_stream_entry = async {
            if let Some((stream_key, entry)) = rx.recv().await {
                res.insert(stream_key, entry);
            }
            res
        };

        match block_option {
            XReadBlockType::Wait(duration) => Ok(time::timeout(duration, await_stream_entry)
                .await
                .unwrap_or_default()),
            XReadBlockType::WaitIndefinitely => Ok(await_stream_entry.await),
            _ => unreachable!(),
        }
    }

    pub async fn zadd(&self, key: &str, member: &str, score: f64) -> Result<bool> {
        let mut storage = self.entries.write().await;
        let data = storage
            .entry(key.to_string())
            .or_insert_with(|| RedisData::new_sorted_set());

        if let RecordType::SortedSet(sorted_set) = &mut data.record {
            Ok(sorted_set.add(member, score))
        } else {
            bail!(RedisError::WrongType);
        }
    }

    pub async fn zrange(&self, key: &str, start: i64, end: i64) -> Result<Vec<String>> {
        let storage = self.entries.read().await;
        let Some(data) = storage.get(key) else {
            return Ok(Vec::new());
        };
        if let RecordType::SortedSet(sorted_set) = &data.record {
            Ok(sorted_set.range(start, end))
        } else {
            bail!(RedisError::WrongType);
        }
    }

    pub async fn zrank(&self, key: &str, member: &str) -> Result<Option<i64>> {
        let storage = self.entries.read().await;
        let Some(data) = storage.get(key) else {
            return Ok(None);
        };
        if let RecordType::SortedSet(sorted_set) = &data.record {
            Ok(sorted_set.rank(member))
        } else {
            bail!(RedisError::WrongType);
        }
    }
}
