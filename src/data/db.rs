use std::{
    collections::{HashMap, VecDeque},
    fs,
    path::PathBuf,
    sync::Arc,
    time::SystemTime,
};

use anyhow::{bail, Result};
use dashmap::DashMap;
use futures::future::select_all;
use glob::Pattern;
use tokio::{
    sync::{mpsc, oneshot, RwLock},
    time::{self, Duration, Instant},
};

use crate::geo::{decode, get_distance};
use crate::types::{Instance, RedisError, StringRecord, XReadBlockType};

use super::sorted_set::SortedSetRecord;
use super::stream::{StreamEntryId, StreamRecord, StreamValue};

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
        let Some(expiry) = self.expiry else {
            return false;
        };
        expiry < SystemTime::now()
    }
}

#[derive(Clone)]
pub struct Store {
    entries: Arc<DashMap<String, RedisData>>,
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
            entries: Arc::new(DashMap::from_iter(entries)),
            list_waiters: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    async fn notify_list_waiters(&self, key: &str) {
        let mut waiters = self.list_waiters.write().await;
        let Some(wait_queue) = waiters.get_mut(key) else {
            return;
        };
        // Notify the first waiter
        while let Some(sender) = wait_queue.pop_front() {
            if sender.send(()).is_ok() {
                return;
            }
        }
    }

    pub fn db_size(&self) -> usize {
        self.entries.len()
    }

    pub fn flushall(&self) {
        self.entries.clear();
    }

    pub fn get(&self, key: &str) -> Option<String> {
        let entry = self.entries.get(key)?;
        if entry.is_expired() {
            drop(entry);
            self.entries.remove(key);
            return None;
        }
        let RecordType::String(string_rec) = &entry.record else {
            return None;
        };
        Some(string_rec.to_string())
    }

    pub fn set(&self, key: String, value: StringRecord, expiry: Option<SystemTime>) {
        self.entries
            .insert(key, RedisData::new(RecordType::String(value), expiry));
    }

    pub fn incr(&self, key: &str) -> Result<i64> {
        let mut entry = self
            .entries
            .entry(key.to_string())
            .or_insert_with(|| RedisData::new(RecordType::String(StringRecord::Integer(0)), None));
        let RecordType::String(string_rec) = &mut entry.record else {
            bail!(RedisError::WrongType);
        };
        string_rec.incr()
    }

    pub async fn rpush(&self, key: &str, elements: &[&str]) -> Result<i64> {
        let len = {
            let mut entry = self
                .entries
                .entry(key.to_string())
                .or_insert_with(|| RedisData::new(RecordType::List(VecDeque::new()), None));
            let RecordType::List(list) = &mut entry.record else {
                bail!(RedisError::WrongType);
            };
            for element in elements {
                list.push_back(element.to_string());
            }
            list.len() as i64
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
            for &key in keys {
                if let Some(mut entry) = self.entries.get_mut(key) {
                    let RecordType::List(list) = &mut entry.record else {
                        bail!(RedisError::WrongType);
                    };
                    if let Some(val) = list.pop_front() {
                        return Ok(Some((key.to_string(), val)));
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
            let Some(deadline) = deadline else {
                let _ = wait_all.await;
                continue;
            };
            let now = Instant::now();
            if now >= deadline {
                return Ok(None);
            }
            let remaining_duration = deadline - now;
            if time::timeout(remaining_duration, wait_all).await.is_err() {
                return Ok(None);
            }

            // 4. Loop back to check for data again
        }
    }

    pub fn lpop(&self, key: &str, count: usize) -> Result<Vec<String>> {
        let Some(mut entry) = self.entries.get_mut(key) else {
            return Ok(Vec::new());
        };
        let RecordType::List(list) = &mut entry.record else {
            bail!(RedisError::WrongType);
        };
        Ok((0..count).filter_map(|_| list.pop_front()).collect())
    }

    pub async fn lpush(&self, key: &str, elements: &[&str]) -> Result<i64> {
        let len = {
            let mut entry = self
                .entries
                .entry(key.to_string())
                .or_insert_with(|| RedisData::new(RecordType::List(VecDeque::new()), None));

            let RecordType::List(list) = &mut entry.record else {
                bail!(RedisError::WrongType);
            };
            for element in elements {
                list.push_front(element.to_string());
            }
            list.len() as i64
        };
        self.notify_list_waiters(key).await;
        Ok(len)
    }

    pub fn lrange(&self, key: &str, start: i64, end: i64) -> Result<Vec<String>> {
        let Some(entry) = self.entries.get(key) else {
            return Ok(Vec::new());
        };
        let RecordType::List(list) = &entry.record else {
            bail!(RedisError::WrongType);
        };

        let len = list.len() as i64;
        let start = if start < 0 { len + start } else { start };
        let end = if end < 0 { len + end } else { end };
        let start = start.max(0) as usize;
        let end = end.min(len - 1) as usize;
        Ok(list.range(start..=end).map(|s| s.to_owned()).collect())
    }

    pub fn llen(&self, key: &str) -> Result<i64> {
        let Some(entry) = self.entries.get(key) else {
            return Ok(0);
        };
        let RecordType::List(list) = &entry.record else {
            bail!(RedisError::WrongType);
        };
        Ok(list.len() as i64)
    }

    pub fn keys(&self, pattern: &str) -> Result<Vec<String>> {
        let pattern = Pattern::new(pattern)?;
        let mut expired = Vec::new();
        let mut matched = Vec::new();

        for entry in self.entries.iter() {
            let (key, data) = entry.pair();
            if data.is_expired() {
                expired.push(key.to_owned());
            } else if pattern.matches(key) {
                matched.push(key.to_owned());
            }
        }

        for key in expired {
            self.entries.remove(&key);
        }

        Ok(matched)
    }

    pub fn type_(&self, key: &str) -> String {
        let Some(entry) = self.entries.get(key) else {
            return "none".into();
        };
        let type_ = match &entry.record {
            RecordType::String(_) => "string",
            RecordType::Stream(_) => "stream",
            RecordType::List(_) => "list",
            RecordType::SortedSet(_) => "zset",
        };
        type_.into()
    }

    pub fn add_stream_entry(
        &self,
        key: &str,
        entry_id: &str,
        values: HashMap<String, String>,
    ) -> Result<StreamEntryId> {
        let mut entry = self
            .entries
            .entry(key.to_string())
            .or_insert_with(|| RedisData::new_stream(key));

        if entry.is_expired() || !matches!(entry.record, RecordType::Stream(_)) {
            *entry = RedisData::new_stream(key);
        }

        let RecordType::Stream(stream) = &mut entry.record else {
            bail!(RedisError::WrongType);
        };
        stream.xadd(entry_id, values)
    }

    pub fn get_range_stream_entries(
        &self,
        key: &str,
        start: &str,
        end: &str,
    ) -> Result<StreamValue> {
        let start = StreamEntryId::parse_start_range(start)?;
        let end = StreamEntryId::parse_end_range(end)?;

        let entry = self
            .entries
            .get(key)
            .ok_or_else(|| anyhow::anyhow!(RedisError::KeyNotFound))?;
        let RecordType::Stream(stream) = &entry.record else {
            bail!(RedisError::WrongType);
        };
        Ok(stream.xrange(start, end, false))
    }

    pub async fn get_bulk_stream_entries(
        &self,
        streams: &[(&str, &str)],
        block_option: XReadBlockType,
    ) -> Result<HashMap<String, StreamValue>> {
        let mut res = HashMap::new();
        let (tx, mut rx) = mpsc::channel(streams.len());

        for &(stream_key, start_id) in streams {
            let Some(mut entry) = self.entries.get_mut(stream_key) else {
                continue;
            };
            let RecordType::Stream(stream) = &mut entry.record else {
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

    pub fn zadd(&self, key: &str, member: &str, score: f64) -> Result<bool> {
        let mut entry = self
            .entries
            .entry(key.to_string())
            .or_insert_with(RedisData::new_sorted_set);

        let RecordType::SortedSet(sorted_set) = &mut entry.record else {
            bail!(RedisError::WrongType);
        };
        Ok(sorted_set.add(member, score))
    }

    pub fn zcard(&self, key: &str) -> Result<i64> {
        let Some(entry) = self.entries.get(key) else {
            return Ok(0);
        };
        let RecordType::SortedSet(sorted_set) = &entry.record else {
            bail!(RedisError::WrongType);
        };
        Ok(sorted_set.len())
    }

    pub fn zrange(&self, key: &str, start: i64, end: i64) -> Result<Vec<String>> {
        let Some(entry) = self.entries.get(key) else {
            return Ok(Vec::new());
        };
        let RecordType::SortedSet(sorted_set) = &entry.record else {
            bail!(RedisError::WrongType);
        };
        Ok(sorted_set.range(start, end))
    }

    pub fn zrank(&self, key: &str, member: &str) -> Result<Option<i64>> {
        let Some(entry) = self.entries.get(key) else {
            return Ok(None);
        };
        let RecordType::SortedSet(sorted_set) = &entry.record else {
            bail!(RedisError::WrongType);
        };
        Ok(sorted_set.rank(member))
    }

    pub fn zrem(&self, key: &str, members: &[&str]) -> Result<i64> {
        let mut entry = self
            .entries
            .entry(key.to_string())
            .or_insert_with(RedisData::new_sorted_set);
        let RecordType::SortedSet(sorted_set) = &mut entry.record else {
            bail!(RedisError::WrongType);
        };
        Ok(sorted_set.remove(members))
    }

    pub fn zscore(&self, key: &str, member: &str) -> Result<Option<f64>> {
        let Some(entry) = self.entries.get(key) else {
            return Ok(None);
        };
        let RecordType::SortedSet(sorted_set) = &entry.record else {
            bail!(RedisError::WrongType);
        };
        Ok(sorted_set.score(member))
    }

    pub fn geodist(&self, key: &str, member1: &str, member2: &str) -> Result<Option<f64>> {
        let Some(entry) = self.entries.get(key) else {
            return Ok(None);
        };
        let RecordType::SortedSet(sorted_set) = &entry.record else {
            bail!(RedisError::WrongType);
        };

        let score1 = sorted_set.score(member1);
        let score2 = sorted_set.score(member2);

        match (score1, score2) {
            (Some(score1), Some(score2)) => {
                let (lat1, lon1) = decode(score1 as u64);
                let (lat2, lon2) = decode(score2 as u64);
                let distance = get_distance(lon1, lat1, lon2, lat2);
                Ok(Some(distance))
            }
            _ => Ok(None),
        }
    }

    pub fn geopos(&self, key: &str, members: &[&str]) -> Result<Vec<Option<(f64, f64)>>> {
        let Some(entry) = self.entries.get(key) else {
            return Ok(vec![None; members.len()]);
        };
        let RecordType::SortedSet(sorted_set) = &entry.record else {
            bail!(RedisError::WrongType);
        };
        let positions = members
            .iter()
            .map(|member| {
                sorted_set.score(member).map(|score| {
                    let (latitude, longitude) = decode(score as u64);
                    (latitude, longitude)
                })
            })
            .collect();
        Ok(positions)
    }

    pub fn geosearch(
        &self,
        key: &str,
        from_lon: f64,
        from_lat: f64,
        radius_m: f64,
    ) -> Result<Vec<String>> {
        let Some(entry) = self.entries.get(key) else {
            return Ok(Vec::new());
        };
        let RecordType::SortedSet(sorted_set) = &entry.record else {
            bail!(RedisError::WrongType);
        };

        let results = sorted_set
            .members()
            .filter_map(|member| {
                sorted_set.score(member).and_then(|score| {
                    let (lat, lon) = decode(score as u64);
                    let distance = get_distance(from_lon, from_lat, lon, lat);
                    if distance <= radius_m {
                        Some(member.to_owned())
                    } else {
                        None
                    }
                })
            })
            .collect();

        Ok(results)
    }
}
