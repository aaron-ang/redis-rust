use std::{
    cmp::Ordering,
    collections::{BTreeMap, HashMap},
    fmt,
    str::FromStr,
    time::SystemTime,
};

use anyhow::Result;
use tokio::sync::mpsc;

use crate::types::RedisError;

#[derive(Clone)]
pub struct StreamRecord {
    stream_id: String,
    value: StreamValue,
    last_entry_id: StreamEntryId,
    listeners: HashMap<StreamEntryId, Vec<mpsc::Sender<(String, StreamValue)>>>,
}

impl StreamRecord {
    pub fn new(id: &str) -> Self {
        Self {
            stream_id: id.to_string(),
            value: StreamValue(BTreeMap::new()),
            last_entry_id: StreamEntryId::default(),
            listeners: HashMap::new(),
        }
    }

    pub fn last_entry_id(&self) -> StreamEntryId {
        self.last_entry_id
    }

    pub fn xadd(
        &mut self,
        entry_id_str: &str,
        values: HashMap<String, String>,
    ) -> Result<StreamEntryId> {
        let entry_id = self.generate_entry_id(entry_id_str)?;
        self.validate_entry_id(entry_id)?;
        self.value.0.insert(entry_id, values.clone());

        // Notify listeners whose expected_id is less than or equal to the new entry_id
        let message = (
            self.stream_id.clone(),
            StreamValue(BTreeMap::from([(entry_id, values.clone())])),
        );
        self.listeners.retain(|expected_id, senders| {
            if *expected_id <= entry_id {
                for tx in senders {
                    let _ = tx.try_send(message.clone());
                }
                false // Remove this listener after notification
            } else {
                true // Keep this listener
            }
        });

        Ok(entry_id)
    }

    pub fn xrange(
        &self,
        start: StreamEntryId,
        end: StreamEntryId,
        start_exclusive: bool,
    ) -> StreamValue {
        let result: BTreeMap<StreamEntryId, HashMap<String, String>> = self
            .value
            .0
            .range(start..=end)
            .filter(|&(id, _)| !start_exclusive || *id > start)
            .map(|(id, value)| (*id, value.clone()))
            .collect();
        StreamValue(result)
    }

    fn generate_entry_id(&self, entry_id: &str) -> Result<StreamEntryId> {
        if entry_id == "*" {
            let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)?;
            let mut ts_ms = now.as_millis();
            let seq_num = match ts_ms.cmp(&self.last_entry_id.ts_ms) {
                Ordering::Less | Ordering::Equal => {
                    ts_ms = self.last_entry_id.ts_ms;
                    self.last_entry_id.seq_no + 1
                }
                Ordering::Greater => 0,
            };
            return Ok(StreamEntryId::new(ts_ms, seq_num));
        }

        let (ts_str, seq_no_str) = entry_id
            .split_once('-')
            .ok_or_else(|| anyhow::anyhow!(RedisError::InvalidEntryId))?;

        if seq_no_str == "*" {
            let ts_ms = ts_str.parse::<u128>()?;
            match ts_ms.cmp(&self.last_entry_id.ts_ms) {
                Ordering::Less => anyhow::bail!(RedisError::XAddIdInvalidSequence),
                Ordering::Equal => Ok(StreamEntryId::new(ts_ms, self.last_entry_id.seq_no + 1)),
                Ordering::Greater => Ok(StreamEntryId::new(ts_ms, 0)),
            }
        } else {
            entry_id.parse()
        }
    }

    fn validate_entry_id(&mut self, entry_id: StreamEntryId) -> Result<()> {
        if entry_id <= self.last_entry_id {
            let err_msg = if entry_id == StreamEntryId::default() {
                RedisError::XAddIdTooSmall
            } else {
                RedisError::XAddIdInvalidSequence
            };
            anyhow::bail!(err_msg);
        }
        self.last_entry_id = entry_id;
        Ok(())
    }

    pub fn subscribe(&mut self, id: StreamEntryId, tx: mpsc::Sender<(String, StreamValue)>) {
        self.listeners.entry(id).or_default().push(tx);
    }
}

#[derive(Clone)]
pub struct StreamValue(BTreeMap<StreamEntryId, HashMap<String, String>>);

impl StreamValue {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&StreamEntryId, &HashMap<String, String>)> {
        self.0.iter()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct StreamEntryId {
    ts_ms: u128,
    seq_no: u64,
}

impl StreamEntryId {
    fn new(ts_ms: u128, seq_no: u64) -> Self {
        Self { ts_ms, seq_no }
    }

    pub fn parse_start_range(s: &str) -> Result<Self> {
        match s {
            "-" => Ok(Self::default()),
            _ => Self::parse_range(s),
        }
    }

    pub fn parse_end_range(s: &str) -> Result<Self> {
        match s {
            "+" => Ok(Self::MAX),
            _ => Self::parse_range(s),
        }
    }

    fn parse_range(s: &str) -> Result<Self> {
        match s.split_once('-') {
            Some((ts_ms_str, seq_no_str)) => Ok(Self {
                ts_ms: ts_ms_str.parse()?,
                seq_no: seq_no_str.parse()?,
            }),
            None => Ok(Self::new(s.parse()?, 0)),
        }
    }

    pub const MAX: Self = Self {
        ts_ms: u128::MAX,
        seq_no: u64::MAX,
    };
}

impl fmt::Display for StreamEntryId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.ts_ms, self.seq_no)
    }
}

impl FromStr for StreamEntryId {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (ts_str, seq_no_str) = s
            .split_once('-')
            .ok_or_else(|| anyhow::anyhow!(RedisError::InvalidEntryId))?;

        let ts = ts_str.parse()?;
        let seq_no = seq_no_str.parse()?;

        Ok(Self::new(ts, seq_no))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stream_entry_id_ordering() {
        let id1 = StreamEntryId {
            ts_ms: 200,
            seq_no: 1,
        };
        let id2 = StreamEntryId {
            ts_ms: 100,
            seq_no: 1,
        };
        let id3 = StreamEntryId {
            ts_ms: 50,
            seq_no: 2,
        };
        let id4 = StreamEntryId {
            ts_ms: 50,
            seq_no: 1,
        };

        let mut ids = vec![id3, id1, id2, id4];
        ids.sort();
        assert_eq!(ids, vec![id4, id3, id2, id1]);
    }
}
