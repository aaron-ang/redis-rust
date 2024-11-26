use anyhow::Result;
use std::{
    cmp::Ordering,
    collections::{BTreeMap, HashMap},
    str::FromStr,
    time::SystemTime,
};

use crate::util::InputError;

#[derive(Clone)]
pub struct StreamRecord {
    id: String,
    last_entry_id: StreamEntryId,
    value: BTreeMap<StreamEntryId, HashMap<String, String>>,
}

impl StreamRecord {
    pub fn new(id: String) -> Self {
        Self {
            id,
            last_entry_id: StreamEntryId::default(),
            value: BTreeMap::new(),
        }
    }

    pub fn xadd(
        &mut self,
        entry_id: &str,
        values: HashMap<String, String>,
    ) -> Result<StreamEntryId> {
        let entry_id = self.generate_entry_id(entry_id)?;
        self.validate_entry_id(&entry_id)?;
        self.value.insert(entry_id.clone(), values);
        Ok(entry_id)
    }

    pub fn xrange(
        &self,
        start: &StreamEntryId,
        end: &StreamEntryId,
        start_exclusive: bool,
    ) -> Vec<(StreamEntryId, HashMap<String, String>)> {
        self.value
            .range(start..=end)
            .filter(|(id, _)| !start_exclusive || *id > start)
            .map(|(id, values)| (id.clone(), values.clone()))
            .collect()
    }

    fn generate_entry_id(&self, entry_id: &str) -> Result<StreamEntryId> {
        if entry_id == "*" {
            let ts_ms = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)?
                .as_millis();
            let seq_num = match ts_ms.cmp(&self.last_entry_id.ts_ms) {
                std::cmp::Ordering::Less => todo!(),
                std::cmp::Ordering::Equal => self.last_entry_id.seq_no + 1,
                std::cmp::Ordering::Greater => 0,
            };
            return Ok(StreamEntryId::new(ts_ms, seq_num));
        }

        let (ts_str, seq_no_str) = entry_id
            .split_once('-')
            .ok_or_else(|| anyhow::anyhow!(InputError::InvalidEntryId))?;

        if seq_no_str == "*" {
            let ts_ms = ts_str.parse::<u128>()?;
            if ts_ms < self.last_entry_id.ts_ms {
                anyhow::bail!("ERR The ID specified in XADD must be greater than the last one");
            } else if ts_ms == self.last_entry_id.ts_ms {
                Ok(StreamEntryId::new(ts_ms, self.last_entry_id.seq_no + 1))
            } else {
                Ok(StreamEntryId::new(ts_ms, 0))
            }
        } else {
            entry_id.parse().map_err(anyhow::Error::from)
        }
    }

    fn validate_entry_id(&mut self, entry_id: &StreamEntryId) -> Result<()> {
        if entry_id <= &self.last_entry_id {
            let err_msg = if entry_id == &StreamEntryId::default() {
                "ERR The ID specified in XADD must be greater than 0-0"
            } else {
                "ERR The ID specified in XADD is equal or smaller than the target stream top item"
            };
            anyhow::bail!(err_msg);
        }
        self.last_entry_id = entry_id.clone();
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
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

impl Ord for StreamEntryId {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.ts_ms.cmp(&other.ts_ms) {
            Ordering::Equal => self.seq_no.cmp(&other.seq_no),
            ord => ord,
        }
    }
}

impl ToString for StreamEntryId {
    fn to_string(&self) -> String {
        format!("{}-{}", self.ts_ms, self.seq_no)
    }
}

impl FromStr for StreamEntryId {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (ts_str, seq_no_str) = s
            .split_once('-')
            .ok_or_else(|| anyhow::anyhow!(InputError::InvalidEntryId))?;

        let ts = ts_str.parse()?;
        let seq_no = seq_no_str.parse()?;

        Ok(Self::new(ts, seq_no))
    }
}

impl Default for StreamEntryId {
    fn default() -> Self {
        Self::new(0, 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stream_entry_id_ordering() {
        let id1 = StreamEntryId {
            ts_ms: 100,
            seq_no: 1,
        };
        let id2 = StreamEntryId {
            ts_ms: 200,
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

        assert!(id3 < id1);
        assert!(id3 < id2);
        assert!(id4 < id1);
        assert!(id4 < id2);
        assert!(id4 < id1);

        let mut map: BTreeMap<StreamEntryId, String> = BTreeMap::new();
        map.insert(id3.clone(), "id3".to_string());
        map.insert(id1.clone(), "id1".to_string());
        map.insert(id2.clone(), "id2".to_string());
        map.insert(id4.clone(), "id4".to_string());

        let keys: Vec<_> = map.keys().cloned().collect();
        assert_eq!(keys, vec![id4, id3, id1, id2]);
    }
}
