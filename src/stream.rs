use anyhow::Result;
use std::{
    collections::{BTreeMap, HashMap},
    str::FromStr,
    time::SystemTime,
};

#[derive(Clone)]
pub struct StreamRecord {
    id: String,
    top_entry_id: StreamEntryId,
    value: BTreeMap<StreamEntryId, HashMap<String, String>>,
}

impl StreamRecord {
    pub fn new(id: String) -> Self {
        Self {
            id,
            top_entry_id: StreamEntryId::default(),
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
        mut start: StreamEntryId,
        end: StreamEntryId,
        start_exclusive: bool,
    ) -> Vec<(StreamEntryId, HashMap<String, String>)> {
        if start_exclusive {
            start = self
                .value
                .range(start..)
                .next()
                .map(|(id, _)| id.clone())
                .unwrap_or(StreamEntryId::MAX);
        }
        self.value
            .range(start..=end)
            .map(|(id, values)| (id.clone(), values.clone()))
            .collect()
    }

    fn generate_entry_id(&self, entry_id: &str) -> Result<StreamEntryId> {
        if entry_id == "*" {
            let ts_ms = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)?
                .as_millis();
            let seq_num = match ts_ms.cmp(&self.top_entry_id.ts_ms) {
                std::cmp::Ordering::Less => todo!(),
                std::cmp::Ordering::Equal => self.top_entry_id.seq_no + 1,
                std::cmp::Ordering::Greater => 0,
            };
            return Ok(StreamEntryId::new(ts_ms, seq_num));
        }

        let (ts_str, seq_no_str) = entry_id
            .split_once('-')
            .ok_or_else(|| anyhow::anyhow!("Invalid entry ID format"))?;

        if seq_no_str == "*" {
            let ts_ms = ts_str.parse::<u128>()?;
            if ts_ms < self.top_entry_id.ts_ms {
                anyhow::bail!("ERR The ID specified in XADD must be greater than the last one");
            } else if ts_ms == self.top_entry_id.ts_ms {
                Ok(StreamEntryId::new(ts_ms, self.top_entry_id.seq_no + 1))
            } else {
                Ok(StreamEntryId::new(ts_ms, 0))
            }
        } else {
            entry_id.parse().map_err(anyhow::Error::from)
        }
    }

    fn validate_entry_id(&mut self, entry_id: &StreamEntryId) -> Result<()> {
        if entry_id <= &self.top_entry_id {
            let err_msg = if entry_id == &StreamEntryId::default() {
                "ERR The ID specified in XADD must be greater than 0-0"
            } else {
                "ERR The ID specified in XADD is equal or smaller than the target stream top item"
            };
            anyhow::bail!(err_msg);
        }
        self.top_entry_id = entry_id.clone();
        Ok(())
    }
}

#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
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
            .ok_or_else(|| anyhow::anyhow!("Invalid entry ID format"))?;

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
