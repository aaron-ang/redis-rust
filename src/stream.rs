use anyhow::Result;
use std::{collections::HashMap, str::FromStr};

#[derive(Clone)]
pub struct StreamRecord {
    id: String,
    top_entry_id: StreamEntryId,
    value: HashMap<StreamEntryId, HashMap<String, String>>,
}

impl StreamRecord {
    pub fn new(id: String) -> Self {
        Self {
            id,
            top_entry_id: StreamEntryId::default(),
            value: HashMap::new(),
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

    fn generate_entry_id(&self, entry_id: &str) -> Result<StreamEntryId> {
        let (ts_str, seq_no_str) = entry_id
            .split_once('-')
            .ok_or_else(|| anyhow::anyhow!("Invalid entry ID format"))?;

        let ts_ms = ts_str.parse::<u64>()?;
        match seq_no_str {
            "*" => {
                if ts_ms < self.top_entry_id.ts_ms {
                    anyhow::bail!("ERR The ID specified in XADD must be greater than the last one");
                } else if ts_ms == self.top_entry_id.ts_ms {
                    Ok(StreamEntryId::new(ts_ms, self.top_entry_id.seq_no + 1))
                } else {
                    Ok(StreamEntryId::new(ts_ms, 0))
                }
            }
            _ => entry_id.parse().map_err(anyhow::Error::from),
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

#[derive(Clone, PartialEq, Eq, Hash, PartialOrd)]
pub struct StreamEntryId {
    ts_ms: u64,
    seq_no: u64,
}

impl StreamEntryId {
    fn new(ts_ms: u64, seq_no: u64) -> Self {
        Self { ts_ms, seq_no }
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
