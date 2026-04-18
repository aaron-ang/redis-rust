//! Closure-based mutators that centralize wrong-type bail, record-type
//! destructuring, and watcher notification for `Store` record variants.

use std::collections::VecDeque;

use anyhow::{bail, Result};

use crate::types::{RedisError, StringRecord};

use super::db::{RecordType, RedisData, Store};
use super::sorted_set::SortedSetRecord;
use super::stream::StreamRecord;

/// Maps a `RecordType` variant to the inner value type it holds.
pub(super) trait RecordVariant: Sized {
    fn default_entry() -> RedisData;
    fn from_record_mut(r: &mut RecordType) -> Result<&mut Self>;
}

impl RecordVariant for VecDeque<String> {
    fn default_entry() -> RedisData {
        RedisData::new(RecordType::List(VecDeque::new()), None)
    }
    fn from_record_mut(r: &mut RecordType) -> Result<&mut Self> {
        match r {
            RecordType::List(list) => Ok(list),
            _ => bail!(RedisError::WrongType),
        }
    }
}

impl RecordVariant for SortedSetRecord {
    fn default_entry() -> RedisData {
        RedisData::new_sorted_set()
    }
    fn from_record_mut(r: &mut RecordType) -> Result<&mut Self> {
        match r {
            RecordType::SortedSet(s) => Ok(s),
            _ => bail!(RedisError::WrongType),
        }
    }
}

impl RecordVariant for StringRecord {
    fn default_entry() -> RedisData {
        RedisData::new(RecordType::String(StringRecord::Integer(0)), None)
    }
    fn from_record_mut(r: &mut RecordType) -> Result<&mut Self> {
        match r {
            RecordType::String(s) => Ok(s),
            _ => bail!(RedisError::WrongType),
        }
    }
}

impl Store {
    /// Run `f` against the record at `key`, creating a default entry if absent.
    /// The closure returns `(result, changed)`; watchers fire only when `changed`.
    pub(super) fn with_record_mut<T: RecordVariant, R>(
        &self,
        key: &str,
        f: impl FnOnce(&mut T) -> Result<(R, bool)>,
    ) -> Result<R> {
        let (result, changed) = {
            let mut entry = self
                .entries
                .entry(key.to_string())
                .or_insert_with(T::default_entry);
            let target = T::from_record_mut(&mut entry.record)?;
            f(target)?
        };
        if changed {
            self.notify_watchers(key);
        }
        Ok(result)
    }

    /// Run `f` against the record at `key` only if it exists; return
    /// `R::default()` otherwise. Does not create a missing key.
    pub(super) fn with_record_get_mut<T: RecordVariant, R: Default>(
        &self,
        key: &str,
        f: impl FnOnce(&mut T) -> Result<(R, bool)>,
    ) -> Result<R> {
        let (result, changed) = {
            let Some(mut entry) = self.entries.get_mut(key) else {
                return Ok(R::default());
            };
            let target = T::from_record_mut(&mut entry.record)?;
            f(target)?
        };
        if changed {
            self.notify_watchers(key);
        }
        Ok(result)
    }

    /// Run `f` against the stream at `key`. If the entry is expired or holds a
    /// different record type, replace it with a fresh stream before invoking `f`.
    pub(super) fn with_stream_mut<R>(
        &self,
        key: &str,
        f: impl FnOnce(&mut StreamRecord) -> Result<(R, bool)>,
    ) -> Result<R> {
        let (result, changed) = {
            let mut entry = self
                .entries
                .entry(key.to_string())
                .or_insert_with(|| RedisData::new_stream(key));
            if entry.is_expired() || !matches!(entry.record, RecordType::Stream(_)) {
                *entry = RedisData::new_stream(key);
            }
            let RecordType::Stream(stream) = &mut entry.record else {
                unreachable!("stream record ensured above");
            };
            f(stream)?
        };
        if changed {
            self.notify_watchers(key);
        }
        Ok(result)
    }
}
