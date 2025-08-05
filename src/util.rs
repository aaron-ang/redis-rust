use std::{
    collections::HashMap,
    fmt,
    io::Read,
    str::FromStr,
    time::{Duration, UNIX_EPOCH},
};

use anyhow::Result;
use byteorder::{BigEndian, LittleEndian, ReadBytesExt};
use strum::{Display, EnumString};
use thiserror::Error;
use tokio::sync::RwLock;

use crate::db::{RecordType, RedisData, Store};

#[derive(Debug, Clone, Copy, PartialEq, Display, EnumString)]
#[strum(ascii_case_insensitive, serialize_all = "UPPERCASE")]
pub enum Command {
    BLPop,
    Command,
    Config,
    Discard,
    Echo,
    Exec,
    Get,
    Incr,
    Info,
    Keys,
    LLen,
    LPop,
    LPush,
    LRange,
    Multi,
    Ping,
    Publish,
    PSync,
    ReplConf,
    RPush,
    Set,
    Subscribe,
    Unsubscribe,
    Type,
    Wait,
    XAdd,
    XRange,
    XRead,
}

impl Command {
    pub fn is_write(&self) -> bool {
        matches!(
            self,
            Command::BLPop
                | Command::Incr
                | Command::LPop
                | Command::LPush
                | Command::RPush
                | Command::Set
                | Command::XAdd
        )
    }
}

#[derive(Error, Debug)]
pub enum RedisError {
    #[error("ERR wrong number of arguments for command")]
    InvalidArgument,
    #[error("ERR value is not an integer or out of range")]
    InvalidInteger,
    #[error("WRONGTYPE Operation against a key holding the wrong kind of value")]
    WrongType,
    #[error("ERR no such key")]
    KeyNotFound,
    #[error("ERR invalid entry ID format")]
    InvalidEntryId,
    #[error("ERR The ID specified in XADD must be greater than 0-0")]
    XAddIdTooSmall,
    #[error("ERR The ID specified in XADD is equal or smaller than the target stream top item")]
    XAddIdInvalidSequence,
    #[error("ERR {0} without MULTI")]
    CommandWithoutMulti(Command),
    #[error(
        "ERR Can't execute '{0}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context"
    )]
    CommandWithoutSubscribe(Command),
}

#[derive(Clone, PartialEq, Display)]
pub enum ReplicaType {
    #[strum(serialize = "master")]
    Leader,
    #[strum(serialize = "slave")]
    Follower,
}

pub struct ReplicationState {
    num_ack: RwLock<usize>,
    num_commands: RwLock<usize>,
}

impl Default for ReplicationState {
    fn default() -> Self {
        Self::new()
    }
}

impl ReplicationState {
    pub fn new() -> Self {
        Self {
            num_ack: RwLock::new(0),
            num_commands: RwLock::new(0),
        }
    }

    pub async fn get_num_ack(&self) -> usize {
        *self.num_ack.read().await
    }

    pub async fn get_num_commands(&self) -> usize {
        *self.num_commands.read().await
    }

    pub async fn incr_num_ack(&self) {
        *self.num_ack.write().await += 1;
    }

    pub async fn incr_num_commands(&self) {
        *self.num_commands.write().await += 1;
    }

    pub async fn reset(&self) {
        *self.num_ack.write().await = 0;
        *self.num_commands.write().await = 0;
    }
}

pub struct Instance {
    _version: [u8; 4],
    _metadata: HashMap<StringRecord, StringRecord>,
    databases: HashMap<usize, Store>,
    _checksum: [u8; 8],
}

struct InstanceBuilder {
    version: [u8; 4],
    metadata: HashMap<StringRecord, StringRecord>,
    databases: HashMap<usize, Store>,
    checksum: [u8; 8],
}

impl InstanceBuilder {
    fn new(version: [u8; 4]) -> Self {
        Self {
            version,
            metadata: HashMap::new(),
            databases: HashMap::new(),
            checksum: [0; 8],
        }
    }

    fn build(self) -> Instance {
        Instance {
            _version: self.version,
            _metadata: self.metadata,
            databases: self.databases,
            _checksum: self.checksum,
        }
    }
}

const MAGIC: &str = "REDIS";

impl Instance {
    pub fn new<T: Read>(mut buf: T) -> Result<Self> {
        Self::validate_magic(&mut buf)?;
        let version = Self::read_version(&mut buf)?;
        let mut builder = InstanceBuilder::new(version);
        Self::read_sections(&mut buf, &mut builder)?;
        Ok(builder.build())
    }

    fn validate_magic<T: Read>(buf: &mut T) -> Result<()> {
        let mut magic = [0u8; MAGIC.len()];
        buf.read_exact(&mut magic)?;
        anyhow::ensure!(
            magic == MAGIC.as_bytes(),
            "Invalid magic string: {}",
            String::from_utf8_lossy(&magic)
        );
        Ok(())
    }

    fn read_version<T: Read>(buf: &mut T) -> Result<[u8; 4]> {
        let mut version = [0u8; 4];
        buf.read_exact(&mut version)?;
        Ok(version)
    }

    fn read_sections<T: Read>(buf: &mut T, builder: &mut InstanceBuilder) -> Result<()> {
        loop {
            match buf.read_u8()?.into() {
                SectionId::Metadata => {
                    let name = read_string(buf)?;
                    let value = read_string(buf)?;
                    builder.metadata.insert(name, value);
                }
                SectionId::Database => {
                    let (index, store) = Self::read_database(buf)?;
                    builder.databases.insert(index, store);
                }
                SectionId::EndOfFile => {
                    buf.read_exact(&mut builder.checksum)?;
                    return Ok(());
                }
            }
        }
    }

    fn read_database<T: Read>(buf: &mut T) -> Result<(usize, Store)> {
        let index = read_numeric_length(buf)?;
        anyhow::ensure!(
            buf.read_u8()? == 0xFBu8,
            "Expected hash table size information (0xFB)"
        );

        let num_entries = read_numeric_length(buf)?;
        let _num_entries_with_expire = read_numeric_length(buf)?;

        let mut entries = HashMap::with_capacity(num_entries);
        for _ in 0..num_entries {
            let (key, value) = Self::read_entry(buf)?;
            entries.insert(key, value);
        }

        Ok((index, Store::new_with_entries(entries)))
    }

    fn read_entry<T: Read>(buf: &mut T) -> Result<(String, RedisData)> {
        let expires_on = match buf.read_u8()? {
            0 => None,
            0xFC => Some(UNIX_EPOCH + Duration::from_millis(buf.read_u64::<LittleEndian>()?)),
            0xFD => Some(UNIX_EPOCH + Duration::from_secs(buf.read_u32::<LittleEndian>()? as u64)),
            flag => anyhow::bail!("Invalid value flag: {}", flag),
        };

        if expires_on.is_some() {
            let _value_type = buf.read_u8()?;
        }

        let key = read_string(buf)?.to_string();
        let value = read_string(buf)?;
        Ok((key, RedisData::new(RecordType::String(value), expires_on)))
    }

    pub fn get_db(&self, index: usize) -> Result<&Store> {
        self.databases
            .get(&index)
            .ok_or_else(|| anyhow::anyhow!("Database not found"))
    }
}

enum SectionId {
    Metadata,
    Database,
    EndOfFile,
}

impl From<u8> for SectionId {
    fn from(value: u8) -> Self {
        match value {
            0xFA => SectionId::Metadata,
            0xFE => SectionId::Database,
            0xFF => SectionId::EndOfFile,
            _ => unreachable!(),
        }
    }
}

enum LengthValue {
    Length(usize),
    IntegerAsString8,
    IntegerAsString16,
    IntegerAsString32,
    CompressedString,
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub enum StringRecord {
    String(String),
    Integer(i64),
}

impl StringRecord {
    pub fn incr(&mut self) -> Result<i64> {
        match self {
            StringRecord::String(s) => {
                let mut i = s.parse::<i64>().map_err(|_| RedisError::InvalidInteger)?;
                i += 1;
                *s = i.to_string();
                Ok(i)
            }
            StringRecord::Integer(i) => {
                *i += 1;
                Ok(*i)
            }
        }
    }
}

impl fmt::Display for StringRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StringRecord::String(s) => write!(f, "{s}"),
            StringRecord::Integer(i) => write!(f, "{i}"),
        }
    }
}

impl<T: AsRef<str>> From<T> for StringRecord {
    fn from(value: T) -> Self {
        StringRecord::String(value.as_ref().to_string())
    }
}

fn read_string<T: Read>(buf: &mut T) -> Result<StringRecord> {
    match read_length(buf)? {
        LengthValue::Length(length) => {
            let mut bytes = vec![0u8; length];
            buf.read_exact(&mut bytes)?;
            Ok(StringRecord::String(String::from_utf8(bytes)?))
        }
        LengthValue::IntegerAsString8 => Ok(StringRecord::Integer(buf.read_u8()?.into())),
        LengthValue::IntegerAsString16 => Ok(StringRecord::Integer(
            buf.read_u16::<LittleEndian>()?.into(),
        )),
        LengthValue::IntegerAsString32 => Ok(StringRecord::Integer(
            buf.read_u32::<LittleEndian>()?.into(),
        )),
        LengthValue::CompressedString => {
            let clen = read_numeric_length(buf)?;
            let _ulen = read_numeric_length(buf)?;
            let mut bytes = vec![0u8; clen];
            buf.read_exact(&mut bytes)?;
            Ok(StringRecord::String(String::from_utf8(bytes)?))
        }
    }
}

fn read_length<T: Read>(buf: &mut T) -> Result<LengthValue> {
    let first_byte = buf.read_u8()?;
    let first_two_bits = first_byte >> 6;
    let remaining_six_bits = first_byte & 0b0011_1111;

    match first_two_bits {
        0b00 => Ok(LengthValue::Length(remaining_six_bits as usize)),
        0b01 => {
            let second_byte = buf.read_u8()?;
            Ok(LengthValue::Length(
                ((remaining_six_bits as usize) << 8) | second_byte as usize,
            ))
        }
        0b10 => Ok(LengthValue::Length(buf.read_u32::<BigEndian>()? as usize)),
        0b11 => match remaining_six_bits {
            0 => Ok(LengthValue::IntegerAsString8),
            1 => Ok(LengthValue::IntegerAsString16),
            2 => Ok(LengthValue::IntegerAsString32),
            3 => Ok(LengthValue::CompressedString),
            _ => anyhow::bail!("Invalid length type: {}", remaining_six_bits),
        },
        _ => unreachable!(),
    }
}

fn read_numeric_length<T: Read>(buf: &mut T) -> Result<usize> {
    match read_length(buf)? {
        LengthValue::Length(length) => Ok(length),
        _ => anyhow::bail!("Expected a numeric length, received special length"),
    }
}

#[derive(PartialEq)]
pub enum XReadBlockType {
    NoWait,
    Wait(Duration),
    WaitIndefinitely,
}

impl FromStr for XReadBlockType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "0" => Ok(XReadBlockType::WaitIndefinitely),
            s => {
                let ms = s.parse::<u64>()?;
                Ok(XReadBlockType::Wait(Duration::from_millis(ms)))
            }
        }
    }
}
