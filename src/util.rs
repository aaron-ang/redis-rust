use anyhow::Result;
use std::{
    collections::HashMap,
    time::{Duration, UNIX_EPOCH},
};
use strum::{Display, EnumString};
use tokio::{io::AsyncReadExt, sync::RwLock};

use crate::{db::RedisData, Store};

#[derive(Clone, Copy, PartialEq, Display, EnumString)]
pub enum Command {
    PING,
    ECHO,
    SET,
    GET,
    INFO,
    REPLCONF,
    PSYNC,
    WAIT,
    CONFIG,
    KEYS,
    TYPE,
}

#[derive(Clone, Copy, PartialEq, Display)]
pub enum ReplicaType {
    #[strum(serialize = "master")]
    Leader,
    #[strum(serialize = "slave")]
    Follower,
}

pub struct ReplicationState {
    num_ack: RwLock<usize>,
    prev_client_cmd: RwLock<Option<Command>>,
}

impl ReplicationState {
    pub fn new() -> Self {
        Self {
            num_ack: RwLock::new(0),
            prev_client_cmd: RwLock::new(None),
        }
    }

    pub async fn get_num_ack(&self) -> usize {
        *self.num_ack.read().await
    }

    pub async fn incr_num_ack(&self) {
        *self.num_ack.write().await += 1;
    }

    pub async fn reset(&self) {
        *self.num_ack.write().await = 0;
        *self.prev_client_cmd.write().await = None;
    }

    pub async fn get_prev_client_cmd(&self) -> Option<Command> {
        *self.prev_client_cmd.read().await
    }

    pub async fn set_prev_client_cmd(&self, cmd: Option<Command>) {
        *self.prev_client_cmd.write().await = cmd;
    }
}

pub struct Instance {
    _version: [u8; 4],
    _metadata: HashMap<StringValue, StringValue>,
    databases: HashMap<usize, Store>,
    _checksum: [u8; 8],
}

const MAGIC: &str = "REDIS";

impl Instance {
    pub async fn new<T: AsyncReadExt + Unpin>(mut buf: T) -> Result<Self> {
        Self::validate_magic(&mut buf).await?;
        let version = Self::read_version(&mut buf).await?;
        let (metadata, databases, checksum) = Self::read_sections(&mut buf).await?;

        Ok(Instance {
            _version: version,
            _metadata: metadata,
            databases,
            _checksum: checksum,
        })
    }

    async fn validate_magic<T: AsyncReadExt + Unpin>(buf: &mut T) -> Result<()> {
        let mut magic = [0u8; MAGIC.len()];
        buf.read_exact(&mut magic).await?;
        anyhow::ensure!(
            magic == MAGIC.as_bytes(),
            "Invalid magic string: {}",
            String::from_utf8_lossy(&magic)
        );
        Ok(())
    }

    async fn read_version<T: AsyncReadExt + Unpin>(buf: &mut T) -> Result<[u8; 4]> {
        let mut version = [0u8; 4];
        buf.read_exact(&mut version).await?;
        Ok(version)
    }

    async fn read_sections<T: AsyncReadExt + Unpin>(
        buf: &mut T,
    ) -> Result<(
        HashMap<StringValue, StringValue>,
        HashMap<usize, Store>,
        [u8; 8],
    )> {
        let mut metadata = HashMap::new();
        let mut databases = HashMap::new();

        loop {
            match buf.read_u8().await?.into() {
                SectionId::Metadata => {
                    let name = read_string(buf).await?;
                    let value = read_string(buf).await?;
                    metadata.insert(name, value);
                }
                SectionId::Database => {
                    let (index, store) = Self::read_database(buf).await?;
                    databases.insert(index, store);
                }
                SectionId::EndOfFile => {
                    let mut checksum = [0u8; 8];
                    buf.read_exact(&mut checksum).await?;
                    return Ok((metadata, databases, checksum));
                }
            }
        }
    }

    async fn read_database<T: AsyncReadExt + Unpin>(buf: &mut T) -> Result<(usize, Store)> {
        let index = read_numeric_length(buf).await?;
        anyhow::ensure!(
            buf.read_u8().await? == 0xFBu8,
            "Expected hash table size information (0xFB)"
        );

        let num_entries = read_numeric_length(buf).await?;
        let _num_entries_with_expire = read_numeric_length(buf).await?;

        let mut entries = HashMap::with_capacity(num_entries);
        for _ in 0..num_entries {
            let (key, value) = Self::read_entry(buf).await?;
            entries.insert(key, value);
        }

        Ok((index, Store::new_with_entries(entries)))
    }

    async fn read_entry<T: AsyncReadExt + Unpin>(buf: &mut T) -> Result<(String, RedisData)> {
        let expires_on = match buf.read_u8().await? {
            0 => None,
            0xFC => Some(UNIX_EPOCH + Duration::from_millis(buf.read_u64_le().await?)),
            0xFD => Some(UNIX_EPOCH + Duration::from_secs(buf.read_u32_le().await? as u64)),
            flag => anyhow::bail!("Invalid value flag: {}", flag),
        };

        if expires_on.is_some() {
            let _value_type = buf.read_u8().await?;
        }

        let key = read_string(buf).await?.to_string();
        let value = read_string(buf).await?.to_string();
        Ok((key, RedisData::new(value, expires_on)))
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

#[derive(Hash, PartialEq, Eq)]
enum StringValue {
    String(String),
    Integer(u32),
}

impl ToString for StringValue {
    fn to_string(&self) -> String {
        match self {
            StringValue::String(s) => s.clone(),
            StringValue::Integer(i) => i.to_string(),
        }
    }
}

async fn read_string<T: AsyncReadExt + Unpin>(buf: &mut T) -> Result<StringValue> {
    match read_length(buf).await? {
        LengthValue::Length(length) => {
            let mut bytes = vec![0u8; length];
            buf.read_exact(&mut bytes).await?;
            Ok(StringValue::String(String::from_utf8(bytes)?))
        }
        LengthValue::IntegerAsString8 => Ok(StringValue::Integer(buf.read_u8().await? as u32)),
        LengthValue::IntegerAsString16 => Ok(StringValue::Integer(buf.read_u16_le().await? as u32)),
        LengthValue::IntegerAsString32 => Ok(StringValue::Integer(buf.read_u32_le().await?)),
        LengthValue::CompressedString => {
            let clen = read_numeric_length(buf).await?;
            let _ulen = read_numeric_length(buf).await?;
            let mut bytes = vec![0u8; clen];
            buf.read_exact(&mut bytes).await?;
            Ok(StringValue::String(String::from_utf8(bytes)?))
        }
    }
}

async fn read_length<T: AsyncReadExt + Unpin>(buf: &mut T) -> Result<LengthValue> {
    let first_byte = buf.read_u8().await?;
    let first_two_bits = first_byte >> 6;
    let remaining_six_bits = first_byte & 0b111111;

    match first_two_bits {
        0b00 => Ok(LengthValue::Length(remaining_six_bits.into())),
        0b01 => {
            let second_byte = buf.read_u8().await?;
            Ok(LengthValue::Length(
                (remaining_six_bits << 8 | second_byte).into(),
            ))
        }
        0b10 => Ok(LengthValue::Length(buf.read_u32().await? as usize)),
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

async fn read_numeric_length<T: AsyncReadExt + Unpin>(buf: &mut T) -> Result<usize> {
    match read_length(buf).await? {
        LengthValue::Length(length) => Ok(length),
        _ => anyhow::bail!("Expected a numeric length, received special length"),
    }
}
