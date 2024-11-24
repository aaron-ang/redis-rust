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
}

#[derive(Clone, Copy, PartialEq, Display)]
pub enum ReplicaType {
    #[strum(serialize = "master")]
    Leader,
    #[strum(serialize = "slave")]
    Follower,
}

pub struct ReplicationState {
    inner: RwLock<InnerReplicationState>,
}

struct InnerReplicationState {
    num_ack: usize,
    prev_client_cmd: Option<Command>,
}

impl ReplicationState {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(InnerReplicationState {
                num_ack: 0,
                prev_client_cmd: None,
            }),
        }
    }

    pub async fn get_num_ack(&self) -> usize {
        self.inner.read().await.num_ack
    }

    pub async fn incr_num_ack(&self) {
        self.inner.write().await.num_ack += 1;
    }

    pub async fn reset(&self) {
        let mut state = self.inner.write().await;
        state.num_ack = 0;
        state.prev_client_cmd = None;
    }

    pub async fn get_prev_client_cmd(&self) -> Option<Command> {
        self.inner.read().await.prev_client_cmd
    }

    pub async fn set_prev_client_cmd(&self, cmd: Option<Command>) {
        self.inner.write().await.prev_client_cmd = cmd;
    }
}

pub struct Instance {
    _version: [u8; 4],
    _metadata: HashMap<StringValue, StringValue>,
    pub dbs: HashMap<usize, Store>,
    _checksum: [u8; 8],
}

const MAGIC: &str = "REDIS";

impl Instance {
    pub async fn new<T: AsyncReadExt + Unpin>(mut buf: T) -> Result<Self> {
        let mut magic = [0u8; MAGIC.len()];
        buf.read_exact(&mut magic).await?;

        anyhow::ensure!(
            magic == MAGIC.as_bytes(),
            "Invalid magic string: {}",
            String::from_utf8_lossy(&magic)
        );

        let mut version = [0u8; 4];
        buf.read_exact(&mut version).await?;

        let mut metadata: HashMap<StringValue, StringValue> = HashMap::new();
        let mut dbs = HashMap::new();

        loop {
            let id = buf.read_u8().await?.into();
            match id {
                SectionId::Metadata => {
                    println!("Reading metadata");
                    let name = read_string(&mut buf).await?;
                    let value = read_string(&mut buf).await?;
                    metadata.insert(name, value);
                }
                SectionId::Database => {
                    println!("Reading database");
                    let index = read_numeric_length(&mut buf).await?;
                    anyhow::ensure!(
                        buf.read_u8().await? == 0xFBu8,
                        "Expected hash table size information (0xFB)"
                    );
                    let num_entries = read_numeric_length(&mut buf).await?;
                    let _num_entries_with_expire = read_numeric_length(&mut buf).await?;
                    let mut entries = HashMap::new();

                    for _ in 0..num_entries {
                        let mut expires_on = None;
                        let type_flag = buf.read_u8().await?;
                        match type_flag {
                            0 => {}
                            0xFC => {
                                let expire_ms = buf.read_u64_le().await?;
                                expires_on = Some(UNIX_EPOCH + Duration::from_millis(expire_ms));
                            }
                            0xFD => {
                                let expire_sec = buf.read_u32_le().await?;
                                expires_on =
                                    Some(UNIX_EPOCH + Duration::from_secs(expire_sec as u64));
                            }
                            _ => anyhow::bail!("Invalid type flag: {}", type_flag),
                        }

                        if expires_on.is_some() {
                            let _value_type = buf.read_u8().await?;
                        }

                        let key = read_string(&mut buf).await?;
                        let value = read_string(&mut buf).await?;

                        entries.insert(
                            key.to_string(),
                            RedisData::new(value.to_string(), expires_on),
                        );
                    }

                    dbs.insert(index, Store::new_with_entries(entries));
                }
                SectionId::EndOfFile => {
                    println!("End of file");
                    let mut checksum = [0u8; 8];
                    buf.read_exact(&mut checksum).await?;
                    return Ok(Instance {
                        _version: version,
                        _metadata: metadata,
                        dbs,
                        _checksum: checksum,
                    });
                }
            }
        }
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
            let mut bytes = vec![0u8; length as usize];
            buf.read_exact(&mut bytes).await?;
            Ok(StringValue::String(String::from_utf8(bytes)?))
        }
        LengthValue::IntegerAsString8 => Ok(StringValue::Integer(buf.read_u8().await? as u32)),
        LengthValue::IntegerAsString16 => Ok(StringValue::Integer(buf.read_u16_le().await? as u32)),
        LengthValue::IntegerAsString32 => Ok(StringValue::Integer(buf.read_u32_le().await?)),
        LengthValue::CompressedString => {
            let clen = read_numeric_length(buf).await?;
            let _ulen = read_numeric_length(buf).await?;
            let mut bytes = vec![0u8; clen as usize];
            buf.read_exact(&mut bytes).await?;
            Ok(StringValue::String(String::from_utf8(bytes)?))
        }
    }
}

async fn read_length<T: AsyncReadExt + Unpin>(buf: &mut T) -> Result<LengthValue> {
    let first_byte = buf.read_u8().await?;
    let first_two_bits = first_byte >> 6;
    let remaining_six_bits = first_byte & 0b111111;
    let size = match first_two_bits {
        0b00 => LengthValue::Length(remaining_six_bits.into()),
        0b01 => {
            let second_byte = buf.read_u8().await?;
            LengthValue::Length((remaining_six_bits << 8 | second_byte).into())
        }
        0b10 => LengthValue::Length(buf.read_u32().await? as usize),
        0b11 => match remaining_six_bits {
            0 => LengthValue::IntegerAsString8,
            1 => LengthValue::IntegerAsString16,
            2 => LengthValue::IntegerAsString32,
            3 => LengthValue::CompressedString,
            _ => anyhow::bail!("Invalid length type: {}", remaining_six_bits),
        },
        _ => unreachable!(),
    };
    Ok(size)
}

async fn read_numeric_length<T: AsyncReadExt + Unpin>(buf: &mut T) -> Result<usize> {
    if let LengthValue::Length(length) = read_length(buf).await? {
        Ok(length)
    } else {
        anyhow::bail!("Expected a numeric length, received special length")
    }
}
