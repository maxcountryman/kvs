use std::convert::TryInto;
use std::io::{Read, Seek, Write};

use crc32fast::Hasher;

use crate::Result;

/// The size of the entry's prefix in bytes.
pub const PREFIX_SIZE: usize = 12;

type Value = Option<String>;

/// An entry in the log which represents adding or removing keys and values.
///
/// Entries hold onto a CRC32 of their contents. This is important because
/// during deserialization we must check for data corruption. To do so, we
/// first read the CRC prefix and later use this to verify the read data.
#[derive(Clone, Debug)]
pub struct Entry {
    /// The key of the entry.
    pub key: String,
    /// The value of the entry.
    pub value: Value,
    crc32: u32,
    key_size: u32,
    value_size: u32,
}

impl Entry {
    /// Create an set entry for a key-value pair.
    ///
    /// # Examples
    ///
    /// ```
    /// use kvs::Entry;
    ///
    /// let entry = Entry::set("foo", "bar");
    /// ```
    pub fn set(key: impl Into<String>, value: impl Into<String>) -> Self {
        Entry::new(key.into(), Some(value.into()))
    }

    /// Create an removal entry for a key.
    ///
    /// # Examples
    ///
    /// ```
    /// use kvs::Entry;
    ///
    /// let entry = Entry::remove("foo");
    /// ```
    pub fn remove(key: impl Into<String>) -> Self {
        // `None` serves as our tombstone value.
        Entry::new(key.into(), None)
    }

    /// Returns a byte buffer of the entry's properties, with the CRC32
    /// occupying the first 4 bytes.
    pub fn as_durable_bytes(&self) -> Vec<u8> {
        let mut byte_buf = vec![];
        byte_buf.extend_from_slice(&self.crc32.to_be_bytes());
        byte_buf.extend_from_slice(&self.as_bytes());
        byte_buf.to_vec()
    }

    /// Returns a byte buffer of the entry's properties, without the CRC32.
    fn as_bytes(&self) -> Vec<u8> {
        as_bytes(self.key_size, self.value_size, &self.key, &self.value)
    }

    fn new(key: String, value: Value) -> Self {
        let key_size = key.len() as u32;

        let mut value_size = 0;
        if let Some(ref v) = value {
            value_size = v.len() as u32;
        }

        let crc32 = generate_crc32(key_size, value_size, &key, &value);

        Self {
            key,
            value,
            crc32,
            key_size,
            value_size,
        }
    }
}

fn generate_crc32(key_size: u32, value_size: u32, key: &str, value: &Value) -> u32 {
    let mut crc_hasher = Hasher::new();
    crc_hasher.update(&as_bytes(key_size, value_size, &key, &value));
    crc_hasher.finalize()
}

fn as_bytes(key_size: u32, value_size: u32, key: &str, value: &Value) -> Vec<u8> {
    let mut byte_buf = vec![];

    byte_buf.extend_from_slice(&key_size.to_ne_bytes());
    byte_buf.extend_from_slice(&value_size.to_ne_bytes());
    byte_buf.extend_from_slice(&key.as_bytes());

    let mut value_bytes: &[u8] = &[];
    if let Some(ref v) = value {
        value_bytes = v.as_bytes();
    }

    byte_buf.extend_from_slice(value_bytes);

    byte_buf
}

/// Write Entry to given writer.
pub fn to_writer<W>(writer: &mut W, entry: &Entry) -> Result<()>
where
    W: Write + Seek,
{
    writer.write_all(&entry.as_durable_bytes())?;
    Ok(())
}

/// Read to a new Entry from given reader.
pub fn from_reader(reader: &mut dyn Read) -> Result<Entry> {
    let mut prefix_bytes = [0; PREFIX_SIZE];
    reader.read_exact(&mut prefix_bytes)?;

    let crc32 = u32::from_be_bytes(prefix_bytes[..4].try_into()?);
    let key_size = u32::from_ne_bytes(prefix_bytes[4..8].try_into()?);
    let value_size = u32::from_ne_bytes(prefix_bytes[8..PREFIX_SIZE].try_into()?);

    let mut bytes: Vec<u8> = vec![0; (key_size + value_size) as usize];
    reader.read_exact(&mut bytes)?;

    let key_offset = key_size as usize;
    let value_offset = key_offset + value_size as usize;
    let key = String::from_utf8(bytes[..key_offset].to_vec())?;
    let value = String::from_utf8(bytes[key_offset..value_offset].to_vec())?;

    let value: Value = match value.len() {
        0 => None,
        _ => Some(value),
    };

    assert_eq!(crc32, generate_crc32(key_size, value_size, &key, &value));

    Ok(Entry {
        crc32,
        key,
        key_size,
        value,
        value_size,
    })
}
