//! Lossless caller-key mapping for an `i64` Vamana index.
//!
//! S3 Vectors accepts arbitrary string keys while Vamana deliberately stores
//! compact integers. This map allocates collision-free integer handles and is
//! serialized into the same snapshot-bound Puffin statistics file as the ANN
//! graph; it is never an authoritative process-local registry.

use std::collections::BTreeMap;

use crate::error::{Result, VectorError};

const MAGIC: [u8; 8] = *b"VGKEYS\x01\0";

/// A bidirectional, lossless mapping between caller string keys and Vamana ids.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StringKeyMap {
    /// The next monotonically allocated id. Deleted ids are not reused, which
    /// avoids a stale ANN slot ever being interpreted as a new key.
    next_id: i64,
    /// Exact UTF-8 key to Vamana id.
    by_key: BTreeMap<String, i64>,
    /// Vamana id to exact UTF-8 key.
    by_id: BTreeMap<i64, String>,
}

impl Default for StringKeyMap {
    /// Creates an empty mapping with the first positive i64 handle.
    fn default() -> Self {
        Self {
            next_id: 1,
            by_key: BTreeMap::new(),
            by_id: BTreeMap::new(),
        }
    }
}

impl StringKeyMap {
    /// Returns an existing id for `key`, or allocates one that cannot collide.
    pub fn id_for_put(&mut self, key: &str) -> Result<i64> {
        if let Some(id) = self.by_key.get(key) {
            return Ok(*id);
        }
        let id = self.next_id;
        self.next_id = self
            .next_id
            .checked_add(1)
            .ok_or(VectorError::KeySpaceExhausted)?;
        self.by_key.insert(key.to_owned(), id);
        self.by_id.insert(id, key.to_owned());
        Ok(id)
    }

    /// Removes `key` and returns its retired Vamana id, if it was mapped.
    pub fn remove(&mut self, key: &str) -> Option<i64> {
        let id = self.by_key.remove(key)?;
        self.by_id.remove(&id);
        Some(id)
    }

    /// Returns the id currently assigned to `key` without changing the map.
    pub fn id_for(&self, key: &str) -> Option<i64> {
        self.by_key.get(key).copied()
    }

    /// Returns the exact caller key for an ANN id.
    pub fn key_for(&self, id: i64) -> Option<&str> {
        self.by_id.get(&id).map(String::as_str)
    }

    /// Returns the durable map's number of live key entries.
    pub fn len(&self) -> usize {
        self.by_key.len()
    }

    /// Reports whether the map contains no live key entries.
    pub fn is_empty(&self) -> bool {
        self.by_key.is_empty()
    }

    /// Serializes exact keys and allocator state. The bytes are Puffin blob data.
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&MAGIC);
        bytes.extend_from_slice(&self.next_id.to_le_bytes());
        bytes.extend_from_slice(&(self.by_id.len() as u64).to_le_bytes());
        for (id, key) in &self.by_id {
            let key_bytes = key.as_bytes();
            let len = u32::try_from(key_bytes.len()).map_err(|_| VectorError::KeyTooLong)?;
            bytes.extend_from_slice(&id.to_le_bytes());
            bytes.extend_from_slice(&len.to_le_bytes());
            bytes.extend_from_slice(key_bytes);
        }
        Ok(bytes)
    }

    /// Decodes and validates persisted map bytes without accepting aliases.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut cursor = Cursor { bytes, position: 0 };
        if cursor.take(8)? != MAGIC {
            return Err(VectorError::CorruptBlob(
                "bad string-key map magic".to_owned(),
            ));
        }
        let next_id = cursor.i64()?;
        if next_id <= 0 {
            return Err(VectorError::CorruptBlob(
                "invalid string-key next id".to_owned(),
            ));
        }
        let count = cursor.u64()? as usize;
        let mut by_key = BTreeMap::new();
        let mut by_id = BTreeMap::new();
        for _ in 0..count {
            let id = cursor.i64()?;
            if id <= 0 || id >= next_id {
                return Err(VectorError::CorruptBlob("invalid string-key id".to_owned()));
            }
            let len = cursor.u32()? as usize;
            let key = std::str::from_utf8(cursor.take(len)?)
                .map_err(|error| VectorError::CorruptBlob(format!("invalid UTF-8 key: {error}")))?
                .to_owned();
            if by_key.insert(key.clone(), id).is_some() || by_id.insert(id, key).is_some() {
                return Err(VectorError::CorruptBlob(
                    "duplicate string-key map entry".to_owned(),
                ));
            }
        }
        if !cursor.done() {
            return Err(VectorError::CorruptBlob(
                "trailing string-key map bytes".to_owned(),
            ));
        }
        Ok(Self {
            next_id,
            by_key,
            by_id,
        })
    }
}

/// A checked byte cursor used only by the durable map decoder.
struct Cursor<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Cursor<'a> {
    /// Returns the next `length` bytes or a corruption error.
    fn take(&mut self, length: usize) -> Result<&'a [u8]> {
        let end = self
            .position
            .checked_add(length)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(|| VectorError::CorruptBlob("truncated string-key map".to_owned()))?;
        let result = &self.bytes[self.position..end];
        self.position = end;
        Ok(result)
    }
    /// Decodes a little-endian i64.
    fn i64(&mut self) -> Result<i64> {
        Ok(i64::from_le_bytes(self.take(8)?.try_into().map_err(
            |_| VectorError::CorruptBlob("bad i64".to_owned()),
        )?))
    }
    /// Decodes a little-endian u32.
    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().map_err(
            |_| VectorError::CorruptBlob("bad u32".to_owned()),
        )?))
    }
    /// Decodes a little-endian u64.
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().map_err(
            |_| VectorError::CorruptBlob("bad u64".to_owned()),
        )?))
    }
    /// Reports whether all persisted bytes were consumed.
    fn done(&self) -> bool {
        self.position == self.bytes.len()
    }
}
