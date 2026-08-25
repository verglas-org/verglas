//! Cache keys and immutable origin metadata for the one-node Foyer cache.
//! Block keys carry the object identity, ETag, geometry, and block index, so
//! recovered bytes can only match the version they were fetched from.

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::time::SystemTime;

use foyer::{Code, Error as FoyerError, Result as FoyerResult};
use verglas_core::read::ObjectMeta;
use verglas_core::{BlockKey, CacheKey};

/// Key of one immutable block in the hybrid cache. The wrapper exists because
/// the Foyer codec is implemented in this crate for the core [`BlockKey`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BlockEntryKey {
    /// The logical block identity (object + ETag + block index).
    pub block: BlockKey,
}

/// Whole-object metadata as cached from an origin response — the value type
/// of the metadata cache.
///
/// Unlike [`ObjectMeta`], the ETag is non-optional: metadata is only ever
/// admitted for cacheable objects (origin reported an ETag). The type makes
/// the "no admission without an ETag" rule unrepresentable to violate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CachedMeta {
    /// Total object size in bytes.
    pub size: u64,
    /// Entity tag of this object version, exactly as the origin reported it.
    pub etag: String,
    /// Last modification time, when the origin reported one.
    pub last_modified: Option<SystemTime>,
    /// MIME type stored with the object, when the origin reported one.
    pub content_type: Option<String>,
    /// The system headers and user metadata (`x-amz-meta-*`) the origin
    /// reported (issue #143), carried so a cache-hit GET/HEAD reports the same
    /// headers a cold read would — the parity the write path now depends on.
    /// Boxed to keep [`CachedMeta`] small: it rides inside the engine's fill
    /// enums next to a data block, where a fat inline struct would trip the
    /// large-enum-variant lint (and bloat the common no-metadata case).
    pub headers: Box<ObjectHeaders>,
}

/// The extra whole-object metadata beyond size/etag/mtime/content-type: the S3
/// system headers and the user metadata map. Grouped so [`CachedMeta`] carries
/// it as one field and the conversions stay legible (issue #143).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ObjectHeaders {
    /// Caching directive (`Cache-Control`).
    pub cache_control: Option<String>,
    /// Presentation hint (`Content-Disposition`).
    pub content_disposition: Option<String>,
    /// Content codings applied to the body (`Content-Encoding`).
    pub content_encoding: Option<String>,
    /// Natural language of the body (`Content-Language`).
    pub content_language: Option<String>,
    /// Expiry directive (`Expires`), issue #189 — the raw HTTP-date string, so a
    /// cache-hit read reports the same `Expires` a cold read would.
    pub expires: Option<String>,
    /// User-defined metadata (`x-amz-meta-<key>`), keys without the prefix.
    pub user_metadata: BTreeMap<String, String>,
}

impl ObjectHeaders {
    /// Approximate DRAM footprint of the header strings and metadata map, for
    /// the metadata cache's weighter.
    fn weight(&self) -> usize {
        let opt = |o: &Option<String>| o.as_ref().map_or(0, String::len);
        opt(&self.cache_control)
            + opt(&self.content_disposition)
            + opt(&self.content_encoding)
            + opt(&self.content_language)
            + opt(&self.expires)
            + self
                .user_metadata
                .iter()
                .map(|(k, v)| k.len() + v.len())
                .sum::<usize>()
    }
}

impl CachedMeta {
    /// Builds cacheable metadata from an origin response, or `None` when the
    /// response carries no ETag — the caller must then serve passthrough
    /// without admitting anything.
    pub fn from_object_meta(meta: &ObjectMeta) -> Option<Self> {
        Some(CachedMeta {
            size: meta.size,
            etag: meta.e_tag.clone()?,
            last_modified: meta.last_modified,
            content_type: meta.content_type.clone(),
            headers: Box::new(ObjectHeaders {
                cache_control: meta.cache_control.clone(),
                content_disposition: meta.content_disposition.clone(),
                content_encoding: meta.content_encoding.clone(),
                content_language: meta.content_language.clone(),
                expires: meta.expires.clone(),
                user_metadata: meta.user_metadata.clone(),
            }),
        })
    }

    /// Converts back to the interface's metadata form for GET/HEAD responses,
    /// keeping HEAD/GET header parity byte-for-byte.
    pub fn to_object_meta(&self) -> ObjectMeta {
        ObjectMeta {
            size: self.size,
            e_tag: Some(self.etag.clone()),
            last_modified: self.last_modified,
            content_type: self.content_type.clone(),
            cache_control: self.headers.cache_control.clone(),
            content_disposition: self.headers.content_disposition.clone(),
            content_encoding: self.headers.content_encoding.clone(),
            content_language: self.headers.content_language.clone(),
            expires: self.headers.expires.clone(),
            user_metadata: self.headers.user_metadata.clone(),
        }
    }

    /// Approximate DRAM footprint of one mapping entry, charged by the
    /// metadata cache's weighter: the strings plus the fixed-size fields.
    pub fn weight(&self, key: &CacheKey) -> usize {
        key.bucket.len()
            + key.key.len()
            + self.etag.len()
            + self.content_type.as_ref().map_or(0, String::len)
            + self.headers.weight()
            + std::mem::size_of::<CachedMeta>()
    }
}

/// Writes a u64-length-prefixed byte slice.
pub(crate) fn write_bytes(writer: &mut impl Write, bytes: &[u8]) -> FoyerResult<()> {
    writer
        .write_all(&(bytes.len() as u64).to_le_bytes())
        .and_then(|()| writer.write_all(bytes))
        .map_err(FoyerError::io_error)
}

/// Reads a length-prefixed UTF-8 string written by [`write_bytes`]. Trusts
/// the local disk's length prefix (entries are checksummed by foyer).
pub(crate) fn read_string(reader: &mut impl Read) -> FoyerResult<String> {
    let mut len = [0u8; 8];
    reader.read_exact(&mut len).map_err(FoyerError::io_error)?;
    let mut buf = vec![0u8; u64::from_le_bytes(len) as usize];
    reader.read_exact(&mut buf).map_err(FoyerError::io_error)?;
    String::from_utf8(buf)
        .map_err(|e| FoyerError::io_error(std::io::Error::new(std::io::ErrorKind::InvalidData, e)))
}

impl Code for BlockEntryKey {
    /// Encodes the logical object, ETag, geometry, and block index using
    /// length-prefixed strings and little-endian integers.
    fn encode(&self, writer: &mut impl Write) -> FoyerResult<()> {
        write_bytes(writer, self.block.object.storage_binding_id.as_bytes())?;
        write_bytes(writer, self.block.object.bucket.as_bytes())?;
        write_bytes(writer, self.block.object.key.as_bytes())?;
        write_bytes(writer, self.block.etag.as_bytes())?;
        writer
            .write_all(&self.block.block_bytes.to_le_bytes())
            .map_err(FoyerError::io_error)?;
        writer
            .write_all(&self.block.block_index.to_le_bytes())
            .map_err(FoyerError::io_error)
    }

    /// Decodes what [`Code::encode`] wrote; used by foyer's disk recovery to
    /// rebuild the index, so it must be a faithful inverse.
    fn decode(reader: &mut impl Read) -> FoyerResult<Self> {
        let storage_binding_id = read_string(reader)?;
        let bucket = read_string(reader)?;
        let key = read_string(reader)?;
        let etag = read_string(reader)?;
        let mut block_bytes = [0u8; 8];
        reader
            .read_exact(&mut block_bytes)
            .map_err(FoyerError::io_error)?;
        let mut index = [0u8; 8];
        reader
            .read_exact(&mut index)
            .map_err(FoyerError::io_error)?;
        Ok(BlockEntryKey {
            block: BlockKey {
                object: CacheKey {
                    storage_binding_id,
                    bucket,
                    key,
                },
                etag,
                block_bytes: u64::from_le_bytes(block_bytes),
                block_index: u64::from_le_bytes(index),
            },
        })
    }

    /// Returns the serialized key size used by Foyer's storage accounting.
    fn estimated_size(&self) -> usize {
        8 + self.block.object.storage_binding_id.len()
            + 8
            + self.block.object.bucket.len()
            + 8
            + self.block.object.key.len()
            + 8
            + self.block.etag.len()
            + 8
            + 8
    }
}
