//! The `MetadataFetch` trait: how the mapper reads manifests and Parquet
//! footers without doing IO itself.
//!
//! The mapper never talks to object storage directly. It reads every manifest
//! list, manifest, and footer through [`MetadataFetch`], so the same parsing
//! code runs two ways: in the server it reads *through the cache* (an
//! [`ObjectRead`] implementation, so metadata fetches populate the metadata
//! store — reading through the cache itself, #50), and offline it reads through
//! a plain `object_store` client. This one trait is what makes the whole
//! Iceberg layer reusable offline.

use std::ops::Range;

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use object_store::{GetOptions, GetRange, ObjectStore, path::Path as StorePath};
use verglas_core::CacheKey;
use verglas_core::read::{ObjectRead, ReadError, ReadRange};

/// A physical object location: origin bucket plus object key. Structurally the
/// same identity as [`CacheKey`]; a distinct alias keeps the metadata-IO interface
/// readable at call sites (a footer read is an `ObjectPath`, not a cache key).
pub type ObjectPath = CacheKey;

/// Errors from a metadata fetch. The mapper degrades on these (it never
/// fabricates bytes): a failed footer read just leaves column chunks
/// unresolved, a failed manifest read leaves that table's index stale.
#[derive(Debug, thiserror::Error)]
pub enum FetchError {
    /// The object does not exist at that path.
    #[error("object not found: {0}")]
    NotFound(String),
    /// The backing store or cache failed to produce the bytes.
    #[error("metadata fetch failed for {path}: {detail}")]
    Backend {
        /// The object key the fetch targeted.
        path: String,
        /// Underlying error detail, for logs.
        detail: String,
    },
}

/// Reads byte ranges of metadata objects (manifests, footers). Range-aware and
/// suffix-aware because a Parquet footer is read from the file tail.
///
/// Implementations are only ever asked for *small* ranges — footers and
/// manifests, never data-file bodies — so returning `Bytes` (buffered) is
/// correct here even though the data read path streams.
#[async_trait]
pub trait MetadataFetch: Send + Sync {
    /// Reads the half-open byte range `range` of the object at `path`.
    async fn fetch_range(&self, path: &ObjectPath, range: Range<u64>) -> Result<Bytes, FetchError>;

    /// Reads the last `suffix_len` bytes of the object at `path` (footer reads).
    async fn fetch_suffix(&self, path: &ObjectPath, suffix_len: u64) -> Result<Bytes, FetchError>;
}

/// Offline `MetadataFetch` over an `object_store` client: tests and benches
/// read metadata straight from the bucket through this. The store is
/// bucket-scoped, so `path.bucket` is informational — the object key drives the
/// read.
pub struct ObjectStoreFetch {
    /// The bucket-scoped object store to read from.
    store: std::sync::Arc<dyn ObjectStore>,
}

impl ObjectStoreFetch {
    /// Wraps a bucket-scoped object store as a metadata fetcher.
    pub fn new(store: std::sync::Arc<dyn ObjectStore>) -> ObjectStoreFetch {
        ObjectStoreFetch { store }
    }

    /// Runs a ranged `get_opts` and buffers the result. Shared by the two
    /// trait methods, which differ only in the `GetRange` they pass.
    async fn get(&self, path: &ObjectPath, range: GetRange) -> Result<Bytes, FetchError> {
        let location = StorePath::from(path.key.as_str());
        let options = GetOptions {
            range: Some(range),
            ..GetOptions::default()
        };
        let result = self
            .store
            .get_opts(&location, options)
            .await
            .map_err(|error| map_store_error(&path.key, error))?;
        result
            .bytes()
            .await
            .map_err(|error| map_store_error(&path.key, error))
    }
}

/// Translates an `object_store` error into a [`FetchError`], preserving the
/// not-found distinction the mapper acts on.
fn map_store_error(key: &str, error: object_store::Error) -> FetchError {
    match error {
        object_store::Error::NotFound { .. } => FetchError::NotFound(key.to_owned()),
        other => FetchError::Backend {
            path: key.to_owned(),
            detail: other.to_string(),
        },
    }
}

#[async_trait]
impl MetadataFetch for ObjectStoreFetch {
    /// Reads `range` via `get_opts` with a bounded range.
    async fn fetch_range(&self, path: &ObjectPath, range: Range<u64>) -> Result<Bytes, FetchError> {
        self.get(path, GetRange::Bounded(range)).await
    }

    /// Reads the tail via `get_opts` with a suffix range.
    async fn fetch_suffix(&self, path: &ObjectPath, suffix_len: u64) -> Result<Bytes, FetchError> {
        self.get(path, GetRange::Suffix(suffix_len)).await
    }
}

/// Server `MetadataFetch` that reads *through the cache*: it wraps any
/// [`ObjectRead`] (the cache engine's, #12), so every manifest/footer fetch
/// flows through the same fill path as data and lands in the metadata store
/// (#50). This is where we read through the cache ourselves — metadata is cached like everything
/// else.
///
/// Integration note (#50): the metadata store pins these blobs with a different
/// admission policy than data; that pinning lives in the cache engine behind
/// `ObjectRead`, not here.
pub struct ObjectReadFetch<R: ObjectRead> {
    /// The cache-backed read trait metadata flows through.
    reader: R,
}

impl<R: ObjectRead> ObjectReadFetch<R> {
    /// Wraps a cache-backed `ObjectRead` as a metadata fetcher.
    pub fn new(reader: R) -> ObjectReadFetch<R> {
        ObjectReadFetch { reader }
    }

    /// Collects an `ObjectRead` body stream into one buffer. Sound here
    /// because metadata objects are small (footers/manifests), never
    /// multi-GB data bodies — this must never be used on the data path.
    async fn collect(&self, path: &ObjectPath, range: ReadRange) -> Result<Bytes, FetchError> {
        let got = self
            .reader
            .get(path, range)
            .await
            .map_err(|error| map_read_error(&path.key, error))?;
        let mut body = got.body;
        let mut buffer = bytes::BytesMut::new();
        while let Some(chunk) = body.next().await {
            let chunk = chunk.map_err(|error| map_read_error(&path.key, error))?;
            buffer.extend_from_slice(&chunk);
        }
        Ok(buffer.freeze())
    }
}

/// Translates a cache/read-path [`ReadError`] into a [`FetchError`].
fn map_read_error(key: &str, error: ReadError) -> FetchError {
    match error {
        ReadError::NoSuchKey | ReadError::NoSuchBucket => FetchError::NotFound(key.to_owned()),
        other => FetchError::Backend {
            path: key.to_owned(),
            detail: other.to_string(),
        },
    }
}

#[async_trait]
impl<R: ObjectRead> MetadataFetch for ObjectReadFetch<R> {
    /// Reads `range` through the cache as a bounded read.
    async fn fetch_range(&self, path: &ObjectPath, range: Range<u64>) -> Result<Bytes, FetchError> {
        // `ReadRange::Bounded` is inclusive on both ends; `range` is half-open.
        if range.end <= range.start {
            return Ok(Bytes::new());
        }
        self.collect(path, ReadRange::Bounded(range.start, range.end - 1))
            .await
    }

    /// Reads the tail through the cache as a suffix read.
    async fn fetch_suffix(&self, path: &ObjectPath, suffix_len: u64) -> Result<Bytes, FetchError> {
        self.collect(path, ReadRange::Suffix(suffix_len)).await
    }
}
