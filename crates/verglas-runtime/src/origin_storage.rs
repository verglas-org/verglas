//! Host-owned Iceberg storage backed by the configured origin and Foyer.
//!
//! The factory fixes one storage binding, bucket, and URI scheme. Its storage
//! reads use [`verglas_cache::HybridCacheEngine`] over
//! [`verglas_s3::PassthroughRead`], while writes go directly to the durable
//! origin and invalidate the exact cache mapping only after the origin accepts
//! the object.

use std::fmt;
use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::channel::mpsc::{self, Sender};
use futures::stream::BoxStream;
use futures::{SinkExt, StreamExt, TryStreamExt};
use iceberg::io::{
    FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage, StorageConfig,
    StorageFactory,
};
use iceberg::{Error, ErrorKind, Result};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use tokio::task::JoinHandle;
use verglas_backend::BackendStores;
use verglas_cache::HybridCacheEngine;
use verglas_core::CacheKey;
use verglas_core::config::Cache as CacheConfig;
use verglas_core::read::{ObjectRead, ReadError, ReadRange};
use verglas_core::write::{Invalidation, ObjectWrite, WriteError, WriteMetadata};
use verglas_s3::{PassthroughRead, PassthroughWrite};

/// A construction failure for the host-owned Iceberg origin storage.
#[derive(Debug, thiserror::Error)]
pub enum OriginStorageError {
    /// The factory configuration does not identify one exact origin route.
    #[error("invalid origin storage configuration: {0}")]
    InvalidConfiguration(String),
    /// The configured binding or bucket cannot be resolved by the backend.
    #[error("origin storage backend is unavailable: {0}")]
    Backend(String),
    /// Foyer could not open the configured cache.
    #[error("origin storage cache could not be opened: {0}")]
    Cache(String),
}

/// Immutable identity and cache settings for one Iceberg origin route.
#[derive(Debug, Clone)]
pub struct OriginStorageConfig {
    /// Exact backend binding included in every cache key.
    storage_binding_id: String,
    /// Exact origin bucket included in every cache key and URI check.
    bucket: String,
    /// URI scheme accepted by the Iceberg storage surface.
    scheme: String,
    /// Foyer DRAM/NVMe settings.
    cache: CacheConfig,
}

impl OriginStorageConfig {
    /// Creates a production S3 route for one binding, bucket, and cache.
    pub fn new(
        storage_binding_id: impl Into<String>,
        bucket: impl Into<String>,
        cache: CacheConfig,
    ) -> Self {
        Self {
            storage_binding_id: storage_binding_id.into(),
            bucket: bucket.into(),
            scheme: "s3".to_owned(),
            cache,
        }
    }

    /// Replaces the URI scheme while retaining the one explicit origin route.
    ///
    /// This is for provider-specific absolute Iceberg locations such as `gs://`
    /// or `az://`; it does not enable path or local-storage fallbacks.
    pub fn with_scheme(mut self, scheme: impl Into<String>) -> Self {
        self.scheme = scheme.into();
        self
    }

    /// Returns the configured storage binding identity.
    pub fn storage_binding_id(&self) -> &str {
        &self.storage_binding_id
    }

    /// Returns the configured origin bucket.
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// Returns the only accepted absolute-location scheme.
    pub fn scheme(&self) -> &str {
        &self.scheme
    }

    /// Returns the Foyer configuration used by the factory.
    pub fn cache(&self) -> &CacheConfig {
        &self.cache
    }
}

/// A host-owned `StorageFactory` for one exact binding and bucket.
///
/// The cache engine is opened before the factory is returned, so a successful
/// construction has already validated the backend route and Foyer device.
#[derive(Clone)]
pub struct OriginStorageFactory {
    /// Backend registry used for direct origin reads and writes.
    stores: Arc<dyn BackendStores>,
    /// Immutable route and cache configuration.
    config: OriginStorageConfig,
    /// Shared Foyer engine used by every storage handle built by this factory.
    cache: HybridCacheEngine,
}

impl fmt::Debug for OriginStorageFactory {
    /// Describes the fixed route without exposing backend credentials.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OriginStorageFactory")
            .field("storage_binding_id", &self.config.storage_binding_id)
            .field("bucket", &self.config.bucket)
            .field("scheme", &self.config.scheme)
            .finish_non_exhaustive()
    }
}

impl Serialize for OriginStorageFactory {
    /// Serializes only the typetag marker because origin handles and cache
    /// devices are process-local host capabilities, never portable state.
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_unit_struct("VerglasOriginStorageFactory")
    }
}

impl<'de> Deserialize<'de> for OriginStorageFactory {
    /// Rejects deserialization because a host-owned backend and Foyer device
    /// must be injected by fresh production wiring, not reconstructed from data.
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let _ = serde::de::IgnoredAny::deserialize(deserializer)?;
        Err(serde::de::Error::custom(
            "host-owned origin storage cannot be deserialized",
        ))
    }
}

impl OriginStorageFactory {
    /// Opens a factory after resolving its one exact backend binding/bucket and
    /// constructing the persistent Foyer engine over `PassthroughRead`.
    pub async fn new(
        stores: Arc<dyn BackendStores>,
        config: OriginStorageConfig,
    ) -> std::result::Result<Self, OriginStorageError> {
        validate_config(&config)?;
        stores
            .store_for(&config.storage_binding_id, &config.bucket)
            .map_err(|error| OriginStorageError::Backend(error.to_string()))?;
        let origin = PassthroughRead::new(Arc::clone(&stores));
        let cache = HybridCacheEngine::new(origin, &config.cache)
            .await
            .map_err(|error| OriginStorageError::Cache(error.to_string()))?;
        Self::with_cache(stores, config, cache)
    }

    /// Opens a factory over an already constructed host cache. Turso and
    /// Iceberg use this path so one Foyer instance is the cell's only local
    /// cache authority.
    pub fn with_cache(
        stores: Arc<dyn BackendStores>,
        config: OriginStorageConfig,
        cache: HybridCacheEngine,
    ) -> std::result::Result<Self, OriginStorageError> {
        validate_config(&config)?;
        stores
            .store_for(&config.storage_binding_id, &config.bucket)
            .map_err(|error| OriginStorageError::Backend(error.to_string()))?;
        Ok(Self {
            stores,
            config,
            cache,
        })
    }

    /// Returns the shared cache engine for metrics, flushing, and acceptance
    /// tests. The returned engine is not a second cache authority.
    pub fn cache(&self) -> &HybridCacheEngine {
        &self.cache
    }

    /// Builds one storage handle directly, equivalent to `StorageFactory::build`.
    pub fn storage(&self) -> Arc<dyn Storage> {
        Arc::new(OriginStorage::new(
            Arc::clone(&self.stores),
            self.config.clone(),
            self.cache.clone(),
        ))
    }
}

#[typetag::serde(name = "VerglasOriginStorageFactory")]
impl StorageFactory for OriginStorageFactory {
    /// Returns a storage handle sharing this factory's exact route and Foyer
    /// engine. Iceberg properties cannot change either identity dimension.
    fn build(&self, _config: &StorageConfig) -> Result<Arc<dyn Storage>> {
        Ok(self.storage())
    }
}

/// The concrete Iceberg storage returned by [`OriginStorageFactory`].
#[derive(Clone)]
struct OriginStorage {
    /// Backend registry used for writes and the cache's origin reader.
    stores: Arc<dyn BackendStores>,
    /// Immutable route and URI validation policy.
    config: OriginStorageConfig,
    /// Shared read-through cache.
    cache: HybridCacheEngine,
}

impl fmt::Debug for OriginStorage {
    /// Describes the fixed route without exposing origin credentials.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OriginStorage")
            .field("storage_binding_id", &self.config.storage_binding_id)
            .field("bucket", &self.config.bucket)
            .field("scheme", &self.config.scheme)
            .finish_non_exhaustive()
    }
}

impl Serialize for OriginStorage {
    /// Serializes only the typetag marker because this handle contains live
    /// backend credentials and an open local cache device.
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_unit_struct("VerglasOriginStorage")
    }
}

impl<'de> Deserialize<'de> for OriginStorage {
    /// Rejects deserialization because storage handles must be made by the host
    /// factory with fresh backend and cache capabilities.
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let _ = serde::de::IgnoredAny::deserialize(deserializer)?;
        Err(serde::de::Error::custom(
            "host-owned origin storage cannot be deserialized",
        ))
    }
}

impl OriginStorage {
    /// Creates one storage handle over an already-opened cache engine.
    fn new(
        stores: Arc<dyn BackendStores>,
        config: OriginStorageConfig,
        cache: HybridCacheEngine,
    ) -> Self {
        Self {
            stores,
            config,
            cache,
        }
    }

    /// Parses and validates one absolute Iceberg location into the exact cache
    /// identity used for both reads and writes.
    fn key(&self, location: &str) -> Result<CacheKey> {
        parse_location(location, &self.config)
    }

    /// Reads one requested range through Foyer and collects the Iceberg byte
    /// result after the cache has served or filled all covering blocks.
    async fn read_range(&self, key: &CacheKey, range: ReadRange) -> Result<Bytes> {
        let response = self.cache.get(key, range).await.map_err(read_error)?;
        let mut output = BytesMut::new();
        let mut body = response.body;
        while let Some(chunk) = body.try_next().await.map_err(read_error)? {
            output.extend_from_slice(&chunk);
        }
        Ok(output.freeze())
    }

    /// Writes one immutable Iceberg object through the durable origin path, then
    /// drops this object's mapping before returning success. A retry for an
    /// existing object is accepted only when its durable bytes are identical.
    async fn write_durable(&self, key: CacheKey, bytes: Bytes) -> Result<()> {
        if let Some(size) = self.existing_size(&key).await? {
            if size != bytes.len() as u64 {
                return Err(immutable_conflict(&key));
            }
            let existing = self.read_range(&key, ReadRange::Full).await?;
            if existing != bytes {
                return Err(immutable_conflict(&key));
            }
            self.invalidate(&key).await?;
            return Ok(());
        }
        let writer = PassthroughWrite::new(Arc::clone(&self.stores));
        let body = futures::stream::once(async move { Ok(bytes) }).boxed();
        writer
            .put(&key, WriteMetadata::default(), body)
            .await
            .map_err(write_error)?;
        self.invalidate(&key).await
    }

    /// Returns the current object size through Foyer's metadata path, or `None`
    /// when the exact origin key does not exist.
    async fn existing_size(&self, key: &CacheKey) -> Result<Option<u64>> {
        match self.cache.head(key).await {
            Ok(meta) => Ok(Some(meta.size)),
            Err(ReadError::NoSuchKey) => Ok(None),
            Err(error) => Err(read_error(error)),
        }
    }

    /// Invalidates one exact binding/bucket/key mapping after durable origin I/O.
    async fn invalidate(&self, key: &CacheKey) -> Result<()> {
        self.cache
            .invalidate(std::slice::from_ref(key))
            .await
            .map_err(storage_message)
    }

    /// Starts a streaming origin writer whose close waits for durable origin
    /// completion and then performs cache invalidation. Existing objects use a
    /// bounded compare writer so deterministic retries cannot overwrite bytes.
    async fn writer_for(&self, key: CacheKey) -> Result<Box<dyn FileWrite>> {
        if let Some(size) = self.existing_size(&key).await? {
            return Ok(Box::new(ExistingFileWriter {
                storage: self.clone(),
                key,
                expected_size: size,
                offset: 0,
                closed: false,
            }));
        }
        let (sender, receiver) = mpsc::channel::<std::result::Result<Bytes, WriteError>>(2);
        let stores = Arc::clone(&self.stores);
        let cache = self.cache.clone();
        let task = tokio::spawn(async move {
            let body = receiver.boxed();
            let writer = PassthroughWrite::new(stores);
            writer
                .put(&key, WriteMetadata::default(), body)
                .await
                .map_err(write_error)?;
            cache
                .invalidate(std::slice::from_ref(&key))
                .await
                .map_err(storage_message)
        });
        Ok(Box::new(OriginFileWriter {
            sender: Some(sender),
            task: Some(task),
        }))
    }

    /// Deletes one origin object and invalidates its exact cache mapping.
    async fn delete_durable(&self, key: CacheKey) -> Result<()> {
        let writer = PassthroughWrite::new(Arc::clone(&self.stores));
        writer.delete(&key).await.map_err(write_error)?;
        self.cache
            .invalidate(std::slice::from_ref(&key))
            .await
            .map_err(storage_message)
    }
}

#[async_trait]
#[typetag::serde(name = "VerglasOriginStorage")]
impl Storage for OriginStorage {
    /// Checks existence through the cached metadata mapping and never consults a
    /// local or alternate storage implementation.
    async fn exists(&self, location: &str) -> Result<bool> {
        let key = self.key(location)?;
        match self.cache.head(&key).await {
            Ok(_) => Ok(true),
            Err(ReadError::NoSuchKey) => Ok(false),
            Err(error) => Err(read_error(error)),
        }
    }

    /// Returns the origin-reported object size through the Foyer metadata path.
    async fn metadata(&self, location: &str) -> Result<FileMetadata> {
        let key = self.key(location)?;
        let meta = self.cache.head(&key).await.map_err(read_error)?;
        Ok(FileMetadata { size: meta.size })
    }

    /// Reads the complete object through Foyer using the exact binding, bucket,
    /// object version mapping, block geometry, and block ranges.
    async fn read(&self, location: &str) -> Result<Bytes> {
        let key = self.key(location)?;
        self.read_range(&key, ReadRange::Full).await
    }

    /// Creates a range reader whose every read is independently routed through
    /// Foyer with the same exact object identity.
    async fn reader(&self, location: &str) -> Result<Box<dyn FileRead>> {
        let key = self.key(location)?;
        Ok(Box::new(OriginFileRead {
            storage: self.clone(),
            key,
        }))
    }

    /// Writes durable origin bytes before invalidating the corresponding cache
    /// mapping and acknowledging the Iceberg write.
    async fn write(&self, location: &str, bytes: Bytes) -> Result<()> {
        let key = self.key(location)?;
        self.write_durable(key, bytes).await
    }

    /// Returns a streaming writer whose close has the same durable-write fence
    /// as [`OriginStorage::write`].
    async fn writer(&self, location: &str) -> Result<Box<dyn FileWrite>> {
        let key = self.key(location)?;
        self.writer_for(key).await
    }

    /// Deletes through the durable origin and invalidates the exact mapping.
    async fn delete(&self, location: &str) -> Result<()> {
        let key = self.key(location)?;
        self.delete_durable(key).await
    }

    /// Deletes each exact path in the supplied stream through the same durable
    /// origin and invalidation fence as a single delete.
    async fn delete_stream(&self, mut locations: BoxStream<'static, String>) -> Result<()> {
        while let Some(location) = locations.next().await {
            self.delete(&location).await?;
        }
        Ok(())
    }

    /// Prefix deletion is deliberately not part of the narrow immutable
    /// proposal-storage capability.
    async fn delete_prefix(&self, location: &str) -> Result<()> {
        let _ = self.key(location)?;
        Err(storage_message(
            "prefix deletion is not supported by host-owned Iceberg storage",
        ))
    }

    /// Creates an input file only after validating its absolute route.
    fn new_input(&self, location: &str) -> Result<InputFile> {
        let _ = self.key(location)?;
        Ok(InputFile::new(Arc::new(self.clone()), location.to_owned()))
    }

    /// Creates an output file only after validating its absolute route.
    fn new_output(&self, location: &str) -> Result<OutputFile> {
        let _ = self.key(location)?;
        Ok(OutputFile::new(Arc::new(self.clone()), location.to_owned()))
    }
}

/// A range-aware Iceberg reader backed by the shared Foyer engine.
struct OriginFileRead {
    /// Shared storage handle containing exact identity and cache state.
    storage: OriginStorage,
    /// Exact object identity resolved when the reader was created.
    key: CacheKey,
}

impl fmt::Debug for OriginFileRead {
    /// Describes the logical object without origin credentials.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OriginFileRead")
            .field("storage_binding_id", &self.key.storage_binding_id)
            .field("bucket", &self.key.bucket)
            .field("key", &self.key.key)
            .finish()
    }
}

#[async_trait]
impl FileRead for OriginFileRead {
    /// Reads the requested half-open range through Foyer's exact block geometry.
    async fn read(&self, range: Range<u64>) -> Result<Bytes> {
        if range.start > range.end {
            return Err(storage_message("file read range start exceeds end"));
        }
        if range.start == range.end {
            return Ok(Bytes::new());
        }
        self.storage
            .read_range(
                &self.key,
                ReadRange::Bounded(range.start, range.end.saturating_sub(1)),
            )
            .await
    }
}

/// A bounded retry writer that compares incoming bytes with an existing
/// immutable origin object instead of overwriting it.
struct ExistingFileWriter {
    /// Shared Foyer-backed storage used for exact range comparisons.
    storage: OriginStorage,
    /// Exact object identity being retried.
    key: CacheKey,
    /// Durable object size captured before the comparison began.
    expected_size: u64,
    /// Number of bytes compared so far.
    offset: u64,
    /// Whether close has already been attempted or a mismatch was observed.
    closed: bool,
}

impl fmt::Debug for ExistingFileWriter {
    /// Names the compared object without exposing its bytes.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExistingFileWriter")
            .field("storage_binding_id", &self.key.storage_binding_id)
            .field("bucket", &self.key.bucket)
            .field("key", &self.key.key)
            .field("offset", &self.offset)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl FileWrite for ExistingFileWriter {
    /// Compares one incoming chunk against the exact cached/origin range.
    async fn write(&mut self, bytes: Bytes) -> Result<()> {
        if self.closed {
            return Err(storage_message("cannot write to a closed origin file"));
        }
        let end = self
            .offset
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| storage_message("origin file size overflow"))?;
        if end > self.expected_size {
            self.closed = true;
            return Err(immutable_conflict(&self.key));
        }
        if !bytes.is_empty() {
            let existing = self
                .storage
                .read_range(
                    &self.key,
                    ReadRange::Bounded(self.offset, end.saturating_sub(1)),
                )
                .await?;
            if existing != bytes {
                self.closed = true;
                return Err(immutable_conflict(&self.key));
            }
        }
        self.offset = end;
        Ok(())
    }

    /// Verifies that the retry supplied the complete immutable object, then
    /// invalidates its mapping before acknowledging the close.
    async fn close(&mut self) -> Result<()> {
        if self.closed {
            return Err(storage_message("origin file is already closed"));
        }
        self.closed = true;
        if self.offset != self.expected_size {
            return Err(storage_message(format!(
                "immutable origin retry for {} ended at {} of {} bytes",
                self.key.key, self.offset, self.expected_size
            )));
        }
        self.storage.invalidate(&self.key).await
    }
}

/// A streaming file writer that forwards chunks to the durable origin task.
struct OriginFileWriter {
    /// Channel feeding the origin write body.
    sender: Option<Sender<std::result::Result<Bytes, WriteError>>>,
    /// Task that completes the origin write and invalidation fence.
    task: Option<JoinHandle<Result<()>>>,
}

impl fmt::Debug for OriginFileWriter {
    /// Names the writer without exposing body contents.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OriginFileWriter")
            .field("open", &self.sender.is_some())
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl FileWrite for OriginFileWriter {
    /// Sends one chunk to the origin stream without buffering the whole object.
    async fn write(&mut self, bytes: Bytes) -> Result<()> {
        let sender = self
            .sender
            .as_mut()
            .ok_or_else(|| storage_message("cannot write to a closed origin file"))?;
        sender
            .send(Ok(bytes))
            .await
            .map_err(|error| storage_message(format!("origin writer stopped: {error}")))
    }

    /// Closes the body, waits for durable origin completion, then observes the
    /// cache invalidation result before returning success.
    async fn close(&mut self) -> Result<()> {
        self.sender
            .take()
            .ok_or_else(|| storage_message("origin file is already closed"))?;
        let task = self
            .task
            .take()
            .ok_or_else(|| storage_message("origin writer task is already complete"))?;
        task.await
            .map_err(|error| storage_message(format!("origin writer task failed: {error}")))??;
        Ok(())
    }
}

impl Drop for OriginFileWriter {
    /// Aborts an unfinished origin write so dropping a writer never publishes a
    /// partial immutable object.
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

/// Validates that configuration names exactly one binding, bucket, and scheme.
fn validate_config(config: &OriginStorageConfig) -> std::result::Result<(), OriginStorageError> {
    for (name, value) in [
        ("storage binding", config.storage_binding_id.as_str()),
        ("bucket", config.bucket.as_str()),
        ("URI scheme", config.scheme.as_str()),
    ] {
        if value.trim().is_empty() || value.contains('/') || value.contains(':') {
            return Err(OriginStorageError::InvalidConfiguration(format!(
                "{name} must be one non-empty name"
            )));
        }
    }
    if matches!(config.scheme.as_str(), "file" | "memory") {
        return Err(OriginStorageError::InvalidConfiguration(
            "local and in-memory URI schemes are not origin routes".to_owned(),
        ));
    }
    Ok(())
}

/// Converts one absolute Iceberg URI into the exact logical cache key.
fn parse_location(location: &str, config: &OriginStorageConfig) -> Result<CacheKey> {
    let Some((scheme, authority_and_key)) = location.split_once("://") else {
        return Err(storage_message(
            "Iceberg origin locations must be absolute URIs",
        ));
    };
    if scheme != config.scheme {
        return Err(storage_message(format!(
            "Iceberg URI scheme `{scheme}` is not configured for this origin"
        )));
    }
    let Some((authority, key)) = authority_and_key.split_once('/') else {
        return Err(storage_message("Iceberg origin URI has no object key"));
    };
    if authority != config.bucket || key.is_empty() || key.starts_with('/') {
        return Err(storage_message(
            "Iceberg origin URI does not match the configured bucket",
        ));
    }
    if key.contains(['\\', '?', '#'])
        || key.chars().any(char::is_control)
        || key
            .split('/')
            .any(|segment| segment.is_empty() || segment == "." || segment == "..")
    {
        return Err(storage_message(
            "Iceberg origin URI contains an ambiguous object key",
        ));
    }
    Ok(CacheKey {
        storage_binding_id: config.storage_binding_id.clone(),
        bucket: config.bucket.clone(),
        key: key.to_owned(),
    })
}

/// Converts a cache read failure into the Iceberg storage error space.
fn read_error(error: ReadError) -> Error {
    storage_message(format!("origin read failed: {error}"))
}

/// Converts a durable origin write failure into the Iceberg storage error space.
fn write_error(error: WriteError) -> Error {
    storage_message(format!("origin write failed: {error}"))
}

/// Reports an attempted overwrite of an immutable object path.
fn immutable_conflict(key: &CacheKey) -> Error {
    storage_message(format!(
        "immutable Iceberg object already exists at {}/{}/{} with different bytes",
        key.storage_binding_id, key.bucket, key.key
    ))
}

/// Creates one non-retryable Iceberg backend error.
fn storage_message(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::Unexpected, message)
}
