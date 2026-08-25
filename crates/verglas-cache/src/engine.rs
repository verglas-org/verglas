//! A single-node Foyer origin cache.
//!
//! The engine resolves an ETag mapping, serves fixed-size range blocks from
//! DRAM or persistent storage, and fills misses from one origin reader. Block
//! keys include the ETag and geometry, so persistent recovery cannot mix object
//! versions. There is no ownership, peer, or write path in this crate.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

use bytes::{Bytes, BytesMut};
use foyer::{
    BlockEngineConfig, Cache, CacheBuilder, DeviceBuilder, FsDeviceBuilder, HybridCache,
    HybridCacheBuilder, HybridCacheEntry, HybridCachePolicy, RecoverMode,
};
use futures::future::{BoxFuture, FutureExt, Shared, TryFutureExt};
use futures::stream::{self, StreamExt, TryStreamExt};
use verglas_core::config::Cache as CacheConfig;
use verglas_core::read::{
    BodyStream, DirectGet, DirectMeta, DirectReadOptions, ObjectGet, ObjectMeta, ObjectRead,
    ReadError, ReadRange, Revalidation, ServedTier, TierCell,
};
use verglas_core::write::Invalidation;
use verglas_core::{BlockKey, CacheKey};

use crate::admission::Admission;
use crate::block::{block_len, covering_blocks, resolve_range};
use crate::counters::CacheCounters;
use crate::entry::{BlockEntryKey, CachedMeta};
use crate::foyer_metrics::BlockCacheMetricsRegistry;

/// Default disk framing added to one data block for Foyer's index and headers.
const DISK_FRAMING_BYTES: usize = 16 * 1024;
/// Per-entry accounting charged to the DRAM cache weighter.
const ENTRY_OVERHEAD_BYTES: usize = 256;
/// Maximum object-mapping cache budget. It is carved out of the configured DRAM
/// budget before the block cache is opened.
const MAPPING_BUDGET_BYTES: usize = 16 * 1024 * 1024;
/// Minimum useful block cache budget after the mapping carve-out.
const MIN_BLOCK_MEMORY_BYTES: u64 = 2 * 4096;
/// Minimum persistent capacity needed by Foyer's reclaimer.
const MIN_DISK_BLOCKS: u64 = 4;

/// A cache construction or configuration failure.
#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    /// The configured DRAM budget cannot hold the mapping cache and two blocks.
    #[error("cache DRAM budget is too small: {0} bytes")]
    DramBudgetTooSmall(u64),
    /// The configured persistent budget cannot hold Foyer's minimum device.
    #[error("cache persistent budget is too small: {0} bytes")]
    DiskBudgetTooSmall(u64),
    /// Foyer could not open or recover the persistent cache.
    #[error("Foyer cache failed to open: {0}")]
    Foyer(String),
}

/// The tier that supplied one complete block.
#[derive(Debug, Clone, Copy)]
enum BlockTier {
    /// The in-memory Foyer tier.
    Dram,
    /// The persistent Foyer tier.
    Nvme,
    /// The origin fill.
    Backend,
}

/// A shared result for one origin fill. The error is reference-counted because
/// every waiter must receive the same outcome without rerunning the origin GET.
type SharedFill = Shared<BoxFuture<'static, Result<Bytes, Arc<ReadError>>>>;

/// Object-safe adapter around the core reader trait. The core trait uses
/// return-position futures, so this small internal seam is what lets the
/// public cache handle stay a concrete single-node type.
#[async_trait::async_trait]
trait CacheBackend: Send + Sync {
    /// Reads one origin range.
    async fn get(&self, key: &CacheKey, range: ReadRange) -> Result<ObjectGet, ReadError>;
    /// Reads origin metadata.
    async fn head(&self, key: &CacheKey) -> Result<ObjectMeta, ReadError>;
    /// Reads a direct origin range.
    async fn get_direct(
        &self,
        key: &CacheKey,
        range: ReadRange,
        options: DirectReadOptions,
    ) -> Result<DirectGet, ReadError>;
    /// Reads direct origin metadata.
    async fn head_direct(
        &self,
        key: &CacheKey,
        options: DirectReadOptions,
    ) -> Result<DirectMeta, ReadError>;
    /// Revalidates one origin mapping.
    async fn revalidate(&self, key: &CacheKey, etag: &str) -> Result<Revalidation, ReadError>;
}

/// Stores the concrete reader behind the internal object-safe adapter.
struct BackendAdapter<B>(B);

#[async_trait::async_trait]
impl<B> CacheBackend for BackendAdapter<B>
where
    B: ObjectRead,
{
    /// Calls the concrete reader's ranged GET.
    async fn get(&self, key: &CacheKey, range: ReadRange) -> Result<ObjectGet, ReadError> {
        self.0.get(key, range).await
    }

    /// Calls the concrete reader's HEAD.
    async fn head(&self, key: &CacheKey) -> Result<ObjectMeta, ReadError> {
        self.0.head(key).await
    }

    /// Calls the concrete reader's direct GET.
    async fn get_direct(
        &self,
        key: &CacheKey,
        range: ReadRange,
        options: DirectReadOptions,
    ) -> Result<DirectGet, ReadError> {
        self.0.get_direct(key, range, options).await
    }

    /// Calls the concrete reader's direct HEAD.
    async fn head_direct(
        &self,
        key: &CacheKey,
        options: DirectReadOptions,
    ) -> Result<DirectMeta, ReadError> {
        self.0.head_direct(key, options).await
    }

    /// Calls the concrete reader's conditional revalidation.
    async fn revalidate(&self, key: &CacheKey, etag: &str) -> Result<Revalidation, ReadError> {
        self.0.revalidate(key, etag).await
    }
}

/// State shared by cheap engine handles and detached fill futures.
struct Inner {
    /// Fixed-size immutable origin blocks.
    blocks: HybridCache<BlockEntryKey, Bytes>,
    /// DRAM-only object mappings. They are intentionally not persisted.
    mappings: Cache<CacheKey, CachedMeta>,
    /// The configured origin reader.
    backend: Arc<dyn CacheBackend>,
    /// Scan-resistant admission state.
    admission: Admission,
    /// Counters recorded by the read path.
    counters: CacheCounters,
    /// In-flight origin fills keyed by the immutable block key.
    fills: Mutex<HashMap<BlockEntryKey, SharedFill>>,
    /// Configured cache block geometry.
    block_bytes: u64,
}

/// A cheap handle to one single-node origin cache.
pub struct HybridCacheEngine {
    /// Shared cache state.
    inner: Arc<Inner>,
}

impl Clone for HybridCacheEngine {
    /// Clones the handle without copying cache state.
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl HybridCacheEngine {
    /// Opens a persistent Foyer cache over `backend`.
    pub async fn new<B>(backend: B, config: &CacheConfig) -> Result<Self, EngineError>
    where
        B: ObjectRead,
    {
        let block_bytes = config.data_block_bytes.0;
        let dram = config.dram_bytes.0;
        let disk = config.capacity_bytes.0;
        let block_memory = block_memory_budget(dram)?;
        let disk_block_bytes = checked_usize(block_bytes.saturating_add(DISK_FRAMING_BYTES as u64))
            .ok_or(EngineError::DiskBudgetTooSmall(disk))?;
        let minimum_disk = MIN_DISK_BLOCKS.saturating_mul(disk_block_bytes as u64);
        if disk < minimum_disk {
            return Err(EngineError::DiskBudgetTooSmall(disk));
        }
        let disk_capacity = checked_usize(disk).ok_or(EngineError::DiskBudgetTooSmall(disk))?;
        let device = FsDeviceBuilder::new(&config.dir)
            .with_capacity(disk_capacity)
            .build()
            .map_err(|error| EngineError::Foyer(error.to_string()))?;
        let overflow_counter = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let metrics = BlockCacheMetricsRegistry::new(Arc::clone(&overflow_counter));
        let blocks = HybridCacheBuilder::<BlockEntryKey, Bytes>::new()
            .with_name("verglas-origin-blocks")
            .with_policy(HybridCachePolicy::WriteOnInsertion)
            .with_metrics_registry(Box::new(metrics))
            .memory(block_memory)
            .with_weighter(|_, value: &Bytes| value.len().saturating_add(ENTRY_OVERHEAD_BYTES))
            .storage()
            .with_engine_config(BlockEngineConfig::new(device).with_block_size(disk_block_bytes))
            .with_recover_mode(RecoverMode::Quiet)
            .build()
            .await
            .map_err(|error| EngineError::Foyer(error.to_string()))?;
        let mapping_budget = mapping_budget(dram);
        let mappings = CacheBuilder::new(mapping_budget)
            .with_name("verglas-object-mappings")
            .with_weighter(|key: &CacheKey, value: &CachedMeta| value.weight(key))
            .build();
        Ok(Self {
            inner: Arc::new(Inner {
                blocks,
                mappings,
                backend: Arc::new(BackendAdapter(backend)),
                admission: Admission::new(&config.admission, disk, block_bytes),
                counters: CacheCounters {
                    storage_buffer_overflows: overflow_counter,
                    ..CacheCounters::default()
                },
                fills: Mutex::new(HashMap::new()),
                block_bytes,
            }),
        })
    }

    /// Returns live read counters.
    pub fn counters(&self) -> &CacheCounters {
        &self.inner.counters
    }

    /// Returns DRAM bytes charged by Foyer's two in-memory caches.
    pub fn dram_usage(&self) -> u64 {
        (self.inner.blocks.memory().usage() + self.inner.mappings.usage()) as u64
    }

    /// Waits for queued persistent writes to complete.
    pub async fn flush(&self) {
        self.inner.blocks.storage().wait().await;
    }

    /// Resolves an object mapping from DRAM or the origin HEAD path.
    async fn mapping(&self, key: &CacheKey) -> Result<Option<CachedMeta>, ReadError> {
        if let Some(entry) = self.inner.mappings.get(key) {
            return Ok(Some(entry.value().clone()));
        }
        CacheCounters::bump(&self.inner.counters.backend_heads);
        let origin = self.inner.backend.head(key).await?;
        let Some(mapping) = CachedMeta::from_object_meta(&origin) else {
            CacheCounters::bump(&self.inner.counters.non_cacheable_passthroughs);
            return Ok(None);
        };
        self.inner.mappings.insert(key.clone(), mapping.clone());
        Ok(Some(mapping))
    }

    /// Reads one resolved block through DRAM, persistent storage, or one shared
    /// origin fill.
    async fn read_block(
        &self,
        key: &CacheKey,
        mapping: &CachedMeta,
        index: u64,
    ) -> Result<(Bytes, BlockTier), ReadError> {
        let block_key = block_key(key, mapping, index, self.inner.block_bytes);
        let expected_len = block_len(mapping.size, index, self.inner.block_bytes);
        if let Some(entry) = self.inner.blocks.memory().get(&block_key)
            && let Some(bytes) = valid_entry(&entry, expected_len)
        {
            CacheCounters::bump(&self.inner.counters.dram_hits);
            self.inner.admission.record_hit(&block_key);
            return Ok((bytes, BlockTier::Dram));
        }
        CacheCounters::bump(&self.inner.counters.dram_misses);
        match self.inner.blocks.get(&block_key).await {
            Ok(Some(entry)) => {
                if let Some(bytes) = valid_entry(&entry, expected_len) {
                    CacheCounters::bump(&self.inner.counters.disk_hits);
                    self.inner.admission.record_hit(&block_key);
                    return Ok((bytes, BlockTier::Nvme));
                }
                CacheCounters::bump(&self.inner.counters.disk_misses);
            }
            Ok(None) | Err(_) => CacheCounters::bump(&self.inner.counters.disk_misses),
        }
        let fill = {
            let mut fills = lock_fills(&self.inner.fills);
            if let Some(existing) = fills.get(&block_key) {
                CacheCounters::bump(&self.inner.counters.deduped_fills);
                (existing.clone(), false)
            } else {
                let inner = Arc::clone(&self.inner);
                let fill_key = key.clone();
                let fill_mapping = mapping.clone();
                let fill = async move { inner.fill_block(fill_key, fill_mapping, index).await }
                    .map_err(Arc::new)
                    .boxed()
                    .shared();
                fills.insert(block_key.clone(), fill.clone());
                (fill, true)
            }
        };
        let result = fill.0.await;
        if fill.1 {
            lock_fills(&self.inner.fills).remove(&block_key);
        }
        match result {
            Ok(bytes) => Ok((bytes, BlockTier::Backend)),
            Err(error) => Err(clone_read_error(&error)),
        }
    }
}

impl Inner {
    /// Fetches and validates one complete block before admitting it.
    async fn fill_block(
        self: Arc<Self>,
        key: CacheKey,
        mapping: CachedMeta,
        index: u64,
    ) -> Result<Bytes, ReadError> {
        let expected_len = block_len(mapping.size, index, self.block_bytes);
        let start = index.saturating_mul(self.block_bytes);
        let end = start.saturating_add(expected_len as u64).saturating_sub(1);
        CacheCounters::bump(&self.counters.backend_fills);
        let response = self
            .backend
            .get(&key, ReadRange::Bounded(start, end))
            .await?;
        if response.meta.e_tag.as_deref() != Some(mapping.etag.as_str()) {
            match CachedMeta::from_object_meta(&response.meta) {
                Some(fresh) => {
                    self.mappings.insert(key.clone(), fresh);
                }
                None => {
                    self.mappings.remove(&key);
                }
            }
            return Err(ReadError::Backend(format!(
                "origin version changed for {}/{}",
                key.bucket, key.key
            )));
        }
        let Some(bytes) = collect_block(response.body, expected_len).await? else {
            return Err(ReadError::Backend(format!(
                "origin returned an invalid block length for {}/{}",
                key.bucket, key.key
            )));
        };
        CacheCounters::add(&self.counters.backend_fill_bytes, bytes.len() as u64);
        let block_key = block_key(&key, &mapping, index, self.block_bytes);
        if self.admission.admit(&block_key, bytes.len() as u64) {
            self.blocks.insert(block_key, bytes.clone());
            CacheCounters::bump(&self.counters.blocks_admitted);
        } else {
            CacheCounters::bump(&self.counters.blocks_rejected);
        }
        response.served_from.set(ServedTier::Backend);
        Ok(bytes)
    }
}

impl ObjectRead for HybridCacheEngine {
    /// Resolves the range locally and streams its covering blocks.
    fn get(
        &self,
        key: &CacheKey,
        requested: ReadRange,
    ) -> impl std::future::Future<Output = Result<ObjectGet, ReadError>> + Send {
        let engine = self.clone();
        let key = key.clone();
        async move {
            let Some(mapping) = engine.mapping(&key).await? else {
                let response = engine.inner.backend.get(&key, requested).await?;
                response.served_from.set(ServedTier::Passthrough);
                return Ok(response);
            };
            let range = resolve_range(requested, mapping.size)?;
            let meta = mapping.to_object_meta();
            let served_from = TierCell::new();
            if range.is_empty() {
                return Ok(ObjectGet {
                    meta,
                    range,
                    body: stream::empty().boxed(),
                    served_from,
                });
            }
            let (first, last) = covering_blocks(&range, engine.inner.block_bytes);
            let body_engine = engine.clone();
            let body_key = key.clone();
            let body_mapping = mapping.clone();
            let body_served_from = served_from.clone();
            let body = stream::iter(first..=last)
                .then(move |index| {
                    let engine = body_engine.clone();
                    let key = body_key.clone();
                    let mapping = body_mapping.clone();
                    async move {
                        let (block, tier) = engine.read_block(&key, &mapping, index).await?;
                        let block_start = index.saturating_mul(engine.inner.block_bytes);
                        let start = range.start.saturating_sub(block_start) as usize;
                        let end = range
                            .end
                            .saturating_sub(block_start)
                            .min(block.len() as u64) as usize;
                        let slice = block.slice(start..end);
                        engine.count_served(tier, slice.len() as u64);
                        Ok::<(Bytes, BlockTier), ReadError>((slice, tier))
                    }
                })
                .map(move |result| match result {
                    Ok((bytes, tier)) => {
                        body_served_from.set(tier.served_tier());
                        Ok(bytes)
                    }
                    Err(error) => Err(error),
                })
                .boxed();
            Ok(ObjectGet {
                meta,
                range,
                body,
                served_from,
            })
        }
    }

    /// Returns a cached object mapping or reads origin metadata.
    fn head(
        &self,
        key: &CacheKey,
    ) -> impl std::future::Future<Output = Result<ObjectMeta, ReadError>> + Send {
        let engine = self.clone();
        let key = key.clone();
        async move {
            match engine.mapping(&key).await? {
                Some(mapping) => Ok(mapping.to_object_meta()),
                None => engine.inner.backend.head(&key).await,
            }
        }
    }

    /// Forwards a version-, part-, or checksum-scoped read to the origin.
    fn get_direct(
        &self,
        key: &CacheKey,
        range: ReadRange,
        options: DirectReadOptions,
    ) -> impl std::future::Future<Output = Result<DirectGet, ReadError>> + Send {
        let backend = Arc::clone(&self.inner.backend);
        let key = key.clone();
        async move { backend.get_direct(&key, range, options).await }
    }

    /// Forwards a version-, part-, or checksum-scoped HEAD to the origin.
    fn head_direct(
        &self,
        key: &CacheKey,
        options: DirectReadOptions,
    ) -> impl std::future::Future<Output = Result<DirectMeta, ReadError>> + Send {
        let backend = Arc::clone(&self.inner.backend);
        let key = key.clone();
        async move { backend.head_direct(&key, options).await }
    }

    /// Forwards conditional mapping revalidation to the origin.
    fn revalidate(
        &self,
        key: &CacheKey,
        etag: &str,
    ) -> impl std::future::Future<Output = Result<Revalidation, ReadError>> + Send {
        let backend = Arc::clone(&self.inner.backend);
        let key = key.clone();
        let etag = etag.to_owned();
        async move { backend.revalidate(&key, &etag).await }
    }
}

#[async_trait::async_trait]
impl Invalidation for HybridCacheEngine {
    /// Drops object mappings before a write acknowledgement can be returned.
    async fn invalidate(&self, keys: &[CacheKey]) -> Result<(), String> {
        for key in keys {
            self.inner.mappings.remove(key);
        }
        Ok(())
    }
}

impl BlockTier {
    /// Converts the internal tier to the core serving label.
    fn served_tier(self) -> ServedTier {
        match self {
            Self::Dram => ServedTier::Dram,
            Self::Nvme => ServedTier::Nvme,
            Self::Backend => ServedTier::Backend,
        }
    }
}

impl HybridCacheEngine {
    /// Records bytes served by one cache rung.
    fn count_served(&self, tier: BlockTier, bytes: u64) {
        let counter = match tier {
            BlockTier::Dram => &self.inner.counters.dram_bytes_served,
            BlockTier::Nvme => &self.inner.counters.disk_bytes_served,
            BlockTier::Backend => &self.inner.counters.backend_bytes_served,
        };
        CacheCounters::add(counter, bytes);
    }
}

/// Returns a key that embeds the current object version and geometry.
fn block_key(key: &CacheKey, mapping: &CachedMeta, index: u64, block_bytes: u64) -> BlockEntryKey {
    BlockEntryKey {
        block: BlockKey {
            object: key.clone(),
            etag: mapping.etag.clone(),
            block_bytes,
            block_index: index,
        },
    }
}

/// Returns a cached block only when its length is exactly the requested length.
fn valid_entry(
    entry: &HybridCacheEntry<BlockEntryKey, Bytes>,
    expected_len: usize,
) -> Option<Bytes> {
    (entry.value().len() == expected_len).then(|| entry.value().clone())
}

/// Collects one origin response into a bounded block buffer.
async fn collect_block(body: BodyStream, expected_len: usize) -> Result<Option<Bytes>, ReadError> {
    let mut buffer = BytesMut::with_capacity(expected_len);
    let mut body = body;
    while let Some(chunk) = body.try_next().await? {
        if buffer.len().saturating_add(chunk.len()) > expected_len {
            return Ok(None);
        }
        buffer.extend_from_slice(&chunk);
    }
    Ok((buffer.len() == expected_len).then(|| buffer.freeze()))
}

/// Converts a shared origin error back into the core error enum.
fn clone_read_error(error: &ReadError) -> ReadError {
    match error {
        ReadError::NoSuchBucket => ReadError::NoSuchBucket,
        ReadError::AccessDenied => ReadError::AccessDenied,
        ReadError::NoSuchKey => ReadError::NoSuchKey,
        ReadError::InvalidRange => ReadError::InvalidRange,
        ReadError::InvalidPart => ReadError::InvalidPart,
        ReadError::Backend(message) => ReadError::Backend(message.clone()),
    }
}

/// Locks the small fill registry and recovers from a poisoned worker lock.
fn lock_fills(
    fills: &Mutex<HashMap<BlockEntryKey, SharedFill>>,
) -> std::sync::MutexGuard<'_, HashMap<BlockEntryKey, SharedFill>> {
    match fills.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

/// Carves the mapping cache out of the configured DRAM ceiling.
fn mapping_budget(dram: u64) -> usize {
    let requested = (dram / 16).min(MAPPING_BUDGET_BYTES as u64);
    requested.max(4096) as usize
}

/// Returns the block cache budget after reserving mapping capacity.
fn block_memory_budget(dram: u64) -> Result<usize, EngineError> {
    let mapping = mapping_budget(dram) as u64;
    let block = dram.saturating_sub(mapping);
    if block < MIN_BLOCK_MEMORY_BYTES {
        return Err(EngineError::DramBudgetTooSmall(dram));
    }
    checked_usize(block).ok_or(EngineError::DramBudgetTooSmall(dram))
}

/// Converts a validated byte budget to a platform-sized integer.
fn checked_usize(value: u64) -> Option<usize> {
    usize::try_from(value).ok()
}
