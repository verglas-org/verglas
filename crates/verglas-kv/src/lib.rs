//! Tenant-scoped persistent key/value storage backed by a checksummed durable
//! local log and a bounded, rebuildable RAM value tier. The durable files are a
//! separate storage domain and are never visible to object-cache eviction.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use arc_swap::ArcSwap;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

const LOG_FILE: &str = "kv.data";
const COMPACTION_FILE: &str = "kv.compact";
const FRAME_HEADER_BYTES: u64 = 8;
const MAX_SCOPE_BYTES: usize = 256;
const MAX_KEY_BYTES: usize = 2_048;
const MAX_VALUE_BYTES: usize = 8 * 1024 * 1024;
const MAX_CONTENT_TYPE_BYTES: usize = 255;
const MAX_METADATA_ENTRIES: usize = 32;
const MAX_METADATA_KEY_BYTES: usize = 128;
const MAX_METADATA_VALUE_BYTES: usize = 1_024;
const MAX_METADATA_BYTES: usize = 8 * 1024;
const MAX_IDEMPOTENCY_KEY_BYTES: usize = 255;
const MAX_LIST_LIMIT: usize = 1_000;

/// Storage limits for one KV engine.
#[derive(Debug, Clone, Copy)]
pub struct StoreConfig {
    /// Maximum durable bytes owned by the KV storage domain.
    pub capacity_bytes: u64,
    /// Maximum value bytes retained in the rebuildable RAM tier.
    pub ram_bytes: u64,
}

/// Tenant and namespace resolved by the authentication boundary.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Scope {
    tenant: String,
    namespace: String,
}

impl Scope {
    /// Validates and constructs one tenant/namespace scope.
    pub fn new(tenant: impl Into<String>, namespace: impl Into<String>) -> Result<Self, Error> {
        let tenant = tenant.into();
        let namespace = namespace.into();
        validate_scope_component("tenant", &tenant)?;
        validate_scope_component("namespace", &namespace)?;
        Ok(Self { tenant, namespace })
    }

    /// Returns the tenant identifier.
    pub fn tenant(&self) -> &str {
        &self.tenant
    }

    /// Returns the namespace identifier.
    pub fn namespace(&self) -> &str {
        &self.namespace
    }
}

/// Optional metadata and write conditions for a put.
#[derive(Debug, Clone, Default)]
pub struct PutOptions {
    /// MIME type returned with the value.
    pub content_type: Option<String>,
    /// Absolute Unix expiration time in milliseconds.
    pub expires_at_ms: Option<u64>,
    /// Bounded application metadata.
    pub metadata: BTreeMap<String, String>,
    /// Required current version.
    pub if_match: Option<String>,
    /// Requires the key to have no live value.
    pub create_only: bool,
    /// Retry identity for one logical write.
    pub idempotency_key: Option<String>,
}

/// Optional conditional-delete requirement.
#[derive(Debug, Clone, Default)]
pub struct DeleteOptions {
    /// Required current version.
    pub if_match: Option<String>,
}

/// The local tier that answered a read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadTier {
    /// The bounded in-process value cache.
    Ram,
    /// The authoritative local durable log.
    Nvme,
}

/// A live value and its bounded metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Value {
    /// Raw value bytes.
    pub bytes: Bytes,
    /// MIME type supplied on write.
    pub content_type: Option<String>,
    /// Committed opaque version.
    pub version: String,
    /// Commit time in Unix milliseconds.
    pub modified_at_ms: u64,
    /// Logical expiration time.
    pub expires_at_ms: Option<u64>,
    /// Bounded application metadata.
    pub metadata: BTreeMap<String, String>,
    /// Tier that served this read.
    pub tier: ReadTier,
}

/// Result of one committed put.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutResult {
    /// Opaque committed version.
    pub version: String,
    /// Whether an earlier idempotent result was replayed.
    pub idempotent: bool,
}

/// Result of one idempotent delete.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteResult {
    /// Whether a live value was removed.
    pub removed: bool,
}

/// One metadata-only list entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListEntry {
    /// Key bytes represented as UTF-8.
    pub key: String,
    /// Current opaque version.
    pub version: String,
    /// Commit time in Unix milliseconds.
    pub modified_at_ms: u64,
    /// Logical expiration time.
    pub expires_at_ms: Option<u64>,
    /// MIME type supplied on write.
    pub content_type: Option<String>,
    /// Bounded application metadata.
    pub metadata: BTreeMap<String, String>,
}

/// One bounded deterministic metadata page.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListPage {
    /// Entries in bytewise key order.
    pub entries: Vec<ListEntry>,
    /// Opaque continuation cursor when more entries remain.
    pub next_cursor: Option<String>,
}

/// Snapshot of bounded observability counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MetricsSnapshot {
    /// Total get requests.
    pub get_requests: u64,
    /// Total put requests.
    pub put_requests: u64,
    /// Total delete requests.
    pub delete_requests: u64,
    /// Total list requests.
    pub list_requests: u64,
    /// Successful hot reads.
    pub ram_hits: u64,
    /// Successful cold reads.
    pub nvme_hits: u64,
    /// Absent reads.
    pub misses: u64,
    /// Reads suppressed by logical expiration.
    pub expired_reads: u64,
    /// Cumulative NVMe read latency in microseconds.
    pub nvme_read_micros: u64,
    /// Value bytes read.
    pub bytes_read: u64,
    /// Value bytes durably written.
    pub bytes_written: u64,
    /// Writes rejected by the durable capacity ceiling.
    pub capacity_failures: u64,
    /// Conditional operations rejected by compare-and-set.
    pub cas_conflicts: u64,
    /// Startup recovery duration in microseconds.
    pub recovery_micros: u64,
    /// Failed compaction attempts.
    pub compaction_failures: u64,
    /// Failed cleanup attempts.
    pub cleanup_failures: u64,
}

/// KV storage failures.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum Error {
    /// A key, scope, option, record, or cursor is invalid.
    #[error("invalid KV input: {0}")]
    Invalid(String),
    /// A conditional operation did not match the live version.
    #[error("KV condition did not match")]
    Precondition,
    /// The durable capacity ceiling cannot accept the record.
    #[error("KV capacity exhausted")]
    Capacity,
    /// Durable local storage failed.
    #[error("KV storage failed: {0}")]
    Storage(String),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
struct EntryKey {
    scope: Scope,
    key: String,
}

#[derive(Debug, Clone)]
struct IndexEntry {
    version: u64,
    modified_at_ms: u64,
    expires_at_ms: Option<u64>,
    content_type: Option<String>,
    metadata: BTreeMap<String, String>,
    tombstone: bool,
    frame_offset: u64,
    frame_len: u32,
    idempotency_key: Option<String>,
}

impl IndexEntry {
    /// Whether this index entry names a value visible at `now_ms`.
    fn is_live(&self, now_ms: u64) -> bool {
        !self.tombstone && self.expires_at_ms.is_none_or(|expiry| expiry > now_ms)
    }

    /// Projects this entry into the metadata-only listing contract.
    fn list_entry(&self, key: &str) -> ListEntry {
        ListEntry {
            key: key.to_owned(),
            version: render_version(self.version),
            modified_at_ms: self.modified_at_ms,
            expires_at_ms: self.expires_at_ms,
            content_type: self.content_type.clone(),
            metadata: self.metadata.clone(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct DiskRecord {
    scope: Scope,
    key: String,
    version: u64,
    modified_at_ms: u64,
    expires_at_ms: Option<u64>,
    content_type: Option<String>,
    metadata: BTreeMap<String, String>,
    value: Option<Vec<u8>>,
    idempotency_key: Option<String>,
}

impl DiskRecord {
    /// Returns the compound key this record updates.
    fn entry_key(&self) -> EntryKey {
        EntryKey {
            scope: self.scope.clone(),
            key: self.key.clone(),
        }
    }

    /// Builds the in-memory index entry for a persisted frame.
    fn index_entry(&self, frame_offset: u64, frame_len: u32) -> IndexEntry {
        IndexEntry {
            version: self.version,
            modified_at_ms: self.modified_at_ms,
            expires_at_ms: self.expires_at_ms,
            content_type: self.content_type.clone(),
            metadata: self.metadata.clone(),
            tombstone: self.value.is_none(),
            frame_offset,
            frame_len,
            idempotency_key: self.idempotency_key.clone(),
        }
    }
}

#[derive(Default)]
struct Snapshot {
    index: BTreeMap<EntryKey, IndexEntry>,
    hot: HashMap<EntryKey, Arc<Bytes>>,
}

struct State {
    log: File,
    log_bytes: u64,
    next_version: u64,
    index: BTreeMap<EntryKey, IndexEntry>,
    idempotency: HashMap<(EntryKey, String), u64>,
    hot: HashMap<EntryKey, Arc<Bytes>>,
    hot_order: VecDeque<EntryKey>,
    hot_bytes: u64,
}

#[derive(Default)]
struct Metrics {
    get_requests: AtomicU64,
    put_requests: AtomicU64,
    delete_requests: AtomicU64,
    list_requests: AtomicU64,
    ram_hits: AtomicU64,
    nvme_hits: AtomicU64,
    misses: AtomicU64,
    expired_reads: AtomicU64,
    nvme_read_micros: AtomicU64,
    bytes_read: AtomicU64,
    bytes_written: AtomicU64,
    capacity_failures: AtomicU64,
    cas_conflicts: AtomicU64,
    recovery_micros: AtomicU64,
    compaction_failures: AtomicU64,
    cleanup_failures: AtomicU64,
}

impl Metrics {
    /// Captures all bounded counters without resetting them.
    fn snapshot(&self) -> MetricsSnapshot {
        let load = |value: &AtomicU64| value.load(Ordering::Relaxed);
        MetricsSnapshot {
            get_requests: load(&self.get_requests),
            put_requests: load(&self.put_requests),
            delete_requests: load(&self.delete_requests),
            list_requests: load(&self.list_requests),
            ram_hits: load(&self.ram_hits),
            nvme_hits: load(&self.nvme_hits),
            misses: load(&self.misses),
            expired_reads: load(&self.expired_reads),
            nvme_read_micros: load(&self.nvme_read_micros),
            bytes_read: load(&self.bytes_read),
            bytes_written: load(&self.bytes_written),
            capacity_failures: load(&self.capacity_failures),
            cas_conflicts: load(&self.cas_conflicts),
            recovery_micros: load(&self.recovery_micros),
            compaction_failures: load(&self.compaction_failures),
            cleanup_failures: load(&self.cleanup_failures),
        }
    }
}

struct Inner {
    dir: PathBuf,
    config: StoreConfig,
    capacity_ceiling: AtomicU64,
    state: Mutex<State>,
    snapshot: ArcSwap<Snapshot>,
    metrics: Metrics,
}

/// A cloneable handle to one durable KV storage domain.
#[derive(Clone)]
pub struct Store(Arc<Inner>);

impl Store {
    /// Opens and recovers a store beneath `data_dir`.
    pub fn open(data_dir: &Path, config: StoreConfig) -> Result<Self, Error> {
        validate_config(config)?;
        let recovery_start = Instant::now();
        std::fs::create_dir_all(data_dir).map_err(storage_error)?;
        let path = data_dir.join(LOG_FILE);
        let mut log = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)
            .map_err(storage_error)?;
        let (index, idempotency, next_version, valid_len) =
            recover(&mut log, config.capacity_bytes)?;
        let file_len = log.metadata().map_err(storage_error)?.len();
        if valid_len < file_len {
            log.set_len(valid_len).map_err(storage_error)?;
            log.sync_data().map_err(storage_error)?;
        }
        if valid_len > config.capacity_bytes {
            return Err(Error::Storage(
                "durable log exceeds the configured KV capacity".to_owned(),
            ));
        }
        let snapshot = Arc::new(Snapshot {
            index: index.clone(),
            hot: HashMap::new(),
        });
        let metrics = Metrics::default();
        metrics.recovery_micros.store(
            saturating_micros(recovery_start.elapsed()),
            Ordering::Relaxed,
        );
        let capacity_ceiling = AtomicU64::new(config.capacity_bytes);
        Ok(Self(Arc::new(Inner {
            dir: data_dir.to_owned(),
            config,
            capacity_ceiling,
            state: Mutex::new(State {
                log,
                log_bytes: valid_len,
                next_version,
                index,
                idempotency,
                hot: HashMap::new(),
                hot_order: VecDeque::new(),
                hot_bytes: 0,
            }),
            snapshot: ArcSwap::new(snapshot),
            metrics,
        })))
    }

    /// Returns one live value, suppressing logically expired bytes.
    pub fn get(&self, scope: &Scope, key: &str) -> Result<Option<Value>, Error> {
        self.0.metrics.get_requests.fetch_add(1, Ordering::Relaxed);
        validate_key(key)?;
        let compound = EntryKey {
            scope: scope.clone(),
            key: key.to_owned(),
        };
        let now = now_ms()?;
        let snapshot = self.0.snapshot.load();
        let Some(entry) = snapshot.index.get(&compound) else {
            self.0.metrics.misses.fetch_add(1, Ordering::Relaxed);
            return Ok(None);
        };
        if entry.tombstone {
            self.0.metrics.misses.fetch_add(1, Ordering::Relaxed);
            return Ok(None);
        }
        if !entry.is_live(now) {
            self.0.metrics.expired_reads.fetch_add(1, Ordering::Relaxed);
            self.0.metrics.misses.fetch_add(1, Ordering::Relaxed);
            return Ok(None);
        }
        if let Some(bytes) = snapshot.hot.get(&compound) {
            self.0.metrics.ram_hits.fetch_add(1, Ordering::Relaxed);
            self.0
                .metrics
                .bytes_read
                .fetch_add(bytes.len() as u64, Ordering::Relaxed);
            return Ok(Some(value_from_entry(
                entry,
                bytes.as_ref().clone(),
                ReadTier::Ram,
            )));
        }
        drop(snapshot);

        let started = Instant::now();
        let mut state = self.lock_state()?;
        let Some(entry) = state.index.get(&compound).cloned() else {
            self.0.metrics.misses.fetch_add(1, Ordering::Relaxed);
            return Ok(None);
        };
        let current_now = now_ms()?;
        if !entry.is_live(current_now) {
            if !entry.tombstone {
                self.0.metrics.expired_reads.fetch_add(1, Ordering::Relaxed);
            }
            self.0.metrics.misses.fetch_add(1, Ordering::Relaxed);
            return Ok(None);
        }
        let record = read_record_at(&mut state.log, entry.frame_offset, entry.frame_len)?;
        if record.version != entry.version || record.entry_key() != compound {
            return Err(Error::Storage(
                "durable index does not match its record".to_owned(),
            ));
        }
        let bytes = record
            .value
            .map(Bytes::from)
            .ok_or_else(|| Error::Storage("live index points at a tombstone".to_owned()))?;
        promote_hot(
            &mut state,
            &compound,
            bytes.clone(),
            self.0.config.ram_bytes,
        );
        self.publish(&state);
        self.0.metrics.nvme_hits.fetch_add(1, Ordering::Relaxed);
        self.0
            .metrics
            .nvme_read_micros
            .fetch_add(saturating_micros(started.elapsed()), Ordering::Relaxed);
        self.0
            .metrics
            .bytes_read
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        Ok(Some(value_from_entry(&entry, bytes, ReadTier::Nvme)))
    }

    /// Durably commits one conditional, optionally idempotent value.
    pub fn put(
        &self,
        scope: &Scope,
        key: &str,
        value: Bytes,
        options: PutOptions,
    ) -> Result<PutResult, Error> {
        self.0.metrics.put_requests.fetch_add(1, Ordering::Relaxed);
        validate_key(key)?;
        validate_put(&value, &options)?;
        let compound = EntryKey {
            scope: scope.clone(),
            key: key.to_owned(),
        };
        let mut state = self.lock_state()?;
        if let Some(idempotency_key) = &options.idempotency_key
            && let Some(version) = state
                .idempotency
                .get(&(compound.clone(), idempotency_key.clone()))
        {
            return Ok(PutResult {
                version: render_version(*version),
                idempotent: true,
            });
        }
        let now = now_ms()?;
        let live = state
            .index
            .get(&compound)
            .filter(|entry| entry.is_live(now));
        if options.create_only && live.is_some() {
            self.0.metrics.cas_conflicts.fetch_add(1, Ordering::Relaxed);
            return Err(Error::Precondition);
        }
        if let Some(expected) = &options.if_match {
            let matches = live.is_some_and(|entry| render_version(entry.version) == *expected);
            if !matches {
                self.0.metrics.cas_conflicts.fetch_add(1, Ordering::Relaxed);
                return Err(Error::Precondition);
            }
        }
        let version = state.next_version;
        let record = DiskRecord {
            scope: scope.clone(),
            key: key.to_owned(),
            version,
            modified_at_ms: now,
            expires_at_ms: options.expires_at_ms,
            content_type: options.content_type,
            metadata: options.metadata,
            value: Some(value.to_vec()),
            idempotency_key: options.idempotency_key,
        };
        let (frame_offset, frame_len) = self.append_with_capacity(&mut state, &record)?;
        state.next_version = state.next_version.saturating_add(1);
        let index_entry = record.index_entry(frame_offset, frame_len);
        if let Some(idempotency_key) = &index_entry.idempotency_key {
            state
                .idempotency
                .insert((compound.clone(), idempotency_key.clone()), version);
        }
        state.index.insert(compound.clone(), index_entry);
        promote_hot(
            &mut state,
            &compound,
            value.clone(),
            self.0.config.ram_bytes,
        );
        self.publish(&state);
        self.0
            .metrics
            .bytes_written
            .fetch_add(value.len() as u64, Ordering::Relaxed);
        Ok(PutResult {
            version: render_version(version),
            idempotent: false,
        })
    }

    /// Durably records an idempotent conditional deletion.
    pub fn delete(
        &self,
        scope: &Scope,
        key: &str,
        options: DeleteOptions,
    ) -> Result<DeleteResult, Error> {
        self.0
            .metrics
            .delete_requests
            .fetch_add(1, Ordering::Relaxed);
        validate_key(key)?;
        let compound = EntryKey {
            scope: scope.clone(),
            key: key.to_owned(),
        };
        let mut state = self.lock_state()?;
        let now = now_ms()?;
        let live = state
            .index
            .get(&compound)
            .filter(|entry| entry.is_live(now));
        if let Some(expected) = &options.if_match {
            let matches = live.is_some_and(|entry| render_version(entry.version) == *expected);
            if !matches {
                self.0.metrics.cas_conflicts.fetch_add(1, Ordering::Relaxed);
                return Err(Error::Precondition);
            }
        }
        if live.is_none() {
            return Ok(DeleteResult { removed: false });
        }
        let version = state.next_version;
        let record = DiskRecord {
            scope: scope.clone(),
            key: key.to_owned(),
            version,
            modified_at_ms: now,
            expires_at_ms: None,
            content_type: None,
            metadata: BTreeMap::new(),
            value: None,
            idempotency_key: None,
        };
        let (frame_offset, frame_len) = self.append_with_capacity(&mut state, &record)?;
        state.next_version = state.next_version.saturating_add(1);
        state.index.insert(
            compound.clone(),
            record.index_entry(frame_offset, frame_len),
        );
        remove_hot(&mut state, &compound);
        self.publish(&state);
        Ok(DeleteResult { removed: true })
    }

    /// Lists a bounded metadata-only page in deterministic bytewise order.
    pub fn list(
        &self,
        scope: &Scope,
        prefix: &str,
        limit: usize,
        cursor: Option<&str>,
    ) -> Result<ListPage, Error> {
        self.0.metrics.list_requests.fetch_add(1, Ordering::Relaxed);
        if limit == 0 || limit > MAX_LIST_LIMIT {
            return Err(Error::Invalid(format!(
                "list limit must be between 1 and {MAX_LIST_LIMIT}"
            )));
        }
        if prefix.len() > MAX_KEY_BYTES {
            return Err(Error::Invalid("prefix is too long".to_owned()));
        }
        let after = cursor
            .map(|value| decode_cursor(value, prefix))
            .transpose()?;
        let now = now_ms()?;
        let snapshot = self.0.snapshot.load();
        let mut matching = snapshot.index.iter().filter(|(entry_key, entry)| {
            entry_key.scope == *scope
                && entry_key.key.starts_with(prefix)
                && after.as_ref().is_none_or(|last| entry_key.key > *last)
                && entry.is_live(now)
        });
        let mut entries = Vec::with_capacity(limit);
        for (entry_key, entry) in matching.by_ref().take(limit) {
            entries.push(entry.list_entry(&entry_key.key));
        }
        let has_more = matching.next().is_some();
        let next_cursor = if has_more {
            entries
                .last()
                .map(|entry| encode_cursor(prefix, &entry.key))
                .transpose()?
        } else {
            None
        };
        Ok(ListPage {
            entries,
            next_cursor,
        })
    }

    /// Rewrites the durable log to its latest committed records.
    pub fn compact(&self) -> Result<(), Error> {
        let mut state = self.lock_state()?;
        if let Err(error) = compact_locked(&self.0.dir, &mut state) {
            self.0
                .metrics
                .compaction_failures
                .fetch_add(1, Ordering::Relaxed);
            return Err(error);
        }
        self.publish(&state);
        Ok(())
    }

    /// Returns a bounded metrics snapshot.
    pub fn metrics(&self) -> MetricsSnapshot {
        self.0.metrics.snapshot()
    }

    /// Returns bytes held by the durable log for shared-disk accounting.
    pub fn used_bytes(&self) -> u64 {
        self.0
            .state
            .lock()
            .map_or(self.0.config.capacity_bytes, |state| state.log_bytes)
    }

    /// Publishes the currently available shared-disk ceiling without evicting KV data.
    pub fn set_capacity_ceiling(&self, capacity_bytes: u64) {
        let used = self.used_bytes();
        self.0.capacity_ceiling.store(
            capacity_bytes.min(self.0.config.capacity_bytes).max(used),
            Ordering::Release,
        );
    }

    /// Renders the KV engine's bounded Prometheus exposition.
    pub fn render_metrics(&self) -> String {
        let m = self.metrics();
        format!(
            concat!(
                "# TYPE verglas_kv_requests_total counter\n",
                "verglas_kv_requests_total{{operation=\"get\"}} {}\n",
                "verglas_kv_requests_total{{operation=\"put\"}} {}\n",
                "verglas_kv_requests_total{{operation=\"delete\"}} {}\n",
                "verglas_kv_requests_total{{operation=\"list\"}} {}\n",
                "# TYPE verglas_kv_hits_total counter\n",
                "verglas_kv_hits_total{{tier=\"ram\"}} {}\n",
                "verglas_kv_hits_total{{tier=\"nvme\"}} {}\n",
                "# TYPE verglas_kv_misses_total counter\n",
                "verglas_kv_misses_total {}\n",
                "# TYPE verglas_kv_expired_reads_total counter\n",
                "verglas_kv_expired_reads_total {}\n",
                "# TYPE verglas_kv_nvme_read_seconds_total counter\n",
                "verglas_kv_nvme_read_seconds_total {:.6}\n",
                "# TYPE verglas_kv_bytes_total counter\n",
                "verglas_kv_bytes_total{{direction=\"read\"}} {}\n",
                "verglas_kv_bytes_total{{direction=\"written\"}} {}\n",
                "# TYPE verglas_kv_capacity_failures_total counter\n",
                "verglas_kv_capacity_failures_total {}\n",
                "# TYPE verglas_kv_cas_conflicts_total counter\n",
                "verglas_kv_cas_conflicts_total {}\n",
                "# TYPE verglas_kv_recovery_seconds gauge\n",
                "verglas_kv_recovery_seconds {:.6}\n",
                "# TYPE verglas_kv_compaction_failures_total counter\n",
                "verglas_kv_compaction_failures_total {}\n",
                "# TYPE verglas_kv_cleanup_failures_total counter\n",
                "verglas_kv_cleanup_failures_total {}\n"
            ),
            m.get_requests,
            m.put_requests,
            m.delete_requests,
            m.list_requests,
            m.ram_hits,
            m.nvme_hits,
            m.misses,
            m.expired_reads,
            m.nvme_read_micros as f64 / 1_000_000.0,
            m.bytes_read,
            m.bytes_written,
            m.capacity_failures,
            m.cas_conflicts,
            m.recovery_micros as f64 / 1_000_000.0,
            m.compaction_failures,
            m.cleanup_failures,
        )
    }

    /// Locks the serial mutation state and reports poisoning honestly.
    fn lock_state(&self) -> Result<MutexGuard<'_, State>, Error> {
        self.0
            .state
            .lock()
            .map_err(|_| Error::Storage("KV mutation state is poisoned".to_owned()))
    }

    /// Publishes a read-only index/hot-tier snapshot after a durable mutation.
    fn publish(&self, state: &State) {
        self.0.snapshot.store(Arc::new(Snapshot {
            index: state.index.clone(),
            hot: state.hot.clone(),
        }));
    }

    /// Appends and flushes one record, compacting once before capacity refusal.
    fn append_with_capacity(
        &self,
        state: &mut State,
        record: &DiskRecord,
    ) -> Result<(u64, u32), Error> {
        let frame = encode_frame(record)?;
        let frame_bytes = frame.len() as u64;
        let capacity = self.0.capacity_ceiling.load(Ordering::Acquire);
        if frame_bytes > capacity {
            self.0
                .metrics
                .capacity_failures
                .fetch_add(1, Ordering::Relaxed);
            return Err(Error::Capacity);
        }
        if state.log_bytes.saturating_add(frame_bytes) > capacity
            && let Err(error) = compact_locked(&self.0.dir, state)
        {
            self.0
                .metrics
                .compaction_failures
                .fetch_add(1, Ordering::Relaxed);
            return Err(error);
        }
        if state.log_bytes.saturating_add(frame_bytes) > capacity {
            self.0
                .metrics
                .capacity_failures
                .fetch_add(1, Ordering::Relaxed);
            return Err(Error::Capacity);
        }
        append_frame(&mut state.log, &mut state.log_bytes, &frame)
    }
}

/// Validates a tenant or namespace without permitting path-like syntax.
fn validate_scope_component(label: &str, value: &str) -> Result<(), Error> {
    if value.is_empty() || value.len() > MAX_SCOPE_BYTES {
        return Err(Error::Invalid(format!(
            "{label} must contain 1 to {MAX_SCOPE_BYTES} bytes"
        )));
    }
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(Error::Invalid(format!(
            "{label} contains unsupported characters"
        )));
    }
    Ok(())
}

/// Validates a key before any index or durable lookup.
fn validate_key(key: &str) -> Result<(), Error> {
    if key.is_empty() || key.len() > MAX_KEY_BYTES {
        return Err(Error::Invalid(format!(
            "key must contain 1 to {MAX_KEY_BYTES} bytes"
        )));
    }
    if key.contains('\0') {
        return Err(Error::Invalid("key contains a NUL byte".to_owned()));
    }
    Ok(())
}

/// Validates hard storage limits.
fn validate_config(config: StoreConfig) -> Result<(), Error> {
    if config.capacity_bytes == 0 {
        return Err(Error::Invalid("capacity_bytes must be positive".to_owned()));
    }
    if config.ram_bytes > config.capacity_bytes {
        return Err(Error::Invalid(
            "ram_bytes cannot exceed capacity_bytes".to_owned(),
        ));
    }
    Ok(())
}

/// Validates all bounded put fields before serializing a record.
fn validate_put(value: &Bytes, options: &PutOptions) -> Result<(), Error> {
    if value.len() > MAX_VALUE_BYTES {
        return Err(Error::Invalid(format!(
            "value exceeds {MAX_VALUE_BYTES} bytes"
        )));
    }
    if options
        .content_type
        .as_ref()
        .is_some_and(|value| value.is_empty() || value.len() > MAX_CONTENT_TYPE_BYTES)
    {
        return Err(Error::Invalid("content type is invalid".to_owned()));
    }
    if options
        .idempotency_key
        .as_ref()
        .is_some_and(|value| value.is_empty() || value.len() > MAX_IDEMPOTENCY_KEY_BYTES)
    {
        return Err(Error::Invalid("idempotency key is invalid".to_owned()));
    }
    if options.metadata.len() > MAX_METADATA_ENTRIES {
        return Err(Error::Invalid("too many metadata entries".to_owned()));
    }
    let mut total = 0usize;
    for (key, value) in &options.metadata {
        if key.is_empty() || key.len() > MAX_METADATA_KEY_BYTES {
            return Err(Error::Invalid("metadata key is invalid".to_owned()));
        }
        if value.len() > MAX_METADATA_VALUE_BYTES {
            return Err(Error::Invalid("metadata value is too long".to_owned()));
        }
        total = total.saturating_add(key.len()).saturating_add(value.len());
    }
    if total > MAX_METADATA_BYTES {
        return Err(Error::Invalid("metadata exceeds its byte limit".to_owned()));
    }
    Ok(())
}

/// Renders a committed version as an opaque quoted decimal ETag token.
fn render_version(version: u64) -> String {
    format!("\"{version}\"")
}

/// Reads wall-clock Unix milliseconds for TTL and metadata timestamps.
fn now_ms() -> Result<u64, Error> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Error::Storage("system clock is before the Unix epoch".to_owned()))?;
    Ok(duration.as_millis().try_into().unwrap_or(u64::MAX))
}

/// Converts a duration to a saturating microsecond counter.
fn saturating_micros(duration: std::time::Duration) -> u64 {
    duration.as_micros().try_into().unwrap_or(u64::MAX)
}

/// Maps an I/O error without including a key, value, or metadata field.
fn storage_error(error: std::io::Error) -> Error {
    Error::Storage(error.to_string())
}

/// Converts one index entry and byte buffer into the public read contract.
fn value_from_entry(entry: &IndexEntry, bytes: Bytes, tier: ReadTier) -> Value {
    Value {
        bytes,
        content_type: entry.content_type.clone(),
        version: render_version(entry.version),
        modified_at_ms: entry.modified_at_ms,
        expires_at_ms: entry.expires_at_ms,
        metadata: entry.metadata.clone(),
        tier,
    }
}

/// Adds a value to the bounded RAM tier, evicting only rebuildable hot copies.
fn promote_hot(state: &mut State, key: &EntryKey, value: Bytes, capacity: u64) {
    remove_hot(state, key);
    if value.len() as u64 > capacity || capacity == 0 {
        return;
    }
    let value = Arc::new(value);
    state.hot_bytes = state.hot_bytes.saturating_add(value.len() as u64);
    state.hot.insert(key.clone(), value);
    state.hot_order.push_back(key.clone());
    while state.hot_bytes > capacity {
        let Some(oldest) = state.hot_order.pop_front() else {
            break;
        };
        if let Some(removed) = state.hot.remove(&oldest) {
            state.hot_bytes = state.hot_bytes.saturating_sub(removed.len() as u64);
        }
    }
}

/// Removes one rebuildable hot copy without touching the durable index.
fn remove_hot(state: &mut State, key: &EntryKey) {
    if let Some(removed) = state.hot.remove(key) {
        state.hot_bytes = state.hot_bytes.saturating_sub(removed.len() as u64);
    }
    state.hot_order.retain(|entry| entry != key);
}

/// Encodes one checksummed append frame.
fn encode_frame(record: &DiskRecord) -> Result<Vec<u8>, Error> {
    let payload = serde_json::to_vec(record)
        .map_err(|_| Error::Storage("could not encode a durable record".to_owned()))?;
    let len: u32 = payload
        .len()
        .try_into()
        .map_err(|_| Error::Invalid("durable record is too large".to_owned()))?;
    let mut frame = Vec::with_capacity(payload.len() + FRAME_HEADER_BYTES as usize);
    frame.extend_from_slice(&len.to_le_bytes());
    frame.extend_from_slice(&crc32c::crc32c(&payload).to_le_bytes());
    frame.extend_from_slice(&payload);
    Ok(frame)
}

/// Appends and flushes one complete frame before returning its location.
fn append_frame(log: &mut File, log_bytes: &mut u64, frame: &[u8]) -> Result<(u64, u32), Error> {
    let offset = *log_bytes;
    log.seek(SeekFrom::End(0)).map_err(storage_error)?;
    log.write_all(frame).map_err(storage_error)?;
    log.sync_data().map_err(storage_error)?;
    *log_bytes = log_bytes.saturating_add(frame.len() as u64);
    let frame_len = frame
        .len()
        .try_into()
        .map_err(|_| Error::Storage("durable frame length overflow".to_owned()))?;
    Ok((offset, frame_len))
}

/// Reads and verifies one indexed frame.
fn read_record_at(log: &mut File, offset: u64, frame_len: u32) -> Result<DiskRecord, Error> {
    if u64::from(frame_len) < FRAME_HEADER_BYTES {
        return Err(Error::Storage("durable frame is too short".to_owned()));
    }
    log.seek(SeekFrom::Start(offset)).map_err(storage_error)?;
    let mut frame = vec![0; frame_len as usize];
    log.read_exact(&mut frame).map_err(storage_error)?;
    decode_frame(&frame)
}

/// Verifies and decodes one complete frame.
fn decode_frame(frame: &[u8]) -> Result<DiskRecord, Error> {
    if frame.len() < FRAME_HEADER_BYTES as usize {
        return Err(Error::Storage("durable frame is truncated".to_owned()));
    }
    let len = u32::from_le_bytes([frame[0], frame[1], frame[2], frame[3]]) as usize;
    let expected_crc = u32::from_le_bytes([frame[4], frame[5], frame[6], frame[7]]);
    if len != frame.len() - FRAME_HEADER_BYTES as usize {
        return Err(Error::Storage("durable frame length is invalid".to_owned()));
    }
    let payload = &frame[FRAME_HEADER_BYTES as usize..];
    if crc32c::crc32c(payload) != expected_crc {
        return Err(Error::Storage("durable frame checksum mismatch".to_owned()));
    }
    let record: DiskRecord = serde_json::from_slice(payload)
        .map_err(|_| Error::Storage("durable record encoding is invalid".to_owned()))?;
    validate_recovered_record(&record)?;
    Ok(record)
}

/// Applies ordinary input bounds to an on-disk record before indexing it.
fn validate_recovered_record(record: &DiskRecord) -> Result<(), Error> {
    validate_scope_component("tenant", record.scope.tenant())?;
    validate_scope_component("namespace", record.scope.namespace())?;
    validate_key(&record.key)?;
    if record.version == 0 {
        return Err(Error::Storage("durable record version is zero".to_owned()));
    }
    validate_put(
        &Bytes::copy_from_slice(record.value.as_deref().unwrap_or_default()),
        &PutOptions {
            content_type: record.content_type.clone(),
            expires_at_ms: record.expires_at_ms,
            metadata: record.metadata.clone(),
            if_match: None,
            create_only: false,
            idempotency_key: record.idempotency_key.clone(),
        },
    )
    .map_err(|_| Error::Storage("durable record exceeds field bounds".to_owned()))
}

type Recovery = (
    BTreeMap<EntryKey, IndexEntry>,
    HashMap<(EntryKey, String), u64>,
    u64,
    u64,
);

/// Scans complete frames, ignoring only an incomplete unacknowledged tail.
fn recover(log: &mut File, capacity: u64) -> Result<Recovery, Error> {
    log.seek(SeekFrom::Start(0)).map_err(storage_error)?;
    let file_len = log.metadata().map_err(storage_error)?.len();
    let mut offset = 0u64;
    let mut index = BTreeMap::new();
    let mut idempotency = HashMap::new();
    let mut next_version = 1u64;
    while offset < file_len {
        let remaining = file_len - offset;
        if remaining < FRAME_HEADER_BYTES {
            break;
        }
        let mut header = [0u8; FRAME_HEADER_BYTES as usize];
        log.read_exact(&mut header).map_err(storage_error)?;
        let payload_len = u32::from_le_bytes([header[0], header[1], header[2], header[3]]) as u64;
        let frame_len = FRAME_HEADER_BYTES.saturating_add(payload_len);
        if frame_len > capacity || payload_len == 0 {
            return Err(Error::Storage("durable frame length is invalid".to_owned()));
        }
        if frame_len > remaining {
            break;
        }
        let mut frame = Vec::with_capacity(frame_len as usize);
        frame.extend_from_slice(&header);
        frame.resize(frame_len as usize, 0);
        log.read_exact(&mut frame[FRAME_HEADER_BYTES as usize..])
            .map_err(storage_error)?;
        let record = decode_frame(&frame)?;
        let key = record.entry_key();
        let frame_len_u32 = frame_len
            .try_into()
            .map_err(|_| Error::Storage("durable frame length overflow".to_owned()))?;
        let entry = record.index_entry(offset, frame_len_u32);
        if let Some(idempotency_key) = &entry.idempotency_key {
            idempotency.insert((key.clone(), idempotency_key.clone()), record.version);
        }
        next_version = next_version.max(record.version.saturating_add(1));
        index.insert(key, entry);
        offset = offset.saturating_add(frame_len);
    }
    Ok((index, idempotency, next_version, offset))
}

/// Rewrites latest records into a new file and atomically installs it.
fn compact_locked(dir: &Path, state: &mut State) -> Result<(), Error> {
    let temp_path = dir.join(COMPACTION_FILE);
    let mut temp = OpenOptions::new()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        .open(&temp_path)
        .map_err(storage_error)?;
    let mut compacted_len = 0u64;
    let mut rebuilt = BTreeMap::new();
    let entries: Vec<_> = state
        .index
        .iter()
        .map(|(key, entry)| (key.clone(), entry.clone()))
        .collect();
    for (key, entry) in entries {
        let mut record = read_record_at(&mut state.log, entry.frame_offset, entry.frame_len)?;
        if record.entry_key() != key || record.version != entry.version {
            return Err(Error::Storage(
                "durable index does not match during compaction".to_owned(),
            ));
        }
        if !entry.is_live(now_ms()?) && !entry.tombstone {
            record.value = None;
            record.content_type = None;
            record.metadata.clear();
            record.expires_at_ms = None;
        }
        let frame = encode_frame(&record)?;
        let offset = compacted_len;
        temp.write_all(&frame).map_err(storage_error)?;
        compacted_len = compacted_len.saturating_add(frame.len() as u64);
        let frame_len = frame
            .len()
            .try_into()
            .map_err(|_| Error::Storage("durable frame length overflow".to_owned()))?;
        rebuilt.insert(key, record.index_entry(offset, frame_len));
    }
    temp.sync_all().map_err(storage_error)?;
    std::fs::rename(&temp_path, dir.join(LOG_FILE)).map_err(storage_error)?;
    File::open(dir)
        .and_then(|file| file.sync_all())
        .map_err(storage_error)?;
    state.log = OpenOptions::new()
        .read(true)
        .append(true)
        .open(dir.join(LOG_FILE))
        .map_err(storage_error)?;
    state.log_bytes = compacted_len;
    state.index = rebuilt;
    Ok(())
}

/// Encodes a prefix-bound continuation cursor with an integrity checksum.
fn encode_cursor(prefix: &str, key: &str) -> Result<String, Error> {
    let prefix_len: u16 = prefix
        .len()
        .try_into()
        .map_err(|_| Error::Invalid("prefix is too long for a cursor".to_owned()))?;
    let mut bytes = Vec::with_capacity(2 + prefix.len() + key.len() + 4);
    bytes.extend_from_slice(&prefix_len.to_le_bytes());
    bytes.extend_from_slice(prefix.as_bytes());
    bytes.extend_from_slice(key.as_bytes());
    bytes.extend_from_slice(&crc32c::crc32c(&bytes).to_le_bytes());
    Ok(hex::encode(bytes))
}

/// Decodes a cursor and verifies it belongs to the requested prefix.
fn decode_cursor(cursor: &str, expected_prefix: &str) -> Result<String, Error> {
    let bytes =
        hex::decode(cursor).map_err(|_| Error::Invalid("cursor is malformed".to_owned()))?;
    if bytes.len() < 6 {
        return Err(Error::Invalid("cursor is malformed".to_owned()));
    }
    let checksum_offset = bytes.len() - 4;
    let expected_crc = u32::from_le_bytes([
        bytes[checksum_offset],
        bytes[checksum_offset + 1],
        bytes[checksum_offset + 2],
        bytes[checksum_offset + 3],
    ]);
    if crc32c::crc32c(&bytes[..checksum_offset]) != expected_crc {
        return Err(Error::Invalid("cursor checksum is invalid".to_owned()));
    }
    let prefix_len = u16::from_le_bytes([bytes[0], bytes[1]]) as usize;
    if 2 + prefix_len > checksum_offset {
        return Err(Error::Invalid("cursor is malformed".to_owned()));
    }
    let prefix = std::str::from_utf8(&bytes[2..2 + prefix_len])
        .map_err(|_| Error::Invalid("cursor prefix is invalid".to_owned()))?;
    if prefix != expected_prefix {
        return Err(Error::Invalid(
            "cursor does not belong to this prefix".to_owned(),
        ));
    }
    let key = std::str::from_utf8(&bytes[2 + prefix_len..checksum_offset])
        .map_err(|_| Error::Invalid("cursor key is invalid".to_owned()))?;
    validate_key(key)?;
    Ok(key.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A truncated last frame is discarded, while a bad complete checksum fails recovery.
    #[test]
    fn recovery_distinguishes_torn_tail_from_corruption() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(
            dir.path(),
            StoreConfig {
                capacity_bytes: 4096,
                ram_bytes: 64,
            },
        )
        .expect("open");
        let scope = Scope::new("tenant", "namespace").expect("scope");
        store
            .put(
                &scope,
                "key",
                Bytes::from_static(b"value"),
                PutOptions::default(),
            )
            .expect("put");
        drop(store);
        let path = dir.path().join(LOG_FILE);
        let original = std::fs::read(&path).expect("read");
        let mut torn = OpenOptions::new().append(true).open(&path).expect("append");
        torn.write_all(&[20, 0, 0, 0, 1]).expect("torn tail");
        drop(torn);
        let recovered = Store::open(
            dir.path(),
            StoreConfig {
                capacity_bytes: 4096,
                ram_bytes: 64,
            },
        )
        .expect("recover torn tail");
        assert!(recovered.get(&scope, "key").expect("get").is_some());
        drop(recovered);

        let mut corrupted = original;
        let last = corrupted.len() - 1;
        corrupted[last] ^= 0xff;
        std::fs::write(&path, corrupted).expect("corrupt");
        assert!(matches!(
            Store::open(
                dir.path(),
                StoreConfig {
                    capacity_bytes: 4096,
                    ram_bytes: 64,
                }
            ),
            Err(Error::Storage(_))
        ));
    }
}
