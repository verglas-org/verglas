//! Durable, actor-owned fragment segments for the EC write plane (#127).
//!
//! A cache member has exactly one append actor.  It owns rolling preallocated
//! segment files, coalesces queued requests into one `sync_data` barrier, and
//! exposes only records from a checksummed committed prefix after restart.
//! There is deliberately no per-fragment file, rename, directory sync, or
//! second local journal in the acknowledgement path.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock, mpsc};
use std::thread;

use bytes::Bytes;

/// Fixed marker for a self-describing fragment record.
const RECORD_MAGIC: [u8; 4] = *b"VGF2";
/// Fixed marker for a persisted group-commit boundary.
const COMMIT_MAGIC: [u8; 4] = *b"VGC2";
/// Segment allocation is outside a client commit barrier.
const SEGMENT_BYTES: u64 = 64 * 1024 * 1024;

/// Computes the Castagnoli checksum used to detect fragment corruption.
pub fn fragment_checksum(bytes: &[u8]) -> u32 {
    crc32c::crc32c(bytes)
}

/// Bytes recovered from one durable fragment record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadedFragment {
    /// Fragment payload.
    pub bytes: Bytes,
    /// Checksum persisted with `bytes`.
    pub checksum: u32,
}

impl LoadedFragment {
    /// Returns whether the recovered payload still has its persisted checksum.
    pub fn is_healthy(&self) -> bool {
        fragment_checksum(&self.bytes) == self.checksum
    }
}

/// Integrity state visible to the scrubber.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FragmentHealth {
    /// A record is present and valid.
    Healthy,
    /// A record is present but invalid.
    Corrupt,
    /// No live record names this key.
    Missing,
}

/// Stable identity of one EC fragment.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FragmentKey {
    /// Immutable transaction or object identity.
    pub object_id: String,
    /// EC fragment index.
    pub index: usize,
}

/// One self-describing record submitted to a cache member's append actor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FragmentRecord {
    /// Record identity.
    pub key: FragmentKey,
    /// Erasure-coded bytes.
    pub bytes: Bytes,
    /// CRC32C over `bytes`.
    pub checksum: u32,
}

impl FragmentRecord {
    /// Builds an integrity-checked fragment record.
    pub fn new(key: FragmentKey, bytes: Bytes) -> Self {
        Self {
            key,
            checksum: fragment_checksum(&bytes),
            bytes,
        }
    }
}

/// A failure in the durable fragment plane.
#[derive(Debug, thiserror::Error)]
pub enum FragmentIoError {
    /// A filesystem, actor, or malformed-record failure.
    #[error("fragment store: {0}")]
    Io(String),
    /// Accepting a new live payload would exceed the hard NVMe budget.
    #[error("fragment store full: {needed} bytes needed, {available} available under the budget")]
    Full {
        /// Requested payload bytes.
        needed: u64,
        /// Available payload bytes.
        available: u64,
    },
}

impl FragmentIoError {
    /// Makes an IO error without forcing call sites to allocate formatting glue.
    fn io(message: impl Into<String>) -> Self {
        Self::Io(message.into())
    }
}

/// Operation sent to the one durable append actor.
enum Request {
    /// Persist records and acknowledge them only after the batch sync succeeds.
    Append {
        /// Records belonging to one caller's durable request.
        records: Vec<FragmentRecord>,
        /// Synchronous receipt for that request.
        reply: mpsc::Sender<Result<(), FragmentIoError>>,
    },
    /// Persist tombstones before forgetting records from the read index.
    Delete {
        /// Keys to remove.
        keys: Vec<FragmentKey>,
        /// Synchronous receipt for that request.
        reply: mpsc::Sender<Result<(), FragmentIoError>>,
    },
}

/// Filesystem-backed EC fragment log with a disposable in-memory read index.
#[derive(Clone)]
pub struct LocalFragmentStore {
    /// Root directory containing append segments only.
    root: Arc<PathBuf>,
    /// Dynamic payload ceiling shared with disk-budget accounting.
    ceiling: Arc<AtomicU64>,
    /// Bytes represented by live records in the index.
    used: Arc<AtomicU64>,
    /// Rebuilt read index; it is never an acknowledgement dependency.
    index: Arc<RwLock<HashMap<FragmentKey, LoadedFragment>>>,
    /// Serializes sending only; the actor serializes durable state itself.
    sender: mpsc::SyncSender<Request>,
}

impl std::fmt::Debug for LocalFragmentStore {
    /// Prints store state without exposing actor internals.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LocalFragmentStore")
            .field("root", &self.root)
            .field("budget_bytes", &self.budget_bytes())
            .field("used_bytes", &self.used_bytes())
            .finish_non_exhaustive()
    }
}

impl LocalFragmentStore {
    /// Opens an unbounded durable fragment log below `cache_dir`.
    pub fn new(cache_dir: impl AsRef<Path>) -> Self {
        Self::with_dynamic_ceiling(cache_dir, Arc::new(AtomicU64::new(u64::MAX)))
    }

    /// Opens a durable fragment log with a fixed payload ceiling.
    pub fn with_budget(cache_dir: impl AsRef<Path>, budget: u64) -> Self {
        Self::with_dynamic_ceiling(cache_dir, Arc::new(AtomicU64::new(budget)))
    }

    /// Opens a durable fragment log using the caller-owned dynamic ceiling.
    pub fn with_dynamic_ceiling(cache_dir: impl AsRef<Path>, ceiling: Arc<AtomicU64>) -> Self {
        let root = cache_dir.as_ref().join("writeback-fragments");
        let segments = root.join("segments");
        fs::create_dir_all(&segments).unwrap_or_else(|error| {
            panic!("create fragment segments {}: {error}", segments.display())
        });
        let recovered = scan_segments(&segments).unwrap_or_else(|error| {
            panic!("scan fragment segments {}: {error}", segments.display())
        });
        let used = Arc::new(AtomicU64::new(index_bytes(&recovered.index)));
        let index = Arc::new(RwLock::new(recovered.index.clone()));
        let (sender, receiver) = mpsc::sync_channel(1024);
        let actor_index = Arc::clone(&index);
        let actor_used = Arc::clone(&used);
        let store_ceiling = Arc::clone(&ceiling);
        thread::Builder::new()
            .name("verglas-fragment-append".to_owned())
            .spawn(move || {
                AppendActor::new(segments, recovered).run(
                    receiver,
                    actor_index,
                    actor_used,
                    ceiling,
                )
            })
            .unwrap_or_else(|error| panic!("spawn fragment append actor: {error}"));
        Self {
            root: Arc::new(root),
            ceiling: store_ceiling,
            used,
            index,
            sender,
        }
    }

    /// Returns the durable-log root.
    pub fn root(&self) -> &Path {
        self.root.as_path()
    }

    /// Returns the current payload ceiling.
    pub fn budget_bytes(&self) -> u64 {
        self.ceiling.load(Ordering::Acquire)
    }

    /// Returns bytes named by live log records.
    pub fn used_bytes(&self) -> u64 {
        self.used.load(Ordering::Acquire)
    }

    /// Returns whether one additional payload could be admitted.
    pub fn has_headroom(&self, bytes: u64) -> bool {
        self.used_bytes().saturating_add(bytes) <= self.budget_bytes()
    }

    /// Appends one record through the same group-commit actor used by peer RPC.
    pub fn store_fragment(&self, record: &FragmentRecord) -> Result<(), FragmentIoError> {
        self.append_batch(std::slice::from_ref(record))
    }

    /// Queues compatible records and returns only after their common `fdatasync`.
    pub fn append_batch(&self, records: &[FragmentRecord]) -> Result<(), FragmentIoError> {
        if records.is_empty() {
            return Ok(());
        }
        let (reply, receipt) = mpsc::channel();
        self.sender
            .send(Request::Append {
                records: records.to_vec(),
                reply,
            })
            .map_err(|_| FragmentIoError::io("fragment append actor stopped"))?;
        receipt
            .recv()
            .map_err(|_| FragmentIoError::io("fragment append actor dropped receipt"))?
    }

    /// Opens a bounded streaming accumulator that commits through the actor.
    pub fn open_fragment(&self, key: &FragmentKey) -> Result<FragmentWriter, FragmentIoError> {
        Ok(FragmentWriter {
            store: self.clone(),
            key: key.clone(),
            bytes: Vec::new(),
            committed: false,
        })
    }

    /// Loads a live record from the disposable index rebuilt by segment scan.
    pub fn load_fragment(
        &self,
        key: &FragmentKey,
    ) -> Result<Option<LoadedFragment>, FragmentIoError> {
        self.index
            .read()
            .map_err(|_| FragmentIoError::io("fragment index lock poisoned"))
            .map(|index| index.get(key).cloned())
    }

    /// Reports the checksum health of one live record.
    pub fn verify_fragment(&self, key: &FragmentKey) -> Result<FragmentHealth, FragmentIoError> {
        Ok(match self.load_fragment(key)? {
            Some(fragment) if fragment.is_healthy() => FragmentHealth::Healthy,
            Some(_) => FragmentHealth::Corrupt,
            None => FragmentHealth::Missing,
        })
    }

    /// Lists live keys from the rebuilt read index.
    pub fn list_fragment_keys(&self) -> Vec<FragmentKey> {
        self.index
            .read()
            .map(|index| index.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// Appends a durable tombstone before removing one key from the index.
    pub fn delete_fragment(&self, key: &FragmentKey) -> Result<(), FragmentIoError> {
        self.delete_keys(vec![key.clone()])
    }

    /// Appends durable tombstones for every live fragment of one object.
    pub fn delete_object(&self, object_id: &str) -> Result<(), FragmentIoError> {
        let keys = self
            .index
            .read()
            .map_err(|_| FragmentIoError::io("fragment index lock poisoned"))?
            .keys()
            .filter(|key| key.object_id == object_id)
            .cloned()
            .collect();
        self.delete_keys(keys)
    }

    /// Sends a durable tombstone batch to the append actor.
    fn delete_keys(&self, keys: Vec<FragmentKey>) -> Result<(), FragmentIoError> {
        if keys.is_empty() {
            return Ok(());
        }
        let (reply, receipt) = mpsc::channel();
        self.sender
            .send(Request::Delete { keys, reply })
            .map_err(|_| FragmentIoError::io("fragment append actor stopped"))?;
        receipt
            .recv()
            .map_err(|_| FragmentIoError::io("fragment append actor dropped receipt"))?
    }
}

/// Streamed fragment builder; no temporary file or filesystem path exists.
pub struct FragmentWriter {
    /// Actor-backed destination.
    store: LocalFragmentStore,
    /// Destination fragment identity.
    key: FragmentKey,
    /// Bounded caller-provided shards until commit.
    bytes: Vec<u8>,
    /// Prevents reuse after the one durable commit.
    committed: bool,
}

impl FragmentWriter {
    /// Adds one shard while respecting the current payload ceiling.
    pub fn append(&mut self, shard: &[u8]) -> Result<(), FragmentIoError> {
        let next = u64::try_from(self.bytes.len().saturating_add(shard.len()))
            .map_err(|_| FragmentIoError::io("fragment stream is too large"))?;
        if !self.store.has_headroom(next) {
            return Err(FragmentIoError::Full {
                needed: next,
                available: self
                    .store
                    .budget_bytes()
                    .saturating_sub(self.store.used_bytes()),
            });
        }
        self.bytes.extend_from_slice(shard);
        Ok(())
    }

    /// Submits the concatenated shards to the append actor's group commit.
    pub fn commit(mut self) -> Result<(), FragmentIoError> {
        if self.committed {
            return Err(FragmentIoError::io("fragment writer already committed"));
        }
        self.store.append_batch(&[FragmentRecord::new(
            self.key.clone(),
            Bytes::from(std::mem::take(&mut self.bytes)),
        )])?;
        self.committed = true;
        Ok(())
    }
}

/// Durable segment state reconstructed before the actor starts.
struct Recovered {
    /// Live records after applying only committed groups.
    index: HashMap<FragmentKey, LoadedFragment>,
    /// Last segment number, if any.
    segment: Option<u64>,
    /// Last durable group boundary in that segment.
    position: u64,
}

/// The sole owner of segment files and group commit ordering.
struct AppendActor {
    /// Segment directory.
    dir: PathBuf,
    /// Current segment number.
    number: u64,
    /// Current durable append offset.
    position: u64,
    /// Open preallocated segment.
    file: File,
}

impl AppendActor {
    /// Opens the last recovered segment or allocates a first segment.
    fn new(dir: PathBuf, recovered: Recovered) -> Self {
        let number = recovered.segment.unwrap_or(0);
        let (file, position) = open_segment(&dir, number, recovered.position)
            .unwrap_or_else(|error| panic!("open fragment segment: {error}"));
        Self {
            dir,
            number,
            position,
            file,
        }
    }

    /// Runs until every sender is dropped, batching all currently queued calls.
    fn run(
        mut self,
        receiver: mpsc::Receiver<Request>,
        index: Arc<RwLock<HashMap<FragmentKey, LoadedFragment>>>,
        used: Arc<AtomicU64>,
        ceiling: Arc<AtomicU64>,
    ) {
        while let Ok(first) = receiver.recv() {
            let mut requests = vec![first];
            while let Ok(request) = receiver.try_recv() {
                requests.push(request);
            }
            self.commit(requests, &index, &used, &ceiling);
        }
    }

    /// Validates, writes, and syncs one compatible queue drain before replying.
    fn commit(
        &mut self,
        requests: Vec<Request>,
        index: &RwLock<HashMap<FragmentKey, LoadedFragment>>,
        used: &AtomicU64,
        ceiling: &AtomicU64,
    ) {
        let mut state = match index.write() {
            Ok(state) => state,
            Err(_) => {
                for request in requests {
                    reply_error(request, FragmentIoError::io("fragment index lock poisoned"));
                }
                return;
            }
        };
        let mut staged = state.clone();
        let mut staged_used = index_bytes(&staged);
        let mut accepted = Vec::new();
        let mut bytes = Vec::new();
        for request in requests {
            match stage_request(
                &request,
                &mut staged,
                &mut staged_used,
                ceiling.load(Ordering::Acquire),
                &mut bytes,
            ) {
                Ok(()) => accepted.push(request),
                Err(error) => reply_error(request, error),
            }
        }
        if accepted.is_empty() {
            return;
        }
        let record_count = accepted
            .iter()
            .map(|request| match request {
                Request::Append { records, .. } => records.len(),
                Request::Delete { keys, .. } => keys.len(),
            })
            .sum();
        let marker = commit_marker(&bytes, record_count);
        bytes.extend_from_slice(&marker);
        let result = self.write_group(&bytes);
        if result.is_ok() {
            *state = staged;
            used.store(staged_used, Ordering::Release);
        }
        let failure = result.err().map(|error| error.to_string());
        for request in accepted {
            let receipt = match &failure {
                Some(error) => Err(FragmentIoError::io(error.clone())),
                None => Ok(()),
            };
            reply_result(request, receipt);
        }
    }

    /// Appends a full group then makes that exact group durable with one sync.
    fn write_group(&mut self, bytes: &[u8]) -> Result<(), FragmentIoError> {
        if self.position.saturating_add(bytes.len() as u64) > SEGMENT_BYTES && self.position > 0 {
            self.number = self.number.saturating_add(1);
            let (file, position) = open_segment(&self.dir, self.number, 0)?;
            self.file = file;
            self.position = position;
        }
        self.file
            .seek(SeekFrom::Start(self.position))
            .map_err(|error| FragmentIoError::io(format!("seek fragment segment: {error}")))?;
        self.file
            .write_all(bytes)
            .map_err(|error| FragmentIoError::io(format!("append fragment segment: {error}")))?;
        self.file
            .sync_data()
            .map_err(|error| FragmentIoError::io(format!("fdatasync fragment segment: {error}")))?;
        self.position = self.position.saturating_add(bytes.len() as u64);
        Ok(())
    }
}

/// Applies one request to a prospective index and encodes its durable records.
fn stage_request(
    request: &Request,
    index: &mut HashMap<FragmentKey, LoadedFragment>,
    used: &mut u64,
    ceiling: u64,
    encoded: &mut Vec<u8>,
) -> Result<(), FragmentIoError> {
    match request {
        Request::Append { records, .. } => {
            let mut trial = index.clone();
            let mut trial_used = *used;
            let mut trial_encoded = Vec::new();
            for record in records {
                let old = trial.insert(
                    record.key.clone(),
                    LoadedFragment {
                        bytes: record.bytes.clone(),
                        checksum: record.checksum,
                    },
                );
                trial_used = trial_used
                    .saturating_sub(old.map_or(0, |fragment| fragment.bytes.len() as u64));
                trial_used = trial_used.saturating_add(record.bytes.len() as u64);
                if trial_used > ceiling {
                    return Err(FragmentIoError::Full {
                        needed: record.bytes.len() as u64,
                        available: ceiling.saturating_sub(*used),
                    });
                }
                trial_encoded.extend_from_slice(&encode_record(
                    1,
                    &record.key,
                    record.checksum,
                    &record.bytes,
                )?);
            }
            *index = trial;
            *used = trial_used;
            encoded.extend_from_slice(&trial_encoded);
            Ok(())
        }
        Request::Delete { keys, .. } => {
            for key in keys {
                if let Some(old) = index.remove(key) {
                    *used = used.saturating_sub(old.bytes.len() as u64);
                }
                encoded.extend_from_slice(&encode_record(2, key, 0, &[])?);
            }
            Ok(())
        }
    }
}

/// Returns the durable group marker that lets recovery reject a torn tail.
fn commit_marker(records: &[u8], request_count: usize) -> Vec<u8> {
    let mut marker = Vec::with_capacity(12);
    marker.extend_from_slice(&COMMIT_MAGIC);
    marker.extend_from_slice(&(request_count as u32).to_le_bytes());
    marker.extend_from_slice(&fragment_checksum(records).to_le_bytes());
    marker
}

/// Encodes one complete checksummed log record.
fn encode_record(
    kind: u8,
    key: &FragmentKey,
    checksum: u32,
    payload: &[u8],
) -> Result<Vec<u8>, FragmentIoError> {
    let name = key.object_id.as_bytes();
    let name_len = u32::try_from(name.len())
        .map_err(|_| FragmentIoError::io("fragment object id is too long"))?;
    let index =
        u64::try_from(key.index).map_err(|_| FragmentIoError::io("fragment index is too large"))?;
    let payload_len = u64::try_from(payload.len())
        .map_err(|_| FragmentIoError::io("fragment payload is too large"))?;
    let mut record = Vec::with_capacity(4 + 1 + 4 + 8 + 8 + 4 + name.len() + payload.len() + 4);
    record.extend_from_slice(&RECORD_MAGIC);
    record.push(kind);
    record.extend_from_slice(&name_len.to_le_bytes());
    record.extend_from_slice(&index.to_le_bytes());
    record.extend_from_slice(&payload_len.to_le_bytes());
    record.extend_from_slice(&checksum.to_le_bytes());
    record.extend_from_slice(name);
    record.extend_from_slice(payload);
    record.extend_from_slice(&fragment_checksum(&record).to_le_bytes());
    Ok(record)
}

/// Opens a preallocated segment at the requested committed offset.
fn open_segment(dir: &Path, number: u64, position: u64) -> Result<(File, u64), FragmentIoError> {
    let path = dir.join(format!("segment-{number:020}.log"));
    let fresh = !path.exists();
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)
        .map_err(|error| FragmentIoError::io(format!("open {}: {error}", path.display())))?;
    if fresh {
        file.set_len(SEGMENT_BYTES).map_err(|error| {
            FragmentIoError::io(format!("preallocate {}: {error}", path.display()))
        })?;
        file.sync_all().map_err(|error| {
            FragmentIoError::io(format!("sync new segment {}: {error}", path.display()))
        })?;
    }
    Ok((file, position))
}

/// Scans all segments in order and applies only records followed by valid commits.
fn scan_segments(dir: &Path) -> Result<Recovered, FragmentIoError> {
    let mut paths: Vec<_> = fs::read_dir(dir)
        .map_err(|error| FragmentIoError::io(format!("list {}: {error}", dir.display())))?
        .filter_map(Result::ok)
        .filter_map(|entry| segment_number(&entry.path()).map(|number| (number, entry.path())))
        .collect();
    paths.sort_by_key(|(number, _)| *number);
    let mut index = HashMap::new();
    let mut segment = None;
    let mut position = 0;
    for (number, path) in paths {
        let (operations, durable) = scan_segment(&path)?;
        for operation in operations {
            apply_operation(&mut index, operation);
        }
        segment = Some(number);
        position = durable;
    }
    Ok(Recovered {
        index,
        segment,
        position,
    })
}

/// Extracts the numeric segment ordering key from one path.
fn segment_number(path: &Path) -> Option<u64> {
    path.file_name()?
        .to_str()?
        .strip_prefix("segment-")?
        .strip_suffix(".log")?
        .parse()
        .ok()
}

/// One recovered operation before it is applied to the volatile index.
enum Operation {
    /// Insert or replace one fragment.
    Upsert(FragmentKey, LoadedFragment),
    /// Remove one fragment.
    Delete(FragmentKey),
}

/// Scans one preallocated segment up through its final valid group marker.
fn scan_segment(path: &Path) -> Result<(Vec<Operation>, u64), FragmentIoError> {
    let mut bytes = Vec::new();
    File::open(path)
        .and_then(|mut file| file.read_to_end(&mut bytes))
        .map_err(|error| FragmentIoError::io(format!("read {}: {error}", path.display())))?;
    let mut offset = 0usize;
    let mut durable = 0u64;
    let mut committed = Vec::new();
    let mut pending = Vec::new();
    let mut group_bytes = Vec::new();
    while offset + 4 <= bytes.len() {
        let magic = &bytes[offset..offset + 4];
        if magic == COMMIT_MAGIC {
            if offset + 12 > bytes.len() {
                break;
            }
            let count = u32::from_le_bytes(
                bytes[offset + 4..offset + 8]
                    .try_into()
                    .map_err(|_| FragmentIoError::io("invalid commit count"))?,
            ) as usize;
            let checksum = u32::from_le_bytes(
                bytes[offset + 8..offset + 12]
                    .try_into()
                    .map_err(|_| FragmentIoError::io("invalid commit checksum"))?,
            );
            if count != pending.len() || checksum != fragment_checksum(&group_bytes) {
                break;
            }
            committed.append(&mut pending);
            group_bytes.clear();
            offset += 12;
            durable = offset as u64;
            continue;
        }
        if magic != RECORD_MAGIC {
            break;
        }
        let Some((operation, end)) = decode_record(&bytes, offset)? else {
            break;
        };
        group_bytes.extend_from_slice(&bytes[offset..end]);
        pending.push(operation);
        offset = end;
    }
    Ok((committed, durable))
}

/// Decodes one complete record, returning `None` for a torn tail.
fn decode_record(
    bytes: &[u8],
    offset: usize,
) -> Result<Option<(Operation, usize)>, FragmentIoError> {
    const HEADER: usize = 4 + 1 + 4 + 8 + 8 + 4;
    if offset + HEADER + 4 > bytes.len() {
        return Ok(None);
    }
    let kind = bytes[offset + 4];
    let name_len = u32::from_le_bytes(
        bytes[offset + 5..offset + 9]
            .try_into()
            .map_err(|_| FragmentIoError::io("invalid name length"))?,
    ) as usize;
    let index = u64::from_le_bytes(
        bytes[offset + 9..offset + 17]
            .try_into()
            .map_err(|_| FragmentIoError::io("invalid index"))?,
    );
    let payload_len = u64::from_le_bytes(
        bytes[offset + 17..offset + 25]
            .try_into()
            .map_err(|_| FragmentIoError::io("invalid payload length"))?,
    ) as usize;
    let checksum = u32::from_le_bytes(
        bytes[offset + 25..offset + 29]
            .try_into()
            .map_err(|_| FragmentIoError::io("invalid payload checksum"))?,
    );
    let end = offset
        .checked_add(HEADER)
        .and_then(|value| value.checked_add(name_len))
        .and_then(|value| value.checked_add(payload_len))
        .and_then(|value| value.checked_add(4));
    let Some(end) = end.filter(|end| *end <= bytes.len()) else {
        return Ok(None);
    };
    let stored = u32::from_le_bytes(
        bytes[end - 4..end]
            .try_into()
            .map_err(|_| FragmentIoError::io("invalid record checksum"))?,
    );
    if stored != fragment_checksum(&bytes[offset..end - 4]) {
        return Ok(None);
    }
    let name_end = offset + HEADER + name_len;
    let key = FragmentKey {
        object_id: String::from_utf8(bytes[offset + HEADER..name_end].to_vec())
            .map_err(|_| FragmentIoError::io("fragment object id is not utf-8"))?,
        index: usize::try_from(index)
            .map_err(|_| FragmentIoError::io("fragment index does not fit usize"))?,
    };
    let operation = match kind {
        1 => Operation::Upsert(
            key,
            LoadedFragment {
                bytes: Bytes::copy_from_slice(&bytes[name_end..end - 4]),
                checksum,
            },
        ),
        2 if payload_len == 0 => Operation::Delete(key),
        _ => return Ok(None),
    };
    Ok(Some((operation, end)))
}

/// Applies a committed operation to the recovered or live read index.
fn apply_operation(index: &mut HashMap<FragmentKey, LoadedFragment>, operation: Operation) {
    match operation {
        Operation::Upsert(key, value) => {
            index.insert(key, value);
        }
        Operation::Delete(key) => {
            index.remove(&key);
        }
    }
}

/// Totals payload bytes represented by an index.
fn index_bytes(index: &HashMap<FragmentKey, LoadedFragment>) -> u64 {
    index
        .values()
        .map(|fragment| fragment.bytes.len() as u64)
        .sum()
}

/// Delivers the same result to either kind of request.
fn reply_result(request: Request, result: Result<(), FragmentIoError>) {
    match request {
        Request::Append { reply, .. } | Request::Delete { reply, .. } => {
            let _ = reply.send(result);
        }
    }
}

/// Delivers a cloned actor failure without retaining partial acknowledgement.
fn reply_error(request: Request, error: FragmentIoError) {
    reply_result(request, Err(error));
}

#[cfg(test)]
mod tests {
    //! Crash, batching, and recovery contracts for the durable append actor.

    use super::*;
    use std::sync::atomic::AtomicU64;

    /// Creates a unique test directory.
    fn scratch(name: &str) -> Scratch {
        let path = std::env::temp_dir().join(format!("verglas-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).expect("create scratch directory");
        Scratch(path)
    }

    /// Temporary test directory removed when the test ends.
    struct Scratch(PathBuf);

    impl Scratch {
        /// Returns the directory path.
        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for Scratch {
        /// Removes only this test's unique scratch directory.
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    /// Builds a compact test record.
    fn record(object: &str, index: usize, bytes: &'static [u8]) -> FragmentRecord {
        FragmentRecord::new(
            FragmentKey {
                object_id: object.to_owned(),
                index,
            },
            Bytes::from_static(bytes),
        )
    }

    /// A group commit exposes every record after one durable receipt.
    #[test]
    fn batch_is_recoverable_from_segments_without_fragment_files() {
        let dir = scratch("fragment-batch");
        let store = LocalFragmentStore::new(dir.path());
        store
            .append_batch(&[record("one", 0, b"alpha"), record("two", 1, b"beta")])
            .expect("append");
        drop(store);
        let reopened = LocalFragmentStore::new(dir.path());
        assert_eq!(
            reopened
                .load_fragment(&FragmentKey {
                    object_id: "one".to_owned(),
                    index: 0
                })
                .expect("load")
                .expect("record")
                .bytes,
            Bytes::from_static(b"alpha")
        );
        assert!(
            !dir.path().join("writeback-fragments/objects").exists(),
            "segments are the only durable fragment representation"
        );
    }

    /// A torn uncommitted suffix never appears after the durable-prefix scan.
    #[test]
    fn recovery_discards_torn_group_after_last_commit_marker() {
        let dir = scratch("fragment-torn");
        let store = LocalFragmentStore::new(dir.path());
        store
            .store_fragment(&record("safe", 0, b"durable"))
            .expect("append");
        let path = store
            .root()
            .join("segments/segment-00000000000000000000.log");
        drop(store);
        let mut file = OpenOptions::new().write(true).open(&path).expect("open");
        let end = scan_segment(&path).expect("scan").1;
        file.seek(SeekFrom::Start(end)).expect("seek");
        file.write_all(
            &encode_record(
                1,
                &FragmentKey {
                    object_id: "torn".to_owned(),
                    index: 1,
                },
                fragment_checksum(b"lost"),
                b"lost",
            )
            .expect("encode"),
        )
        .expect("write torn");
        let reopened = LocalFragmentStore::new(dir.path());
        assert!(
            reopened
                .load_fragment(&FragmentKey {
                    object_id: "safe".to_owned(),
                    index: 0
                })
                .expect("load")
                .is_some()
        );
        assert!(
            reopened
                .load_fragment(&FragmentKey {
                    object_id: "torn".to_owned(),
                    index: 1
                })
                .expect("load")
                .is_none()
        );
    }

    /// Tombstones survive restart so cleaned fragments never reappear.
    #[test]
    fn durable_tombstone_prevents_resurrection() {
        let dir = scratch("fragment-delete");
        let store = LocalFragmentStore::new(dir.path());
        let key = FragmentKey {
            object_id: "gone".to_owned(),
            index: 0,
        };
        store
            .store_fragment(&FragmentRecord::new(
                key.clone(),
                Bytes::from_static(b"bytes"),
            ))
            .expect("append");
        store.delete_fragment(&key).expect("delete");
        drop(store);
        assert!(
            LocalFragmentStore::new(dir.path())
                .load_fragment(&key)
                .expect("load")
                .is_none()
        );
    }

    /// A current dynamic ceiling refuses records before any durable append.
    #[test]
    fn ceiling_is_a_hard_live_record_limit() {
        let dir = scratch("fragment-budget");
        let ceiling = Arc::new(AtomicU64::new(3));
        let store = LocalFragmentStore::with_dynamic_ceiling(dir.path(), Arc::clone(&ceiling));
        assert!(store.store_fragment(&record("no", 0, b"four")).is_err());
        assert_eq!(store.used_bytes(), 0);
    }
}
