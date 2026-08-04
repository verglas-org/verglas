//! Local fragment blob store for the write-back tier (#180).
//!
//! A node persists erasure-coded fragments it is assigned as fsynced files
//! under its cache directory. This module owns only the on-disk blob store and
//! the fragment identity types; the journal, quorum, placement, and origin
//! propagation live in `verglas-write`, and the fragment transport lives in
//! [`crate::peer`]. Returning `Ok` from [`LocalFragmentStore::store_fragment`]
//! means the bytes are durable on this node's NVMe — that is the unit the
//! write-back ack counts.
//!
//! ## Per-fragment integrity (#220)
//!
//! Every fragment file is written as `payload || CRC32C(payload)`: a fixed
//! 4-byte little-endian trailer. Reed-Solomon is an *erasure* code, so a
//! bit-flip in a fragment used for reconstruction produces silently wrong bytes.
//! The trailer lets a load detect that flip and treat the fragment as an erasure
//! instead of feeding corrupt data to the decoder. Loads return the payload plus
//! the stored checksum so the caller can verify end-to-end; the scrubber uses
//! [`LocalFragmentStore::verify_fragment`] to find corrupt fragments on a
//! schedule and re-encode them before enough accumulate to cross `m`.

use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;

/// Bytes of the CRC32C integrity trailer appended to every stored fragment file
/// (#220). Little-endian `u32` over the fragment payload.
const CHECKSUM_TRAILER_LEN: usize = 4;

/// The per-fragment integrity checksum (CRC32C, Castagnoli) over a fragment's
/// payload. Chosen for bit-flip detection on the storage path: hardware
/// accelerated, the standard storage checksum, cheap on the write path. Not
/// cryptographic — it defends against bit-rot, not a malicious rewrite (#220).
pub fn fragment_checksum(bytes: &[u8]) -> u32 {
    crc32c::crc32c(bytes)
}

/// A fragment loaded from disk: its payload plus the checksum trailer stored
/// with it (#220). The caller verifies `checksum` against `bytes` before using
/// the fragment, so a corrupt fragment is caught end-to-end (on the storing node
/// and again at reassembly).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadedFragment {
    /// The fragment payload (without the trailer).
    pub bytes: Bytes,
    /// The CRC32C read from the fragment's on-disk trailer.
    pub checksum: u32,
}

impl LoadedFragment {
    /// True when the payload still matches the stored checksum.
    pub fn is_healthy(&self) -> bool {
        fragment_checksum(&self.bytes) == self.checksum
    }
}

/// The health of a stored fragment as the scrubber sees it (#220).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FragmentHealth {
    /// The fragment is present and its payload matches its checksum.
    Healthy,
    /// The fragment is present but its payload no longer matches its checksum
    /// (bit-rot) — the scrubber re-encodes it from the survivors.
    Corrupt,
    /// The fragment file is absent on this node.
    Missing,
}

/// Identifies one fragment: the object it belongs to and its index within the
/// `k+m` fragment set.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FragmentKey {
    /// Stable per-object id assigned by the write coordinator.
    pub object_id: String,
    /// Zero-based fragment index (`0..k` data, `k..k+m` parity).
    pub index: usize,
}

/// A fragment's bytes plus its key and integrity checksum, the unit stored and
/// moved over peer RPC. `checksum` is the CRC32C over `bytes` (#220), carried so
/// a receiving node can persist the trailer and a reader can verify end-to-end.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FragmentRecord {
    /// The fragment's identity.
    pub key: FragmentKey,
    /// The fragment payload (already erasure-padded by the codec).
    pub bytes: Bytes,
    /// CRC32C over `bytes`.
    pub checksum: u32,
}

impl FragmentRecord {
    /// Builds a record for `key` over `bytes`, computing its CRC32C. The only
    /// constructor, so `checksum` always describes `bytes` at creation time.
    pub fn new(key: FragmentKey, bytes: Bytes) -> Self {
        let checksum = fragment_checksum(&bytes);
        Self {
            key,
            bytes,
            checksum,
        }
    }
}

/// A fragment store or transport failure. A local IO error here fails the
/// containing write-back placement; the write then degrades to write-through.
#[derive(Debug, thiserror::Error)]
pub enum FragmentIoError {
    /// A filesystem or encoding error touching a fragment.
    #[error("fragment store: {0}")]
    Io(String),
    /// The fragment sub-budget has no headroom for this fragment. This is a
    /// hard-ceiling refusal, not an IO failure: the coordinator excludes a full
    /// node from placement, and if fewer than `w` nodes have headroom the write
    /// degrades to write-through. The ceiling is never exceeded.
    #[error("fragment store full: {needed} bytes needed, {available} available under the budget")]
    Full {
        /// Bytes the refused fragment would add.
        needed: u64,
        /// Bytes still free under the budget.
        available: u64,
    },
}

impl FragmentIoError {
    /// Builds an IO-flavored error from any displayable message. Keeps call
    /// sites terse now that the type is an enum.
    fn io(message: impl Into<String>) -> Self {
        FragmentIoError::Io(message.into())
    }
}

/// Filesystem-backed fragment store rooted under a node's cache directory.
///
/// The store enforces a byte budget as a hard ceiling: [`store_fragment`] first
/// reserves the fragment's bytes against the budget and refuses with
/// [`FragmentIoError::Full`] if that would exceed it, so total fragment bytes
/// never pass the configured sub-budget. Deletes release bytes. The budget is a
/// reservation gate on *new* writes; it never evicts a fragment already on disk,
/// which is what keeps an un-propagated fragment (the only durable copy of acked
/// data) safe until propagation deletes it explicitly.
///
/// The ceiling is **dynamic** (#223): a shared atomic the daemon's disk poll
/// updates each tick to what the store holds plus the share of the one NVMe
/// budget (`cache.capacity_bytes`) the block cache is not physically using —
/// first come, first served, no carve and no fraction. Fragments are transient,
/// so a write burst takes whatever budget reads are not using instead of
/// degrading to write-through while the disk sits idle; when the budget or the
/// disk is spent, the poll lowers the ceiling toward the bytes already stored,
/// so new writes degrade to write-through but no stored fragment is dropped.
/// Reading the ceiling is one relaxed atomic load, so it never blocks placement.
///
/// [`store_fragment`]: LocalFragmentStore::store_fragment
#[derive(Debug, Clone)]
pub struct LocalFragmentStore {
    /// Root holding all fragment blobs for this node.
    root: Arc<PathBuf>,
    /// Live ceiling on total fragment bytes; `u64::MAX` means unbudgeted. Shared
    /// so the daemon's disk poll can raise or lower it while placements run.
    ceiling: Arc<AtomicU64>,
    /// Live fragment bytes on disk, charged on store and released on delete.
    used: Arc<AtomicU64>,
}

impl LocalFragmentStore {
    /// Creates an unbudgeted store rooted at `<cache_dir>/writeback-fragments`.
    /// Used by tests and the fragment RPC surface, which do not gate on budget.
    pub fn new(cache_dir: impl AsRef<Path>) -> Self {
        Self::with_dynamic_ceiling(cache_dir, Arc::new(AtomicU64::new(u64::MAX)))
    }

    /// Creates a store with a fixed byte ceiling, rebuilding the used-byte count
    /// from any fragments a previous run left on disk so the bound holds across a
    /// restart. Convenience for tests and callers with a static bound.
    pub fn with_budget(cache_dir: impl AsRef<Path>, budget: u64) -> Self {
        Self::with_dynamic_ceiling(cache_dir, Arc::new(AtomicU64::new(budget)))
    }

    /// Creates a store whose ceiling is a shared atomic the daemon's disk poll
    /// updates dynamically (#223), rebuilding the used-byte count from any
    /// fragments a previous run left on disk so the bound holds across a restart.
    pub fn with_dynamic_ceiling(cache_dir: impl AsRef<Path>, ceiling: Arc<AtomicU64>) -> Self {
        let root = cache_dir.as_ref().join("writeback-fragments");
        let used = scan_used_bytes(&root);
        Self {
            root: Arc::new(root),
            ceiling,
            used: Arc::new(AtomicU64::new(used)),
        }
    }

    /// The root directory this store writes under.
    pub fn root(&self) -> &Path {
        self.root.as_path()
    }

    /// The current byte ceiling (`u64::MAX` when unbudgeted) — the live dynamic
    /// bound.
    pub fn budget_bytes(&self) -> u64 {
        self.ceiling.load(Ordering::Acquire)
    }

    /// Live fragment bytes currently on disk.
    pub fn used_bytes(&self) -> u64 {
        self.used.load(Ordering::Acquire)
    }

    /// True when a write of `bytes` would still fit under the current ceiling.
    /// Placement consults this to exclude a full node before attempting a write.
    pub fn has_headroom(&self, bytes: u64) -> bool {
        self.used_bytes().saturating_add(bytes) <= self.budget_bytes()
    }

    /// Persists and fsyncs one fragment. `Ok` means the bytes reached this
    /// node's NVMe durably (file and directory both fsynced). Reserves the
    /// fragment's bytes against the budget first and refuses with
    /// [`FragmentIoError::Full`] if that would exceed the ceiling — the reserve
    /// happens before the write, so the on-disk total never passes the budget.
    pub fn store_fragment(&self, record: &FragmentRecord) -> Result<(), FragmentIoError> {
        let len = record.bytes.len() as u64;
        // If this key already has bytes on disk (an idempotent re-place), free
        // its old charge first so a re-write of the same fragment is neutral.
        let existing = self.fragment_len(&record.key);
        if existing > 0 {
            self.used.fetch_sub(existing, Ordering::AcqRel);
        }
        self.reserve(len)?;
        let path = self.fragment_path(&record.key);
        // Persist `payload || CRC32C(payload)` so a later load can detect a
        // bit-flip and treat the fragment as an erasure (#220). The budget is
        // charged for the payload only; the 4-byte trailer is fixed overhead.
        match write_fsynced(&path, &record.bytes, record.checksum) {
            Ok(()) => Ok(()),
            Err(error) => {
                // The write failed: undo the reservation so a transient IO error
                // does not permanently shrink the budget.
                self.used.fetch_sub(len, Ordering::AcqRel);
                Err(error)
            }
        }
    }

    /// Opens a streaming writer for one fragment: shards are appended to a temp
    /// file as they arrive, and the fragment becomes live (rename + directory
    /// fsync) only on [`FragmentWriter::commit`]. This is how the coordinator
    /// streams a large fragment stripe by stripe without buffering it whole, so
    /// DRAM stays at one stripe regardless of object size. Budget is reserved as
    /// each shard is written and released if the writer is dropped without a
    /// commit, so the hard ceiling holds even mid-stream.
    pub fn open_fragment(&self, key: &FragmentKey) -> Result<FragmentWriter, FragmentIoError> {
        let path = self.fragment_path(key);
        // A re-place of an existing fragment frees its old charge first so a
        // rewrite is budget-neutral.
        let existing = self.fragment_len(key);
        if existing > 0 {
            self.used.fetch_sub(existing, Ordering::AcqRel);
        }
        let parent = path
            .parent()
            .ok_or_else(|| FragmentIoError::io(format!("{} has no parent", path.display())))?
            .to_path_buf();
        fs::create_dir_all(&parent).map_err(|error| {
            FragmentIoError::io(format!("create fragment dir {}: {error}", parent.display()))
        })?;
        let tmp = path.with_extension("tmp");
        let file = File::create(&tmp)
            .map_err(|error| FragmentIoError::io(format!("create {}: {error}", tmp.display())))?;
        Ok(FragmentWriter {
            store: self.clone(),
            file: Some(file),
            tmp,
            path,
            parent,
            written: 0,
            hasher: crc32c::Crc32cHasher::default(),
            committed: false,
        })
    }

    /// Loads one fragment by key, returning `None` when this node lacks it. The
    /// returned [`LoadedFragment`] carries the payload and the checksum read from
    /// the on-disk trailer, so the caller verifies before use (#220). A file too
    /// short to hold a trailer is a corrupt/partial write and is an IO error.
    pub fn load_fragment(
        &self,
        key: &FragmentKey,
    ) -> Result<Option<LoadedFragment>, FragmentIoError> {
        let path = self.fragment_path(key);
        match fs::read(&path) {
            Ok(raw) => Ok(Some(split_trailer(&path, raw)?)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(FragmentIoError::io(format!(
                "read fragment {}: {error}",
                path.display()
            ))),
        }
    }

    /// Verifies one stored fragment's integrity (#220): [`FragmentHealth::Healthy`]
    /// when its payload matches its trailer, [`FragmentHealth::Corrupt`] when it
    /// does not, [`FragmentHealth::Missing`] when the fragment is absent. The
    /// scrubber calls this per fragment on a schedule.
    pub fn verify_fragment(&self, key: &FragmentKey) -> Result<FragmentHealth, FragmentIoError> {
        match self.load_fragment(key)? {
            None => Ok(FragmentHealth::Missing),
            Some(loaded) if loaded.is_healthy() => Ok(FragmentHealth::Healthy),
            Some(_) => Ok(FragmentHealth::Corrupt),
        }
    }

    /// Lists the keys of every fragment this node currently stores, so the
    /// scrubber can walk them (#220). Reads the object directory tree; a `.tmp`
    /// (crashed write) is skipped because it is not a live fragment.
    pub fn list_fragment_keys(&self) -> Vec<FragmentKey> {
        list_fragment_keys(&self.root.join("objects"))
    }

    /// Deletes one fragment if present, releasing its bytes back to the budget.
    /// Missing is success (idempotent) — a repair or propagation cleanup may run
    /// more than once.
    pub fn delete_fragment(&self, key: &FragmentKey) -> Result<(), FragmentIoError> {
        let path = self.fragment_path(key);
        let len = self.fragment_len(key);
        match fs::remove_file(&path) {
            Ok(()) => {
                self.used.fetch_sub(len, Ordering::AcqRel);
                Ok(())
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(FragmentIoError::io(format!(
                "delete fragment {}: {error}",
                path.display()
            ))),
        }
    }

    /// Deletes every fragment stored for `object_id`, releasing their bytes back
    /// to the budget (propagation cleanup). Missing is success.
    pub fn delete_object(&self, object_id: &str) -> Result<(), FragmentIoError> {
        let dir = self.object_dir(object_id);
        let freed = dir_bytes(&dir);
        match fs::remove_dir_all(&dir) {
            Ok(()) => {
                self.used.fetch_sub(freed, Ordering::AcqRel);
                Ok(())
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(FragmentIoError::io(format!(
                "delete object dir {}: {error}",
                dir.display()
            ))),
        }
    }

    /// Charges `len` bytes against the current ceiling, refusing with
    /// [`FragmentIoError::Full`] if it would exceed it. The check and the charge
    /// are one compare-and-swap loop, so two concurrent writers can never both
    /// slip past the ceiling. The ceiling is re-read each attempt, so a
    /// concurrent poll that lowered it takes effect at once — but a lowered
    /// ceiling only refuses *new* writes; bytes already charged stay put (an
    /// acked fragment is never dropped by budget pressure).
    fn reserve(&self, len: u64) -> Result<(), FragmentIoError> {
        let mut used = self.used.load(Ordering::Acquire);
        loop {
            let ceiling = self.ceiling.load(Ordering::Acquire);
            let next = used.saturating_add(len);
            if next > ceiling {
                return Err(FragmentIoError::Full {
                    needed: len,
                    available: ceiling.saturating_sub(used),
                });
            }
            match self
                .used
                .compare_exchange_weak(used, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return Ok(()),
                Err(observed) => used = observed,
            }
        }
    }

    /// Releases `len` bytes back to the budget (used by [`FragmentWriter`] to
    /// undo a partial stream that never commits).
    fn release(&self, len: u64) {
        if len > 0 {
            self.used.fetch_sub(len, Ordering::AcqRel);
        }
    }

    /// The payload byte length of a stored fragment (its on-disk size minus the
    /// checksum trailer), or 0 if absent. The budget tracks payload bytes, so
    /// the fixed trailer overhead never counts against the sub-budget (#220).
    fn fragment_len(&self, key: &FragmentKey) -> u64 {
        fs::metadata(self.fragment_path(key))
            .map(|m| payload_len(m.len()))
            .unwrap_or(0)
    }

    /// The directory holding one object's fragments.
    fn object_dir(&self, object_id: &str) -> PathBuf {
        self.root.join("objects").join(hex_component(object_id))
    }

    /// The filesystem path for a fragment key.
    fn fragment_path(&self, key: &FragmentKey) -> PathBuf {
        self.object_dir(&key.object_id)
            .join(format!("fragment-{:05}.bin", key.index))
    }
}

/// A streaming writer for one fragment (append shards, then commit).
///
/// Shards are appended to a temp file with [`FragmentWriter::append`], each
/// append reserving its bytes against the store budget. [`FragmentWriter::commit`]
/// fsyncs the file, renames it into place, and fsyncs the directory — only then
/// is the fragment durable and live. Dropping the writer without committing
/// releases the reserved bytes and removes the temp file, so a failed stream
/// neither leaks budget nor leaves a live fragment.
pub struct FragmentWriter {
    /// The owning store (for budget reserve/release).
    store: LocalFragmentStore,
    /// The open temp file; taken on commit.
    file: Option<File>,
    /// Temp path being written.
    tmp: PathBuf,
    /// Final fragment path.
    path: PathBuf,
    /// The fragment's parent directory (fsynced on commit).
    parent: PathBuf,
    /// Bytes reserved and written so far.
    written: u64,
    /// Running CRC32C over the appended payload; finalized into the trailer on
    /// commit so the fragment carries its integrity check (#220).
    hasher: crc32c::Crc32cHasher,
    /// Set once committed, so `Drop` does not release/clean up.
    committed: bool,
}

impl FragmentWriter {
    /// Appends one shard, reserving its bytes against the budget first, and folds
    /// it into the running checksum. Refuses with [`FragmentIoError::Full`] if the
    /// ceiling would be crossed — the stream is abandoned and the partial
    /// fragment never becomes live.
    pub fn append(&mut self, shard: &[u8]) -> Result<(), FragmentIoError> {
        use std::hash::Hasher;
        let len = shard.len() as u64;
        self.store.reserve(len)?;
        let Some(file) = self.file.as_mut() else {
            self.store.release(len);
            return Err(FragmentIoError::io("fragment writer already committed"));
        };
        match file.write_all(shard) {
            Ok(()) => {
                self.written += len;
                self.hasher.write(shard);
                Ok(())
            }
            Err(error) => {
                self.store.release(len);
                Err(FragmentIoError::io(format!(
                    "append {}: {error}",
                    self.tmp.display()
                )))
            }
        }
    }

    /// Writes the checksum trailer, fsyncs the fragment, renames it into place,
    /// and fsyncs the directory, making the fragment durable and live. The
    /// trailer is the CRC32C of everything appended, so a later load can detect a
    /// bit-flip (#220). Consumes the writer.
    pub fn commit(mut self) -> Result<(), FragmentIoError> {
        use std::hash::Hasher;
        let mut file = self
            .file
            .take()
            .ok_or_else(|| FragmentIoError::io("fragment writer already committed"))?;
        let checksum = self.hasher.finish() as u32;
        file.write_all(&checksum.to_le_bytes()).map_err(|error| {
            FragmentIoError::io(format!("write trailer {}: {error}", self.tmp.display()))
        })?;
        file.sync_all().map_err(|error| {
            FragmentIoError::io(format!("fsync {}: {error}", self.tmp.display()))
        })?;
        drop(file);
        fs::rename(&self.tmp, &self.path).map_err(|error| {
            FragmentIoError::io(format!(
                "rename {} to {}: {error}",
                self.tmp.display(),
                self.path.display()
            ))
        })?;
        sync_dir(&self.parent)?;
        self.committed = true;
        Ok(())
    }
}

impl Drop for FragmentWriter {
    /// Releases the reserved budget and removes the temp file if the stream was
    /// never committed, so an abandoned write leaks neither budget nor a file.
    fn drop(&mut self) {
        if !self.committed {
            self.store.release(self.written);
            let _ = fs::remove_file(&self.tmp);
        }
    }
}

/// Sums the bytes of every fragment file under `root/objects`, ignoring the
/// temp files a crash may have left. Used to rebuild the budget count on open.
fn scan_used_bytes(root: &Path) -> u64 {
    dir_bytes(&root.join("objects"))
}

/// The payload byte count of a fragment file of on-disk size `file_len`: the
/// file minus its fixed checksum trailer. A file too short to hold a trailer
/// counts as zero payload (a crashed partial write).
fn payload_len(file_len: u64) -> u64 {
    file_len.saturating_sub(CHECKSUM_TRAILER_LEN as u64)
}

/// Sums the payload length of every `.bin` fragment file beneath `dir`
/// (recursively over per-object subdirectories), excluding each file's checksum
/// trailer so the budget tracks payload bytes (#220). A `.tmp` file (a crashed
/// write) is not counted — it never became a live fragment.
fn dir_bytes(dir: &Path) -> u64 {
    let mut total = 0u64;
    let mut stack = vec![dir.to_path_buf()];
    while let Some(current) = stack.pop() {
        let Ok(entries) = fs::read_dir(&current) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let Ok(meta) = entry.metadata() else {
                continue;
            };
            if meta.is_dir() {
                stack.push(path);
            } else if path.extension().and_then(|x| x.to_str()) == Some("bin") {
                total += payload_len(meta.len());
            }
        }
    }
    total
}

/// Splits a fragment file's raw bytes into its payload and the checksum trailer
/// (#220). A file too short to hold the fixed trailer is a corrupt or partial
/// write and is an IO error, never silently read as an empty fragment.
fn split_trailer(path: &Path, mut raw: Vec<u8>) -> Result<LoadedFragment, FragmentIoError> {
    if raw.len() < CHECKSUM_TRAILER_LEN {
        return Err(FragmentIoError::io(format!(
            "fragment {} is {} bytes, too short for a checksum trailer",
            path.display(),
            raw.len()
        )));
    }
    let trailer = raw.split_off(raw.len() - CHECKSUM_TRAILER_LEN);
    let checksum = u32::from_le_bytes([trailer[0], trailer[1], trailer[2], trailer[3]]);
    Ok(LoadedFragment {
        bytes: Bytes::from(raw),
        checksum,
    })
}

/// Walks `dir` (the store's `objects` root) and rebuilds the [`FragmentKey`] of
/// every live `.bin` fragment: the object id from the hex-encoded subdirectory
/// name, the index from the `fragment-NNNNN.bin` filename (#220).
fn list_fragment_keys(dir: &Path) -> Vec<FragmentKey> {
    let mut keys = Vec::new();
    let Ok(objects) = fs::read_dir(dir) else {
        return keys;
    };
    for object_entry in objects.flatten() {
        let object_path = object_entry.path();
        if !object_entry
            .file_type()
            .map(|t| t.is_dir())
            .unwrap_or(false)
        {
            continue;
        }
        let Some(object_id) = object_entry
            .file_name()
            .to_str()
            .and_then(hex_component_decode)
        else {
            continue;
        };
        let Ok(frags) = fs::read_dir(&object_path) else {
            continue;
        };
        for frag in frags.flatten() {
            let name = frag.file_name();
            let Some(name) = name.to_str() else { continue };
            if let Some(index) = name
                .strip_prefix("fragment-")
                .and_then(|rest| rest.strip_suffix(".bin"))
                .and_then(|digits| digits.parse::<usize>().ok())
            {
                keys.push(FragmentKey {
                    object_id: object_id.clone(),
                    index,
                });
            }
        }
    }
    keys
}

/// Writes `bytes` to `path` durably with a trailing CRC32C, so a later load can
/// detect a bit-flip (#220): write `payload || checksum` to a temp file, fsync
/// it, rename into place, then fsync the directory so the rename survives a
/// crash.
fn write_fsynced(path: &Path, bytes: &[u8], checksum: u32) -> Result<(), FragmentIoError> {
    let parent = path
        .parent()
        .ok_or_else(|| FragmentIoError::io(format!("{} has no parent", path.display())))?;
    fs::create_dir_all(parent).map_err(|error| {
        FragmentIoError::io(format!("create fragment dir {}: {error}", parent.display()))
    })?;
    let tmp = path.with_extension("tmp");
    {
        let mut file = File::create(&tmp)
            .map_err(|error| FragmentIoError::io(format!("create {}: {error}", tmp.display())))?;
        file.write_all(bytes)
            .map_err(|error| FragmentIoError::io(format!("write {}: {error}", tmp.display())))?;
        file.write_all(&checksum.to_le_bytes()).map_err(|error| {
            FragmentIoError::io(format!("write trailer {}: {error}", tmp.display()))
        })?;
        file.sync_all()
            .map_err(|error| FragmentIoError::io(format!("fsync {}: {error}", tmp.display())))?;
    }
    fs::rename(&tmp, path).map_err(|error| {
        FragmentIoError::io(format!(
            "rename {} to {}: {error}",
            tmp.display(),
            path.display()
        ))
    })?;
    sync_dir(parent)
}

/// Fsyncs a directory so a create or rename within it is durable.
fn sync_dir(path: &Path) -> Result<(), FragmentIoError> {
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|error| FragmentIoError::io(format!("fsync dir {}: {error}", path.display())))
}

/// Encodes an object id as a path-safe lowercase hex component so arbitrary
/// key bytes never escape the store directory.
fn hex_component(input: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(input.len() * 2);
    for &byte in input.as_bytes() {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

/// Decodes a hex object-directory component back to the object id, or `None` if
/// it is not valid hex. The inverse of [`hex_component`], used when the scrubber
/// walks the store and rebuilds fragment keys from directory names (#220).
fn hex_component_decode(input: &str) -> Option<String> {
    if !input.len().is_multiple_of(2) {
        return None;
    }
    let nibble = |b: u8| match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        _ => None,
    };
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(input.len() / 2);
    for pair in bytes.chunks_exact(2) {
        let hi = nibble(pair[0])?;
        let lo = nibble(pair[1])?;
        out.push((hi << 4) | lo);
    }
    String::from_utf8(out).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A unique scratch directory per test.
    fn scratch(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("verglas-frag-{}-{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create scratch");
        dir
    }

    /// A stored fragment reads back byte-identical, and its load carries the
    /// checksum computed over the payload (#220).
    #[test]
    fn fragment_round_trips() {
        let store = LocalFragmentStore::new(scratch("roundtrip"));
        let record = FragmentRecord::new(
            FragmentKey {
                object_id: "bucket/key/etag".to_owned(),
                index: 2,
            },
            Bytes::from_static(b"fragment-bytes"),
        );
        store.store_fragment(&record).expect("store");
        let got = store
            .load_fragment(&record.key)
            .expect("load")
            .expect("present");
        assert_eq!(got.bytes, record.bytes);
        assert_eq!(
            got.checksum, record.checksum,
            "checksum survives round-trip"
        );
        assert_eq!(
            got.checksum,
            crate::fragments::fragment_checksum(&record.bytes)
        );
    }

    /// A byte flipped on disk after the store makes the fragment fail
    /// verification: the trailer still describes the original bytes, so the
    /// corruption is caught (#220).
    #[test]
    fn disk_bit_flip_is_detected_by_verify() {
        let store = LocalFragmentStore::new(scratch("bitflip"));
        let key = FragmentKey {
            object_id: "obj".to_owned(),
            index: 0,
        };
        store
            .store_fragment(&FragmentRecord::new(
                key.clone(),
                Bytes::from(vec![7u8; 512]),
            ))
            .expect("store");
        assert_eq!(
            store.verify_fragment(&key).expect("verify"),
            FragmentHealth::Healthy
        );

        // Flip one payload byte directly in the on-disk file.
        let path = store.fragment_path(&key);
        let mut raw = std::fs::read(&path).expect("read raw");
        raw[10] ^= 0xff;
        std::fs::write(&path, &raw).expect("rewrite corrupt");

        // The load still returns the (now-corrupt) bytes plus the ORIGINAL
        // trailer, so the mismatch is detectable, and verify reports Corrupt.
        let loaded = store.load_fragment(&key).expect("load").expect("present");
        assert_ne!(
            fragment_checksum(&loaded.bytes),
            loaded.checksum,
            "loaded bytes no longer match the stored checksum"
        );
        assert_eq!(
            store.verify_fragment(&key).expect("verify"),
            FragmentHealth::Corrupt
        );
    }

    /// The store lists the keys of every fragment it holds, so the scrubber can
    /// walk them (#220).
    #[test]
    fn lists_stored_fragment_keys() {
        let store = LocalFragmentStore::new(scratch("listkeys"));
        for (object_id, index) in [("a", 0usize), ("a", 1), ("b", 0)] {
            store
                .store_fragment(&FragmentRecord::new(
                    FragmentKey {
                        object_id: object_id.to_owned(),
                        index,
                    },
                    Bytes::from(vec![index as u8; 8]),
                ))
                .expect("store");
        }
        let mut keys = store.list_fragment_keys();
        keys.sort_by(|a, b| (a.object_id.as_str(), a.index).cmp(&(b.object_id.as_str(), b.index)));
        assert_eq!(
            keys,
            vec![
                FragmentKey {
                    object_id: "a".to_owned(),
                    index: 0
                },
                FragmentKey {
                    object_id: "a".to_owned(),
                    index: 1
                },
                FragmentKey {
                    object_id: "b".to_owned(),
                    index: 0
                },
            ]
        );
    }

    /// A streamed fragment computes its trailer over the concatenated shards, so
    /// it verifies and loads back with the right checksum (#220).
    #[test]
    fn streamed_fragment_verifies() {
        let store = LocalFragmentStore::new(scratch("stream-verify"));
        let key = FragmentKey {
            object_id: "obj".to_owned(),
            index: 1,
        };
        let mut writer = store.open_fragment(&key).expect("open");
        writer.append(b"hello ").expect("append 1");
        writer.append(b"world").expect("append 2");
        writer.commit().expect("commit");
        let loaded = store.load_fragment(&key).expect("load").expect("present");
        assert_eq!(loaded.bytes.as_ref(), b"hello world");
        assert_eq!(loaded.checksum, fragment_checksum(b"hello world"));
        assert_eq!(
            store.verify_fragment(&key).expect("verify"),
            FragmentHealth::Healthy
        );
    }

    /// A missing fragment loads as `None`, and delete is idempotent.
    #[test]
    fn missing_fragment_is_none_and_delete_idempotent() {
        let store = LocalFragmentStore::new(scratch("missing"));
        let key = FragmentKey {
            object_id: "obj".to_owned(),
            index: 0,
        };
        assert!(store.load_fragment(&key).expect("load").is_none());
        store.delete_fragment(&key).expect("delete missing is ok");
    }

    /// Deleting an object removes all of its fragments.
    #[test]
    fn delete_object_removes_all_fragments() {
        let store = LocalFragmentStore::new(scratch("delobj"));
        for index in 0..3 {
            store
                .store_fragment(&FragmentRecord::new(
                    FragmentKey {
                        object_id: "obj".to_owned(),
                        index,
                    },
                    Bytes::from_static(b"x"),
                ))
                .expect("store");
        }
        store.delete_object("obj").expect("delete object");
        assert!(
            store
                .load_fragment(&FragmentKey {
                    object_id: "obj".to_owned(),
                    index: 0,
                })
                .expect("load")
                .is_none()
        );
    }

    /// A record of the given size, unique per (object, index).
    fn record(object_id: &str, index: usize, len: usize) -> FragmentRecord {
        FragmentRecord::new(
            FragmentKey {
                object_id: object_id.to_owned(),
                index,
            },
            Bytes::from(vec![index as u8; len]),
        )
    }

    /// A store with a byte budget refuses a fragment that would exceed the
    /// ceiling and reports it as [`FragmentStoreFull`], not a silent success.
    #[test]
    fn budget_refuses_over_ceiling() {
        let store = LocalFragmentStore::with_budget(scratch("budget-refuse"), 100);
        assert_eq!(store.budget_bytes(), 100);
        assert_eq!(store.used_bytes(), 0);
        // 60 bytes fits.
        store
            .store_fragment(&record("a", 0, 60))
            .expect("first fits");
        assert_eq!(store.used_bytes(), 60);
        // Another 60 would exceed 100; refused, and no bytes are charged.
        let err = store
            .store_fragment(&record("b", 0, 60))
            .expect_err("over ceiling");
        assert!(matches!(err, FragmentIoError::Full { .. }), "got {err:?}");
        assert_eq!(store.used_bytes(), 60, "a refused write charges nothing");
        // The refused fragment is not on disk.
        assert!(
            store
                .load_fragment(&FragmentKey {
                    object_id: "b".to_owned(),
                    index: 0,
                })
                .expect("load")
                .is_none()
        );
    }

    /// Deleting fragments releases their bytes back to the budget, so a later
    /// write that would not have fit now fits.
    #[test]
    fn delete_releases_budget() {
        let store = LocalFragmentStore::with_budget(scratch("budget-release"), 100);
        store.store_fragment(&record("a", 0, 60)).expect("store a");
        // b does not fit yet.
        assert!(store.store_fragment(&record("b", 0, 60)).is_err());
        // Free a; now b fits and used tracks it.
        store
            .delete_fragment(&FragmentKey {
                object_id: "a".to_owned(),
                index: 0,
            })
            .expect("delete a");
        assert_eq!(store.used_bytes(), 0);
        store
            .store_fragment(&record("b", 0, 60))
            .expect("store b now fits");
        assert_eq!(store.used_bytes(), 60);
    }

    /// `has_headroom` answers whether a write of `n` bytes would fit under the
    /// ceiling, so placement can exclude a full node before attempting a write.
    #[test]
    fn headroom_tracks_used_bytes() {
        let store = LocalFragmentStore::with_budget(scratch("budget-headroom"), 100);
        assert!(store.has_headroom(100));
        assert!(!store.has_headroom(101));
        store.store_fragment(&record("a", 0, 80)).expect("store");
        assert!(store.has_headroom(20));
        assert!(!store.has_headroom(21));
    }

    /// Reopening a store rebuilds the used-byte count from the fragments left on
    /// disk, so the ceiling holds across a restart (crash recovery).
    #[test]
    fn reopen_rebuilds_used_bytes() {
        let dir = scratch("budget-reopen");
        {
            let store = LocalFragmentStore::with_budget(&dir, 1000);
            store.store_fragment(&record("a", 0, 60)).expect("store a");
            store.store_fragment(&record("a", 1, 40)).expect("store a1");
        }
        let store = LocalFragmentStore::with_budget(&dir, 1000);
        assert_eq!(store.used_bytes(), 100, "used rebuilt from disk on reopen");
    }

    /// A streamed fragment (appended shard by shard, then committed) reads back
    /// as the concatenation of its shards and charges the budget once.
    #[test]
    fn streamed_fragment_round_trips_and_charges_budget() {
        let store = LocalFragmentStore::with_budget(scratch("stream-commit"), 1000);
        let key = FragmentKey {
            object_id: "obj".to_owned(),
            index: 3,
        };
        let mut writer = store.open_fragment(&key).expect("open");
        writer.append(b"aaa").expect("append 1");
        writer.append(b"bbbb").expect("append 2");
        writer.commit().expect("commit");
        assert_eq!(store.used_bytes(), 7);
        let got = store.load_fragment(&key).expect("load").expect("present");
        assert_eq!(got.bytes.as_ref(), b"aaabbbb");
    }

    /// A streaming write dropped before commit releases its reserved budget and
    /// leaves no live fragment.
    #[test]
    fn dropped_stream_releases_budget() {
        let store = LocalFragmentStore::with_budget(scratch("stream-drop"), 1000);
        let key = FragmentKey {
            object_id: "obj".to_owned(),
            index: 0,
        };
        {
            let mut writer = store.open_fragment(&key).expect("open");
            writer.append(b"partial").expect("append");
            assert_eq!(store.used_bytes(), 7, "reserved mid-stream");
            // Drop without commit.
        }
        assert_eq!(store.used_bytes(), 0, "budget released on drop");
        assert!(store.load_fragment(&key).expect("load").is_none());
    }

    /// A streaming write that would cross the ceiling is refused mid-stream and
    /// never becomes live.
    #[test]
    fn streaming_write_honors_ceiling() {
        let store = LocalFragmentStore::with_budget(scratch("stream-ceiling"), 10);
        let key = FragmentKey {
            object_id: "obj".to_owned(),
            index: 0,
        };
        let mut writer = store.open_fragment(&key).expect("open");
        writer.append(&[0u8; 6]).expect("first shard fits");
        let err = writer
            .append(&[0u8; 6])
            .expect_err("second shard overflows");
        assert!(matches!(err, FragmentIoError::Full { .. }));
        drop(writer);
        assert_eq!(store.used_bytes(), 0, "abandoned stream charges nothing");
        assert!(store.load_fragment(&key).expect("load").is_none());
    }

    /// A stored (un-propagated) fragment is never dropped by budget pressure:
    /// the budget refuses new writes but only an explicit delete frees a
    /// fragment. This is the invariant that keeps the pod's only durable copy of
    /// acked data safe until propagation deletes it.
    #[test]
    fn budget_never_evicts_a_stored_fragment() {
        let store = LocalFragmentStore::with_budget(scratch("budget-noevict"), 100);
        let pinned = FragmentKey {
            object_id: "dirty".to_owned(),
            index: 0,
        };
        store
            .store_fragment(&FragmentRecord::new(
                pinned.clone(),
                Bytes::from(vec![7u8; 90]),
            ))
            .expect("store the dirty fragment");
        // The store is now nearly full. Every further write is refused...
        for i in 0..5 {
            assert!(
                store.store_fragment(&record("other", i, 90)).is_err(),
                "further writes refused under pressure"
            );
        }
        // ...but the pinned fragment is untouched and still reads back.
        assert_eq!(store.used_bytes(), 90);
        let got = store
            .load_fragment(&pinned)
            .expect("load")
            .expect("present");
        assert_eq!(
            got.bytes.len(),
            90,
            "the dirty fragment survives disk pressure"
        );
        // Only an explicit delete (propagation cleanup) frees it.
        store.delete_fragment(&pinned).expect("delete");
        assert_eq!(store.used_bytes(), 0);
    }

    /// The fragment store's ceiling is dynamic (#223): a shared atomic the
    /// daemon's disk poll updates. A write burst is absorbed up to the current
    /// ceiling (far past the retired flat 10% cap); raising the ceiling (free
    /// NVMe appeared) lets the burst continue; lowering it below what is already
    /// stored refuses new writes but never drops an acked fragment; and
    /// propagation delete reclaims the space so it is never permanently consumed.
    #[test]
    fn dynamic_ceiling_flexes_and_reclaims() {
        let ceiling = Arc::new(AtomicU64::new(1000));
        let store =
            LocalFragmentStore::with_dynamic_ceiling(scratch("dyn-ceiling"), Arc::clone(&ceiling));
        // A burst up to the current ceiling is absorbed.
        store.store_fragment(&record("a", 0, 900)).expect("fits");
        assert!(
            store.store_fragment(&record("b", 0, 200)).is_err(),
            "past the current ceiling is refused"
        );
        // Free NVMe appeared: raise the ceiling and the burst continues.
        ceiling.store(2000, Ordering::Release);
        assert_eq!(store.budget_bytes(), 2000, "budget tracks the live ceiling");
        store
            .store_fragment(&record("b", 0, 200))
            .expect("fits once the ceiling rises");
        // The disk filled: the ceiling shrinks below what is stored. New writes
        // are refused, but the stored (acked) fragments are kept.
        ceiling.store(100, Ordering::Release);
        assert!(
            store.store_fragment(&record("c", 0, 50)).is_err(),
            "no new writes under a shrunk ceiling"
        );
        assert_eq!(
            store.used_bytes(),
            1100,
            "stored fragments survive a shrunk ceiling"
        );
        // Propagation cleanup reclaims the space.
        store.delete_object("a").expect("propagated");
        store.delete_object("b").expect("propagated");
        assert_eq!(store.used_bytes(), 0, "reclaimed after propagation");
    }

    /// An unbudgeted store (the default) never refuses on budget grounds.
    #[test]
    fn unbudgeted_store_has_infinite_headroom() {
        let store = LocalFragmentStore::new(scratch("budget-none"));
        assert_eq!(store.budget_bytes(), u64::MAX);
        assert!(store.has_headroom(u64::MAX));
        store
            .store_fragment(&record("a", 0, 1_000_000))
            .expect("store");
    }
}
