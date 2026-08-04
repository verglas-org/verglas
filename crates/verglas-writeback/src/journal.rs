//! The write-back journal (#180): the durable record that makes an acked but
//! not-yet-propagated object real and readable.
//!
//! One JSON journal per object records its identity, geometry, metadata, and
//! which node holds each fragment. A journal is written and fsynced *before*
//! the client is acked, so a crash after ack can replay it: reassemble from the
//! surviving fragments and finish the origin upload. An in-memory dirty index
//! keyed by `(bucket, key)` answers read-your-writes lookups; when it is empty
//! (no unpropagated objects) the read path pays a single atomic load and skips
//! the map entirely, so an enabled-but-idle tier costs the read path nothing.

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::RwLock;
use std::sync::atomic::{AtomicUsize, Ordering};

use serde::{Deserialize, Serialize};

use crate::meta::{StoredMetadata, now_unix_ms};

/// Whether an object has propagated to the origin yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JournalState {
    /// Acked on the pod, not yet uploaded to the origin. The pod is the only
    /// copy — this is the durability-contract window.
    Dirty,
    /// Uploaded to the origin and confirmed durable there.
    Clean,
}

/// Which node holds one fragment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Placement {
    /// Fragment index (`0..k` data, `k..k+m` parity).
    pub index: usize,
    /// The node id holding this fragment.
    pub node: String,
}

/// The per-object journal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Journal {
    /// Pod-unique object id assigned by the write coordinator.
    pub object_id: String,
    /// Origin bucket.
    pub bucket: String,
    /// Origin key.
    pub key: String,
    /// Client PUT metadata to report on read and store on propagation.
    pub metadata: StoredMetadata,
    /// Original object length before stripe padding.
    pub object_len: u64,
    /// Reed-Solomon data fragments.
    pub k: usize,
    /// Reed-Solomon parity fragments.
    pub m: usize,
    /// Per-fragment stripe chunk size.
    pub chunk: usize,
    /// Synthetic ETag reported during the dirty window (opaque; the origin
    /// assigns the durable ETag on propagation).
    pub etag: String,
    /// Millisecond Unix time the write was acked.
    pub created_ms: u64,
    /// Dirty or clean.
    pub state: JournalState,
    /// Millisecond Unix time propagation completed, when clean.
    pub propagated_ms: Option<u64>,
    /// Where each fragment lives.
    pub placements: Vec<Placement>,
}

impl Journal {
    /// The `(bucket, key)` pair used as the dirty-index key.
    fn index_key(&self) -> (String, String) {
        (self.bucket.clone(), self.key.clone())
    }
}

/// Filesystem-backed journal store plus the in-memory dirty index.
pub struct JournalStore {
    /// Directory holding the JSON journals.
    dir: PathBuf,
    /// `(bucket, key) -> object_id` for dirty objects. Guards read-your-writes.
    dirty: RwLock<HashMap<(String, String), String>>,
    /// Count of dirty entries. When zero, the read path skips the index lock.
    dirty_count: AtomicUsize,
    /// Serializes read-modify-write journal mutations so a straggler merge and
    /// a repair pass touching the same journal never interleave.
    write_lock: std::sync::Mutex<()>,
}

impl JournalStore {
    /// Opens (creating if needed) a journal store under `cache_dir`, rebuilding
    /// the dirty index from any journals left by a previous run.
    pub fn open(cache_dir: impl AsRef<Path>) -> Result<Self, JournalError> {
        let dir = cache_dir.as_ref().join("writeback-journals");
        fs::create_dir_all(&dir)
            .map_err(|e| JournalError(format!("create journal dir {}: {e}", dir.display())))?;
        let store = Self {
            dir,
            dirty: RwLock::new(HashMap::new()),
            dirty_count: AtomicUsize::new(0),
            write_lock: std::sync::Mutex::new(()),
        };
        for journal in store.list()? {
            if journal.state == JournalState::Dirty {
                store.insert_index(&journal);
            }
        }
        Ok(store)
    }

    /// Writes and fsyncs `journal`, and if it is dirty registers it in the
    /// index so a read-your-writes GET finds it.
    pub fn put(&self, journal: &Journal) -> Result<(), JournalError> {
        let _guard = self.write_lock.lock();
        self.write_fsynced(journal)?;
        if journal.state == JournalState::Dirty {
            self.insert_index(journal);
        }
        Ok(())
    }

    /// Marks the object clean and removes it from the dirty index. The journal
    /// stays on disk (clean) until propagation cleanup deletes it.
    pub fn mark_clean(&self, object_id: &str) -> Result<(), JournalError> {
        let _guard = self.write_lock.lock();
        let Some(mut journal) = self.read(object_id)? else {
            return Ok(());
        };
        journal.state = JournalState::Clean;
        journal.propagated_ms = Some(now_unix_ms());
        self.write_fsynced(&journal)?;
        self.remove_index(&journal);
        Ok(())
    }

    /// Replaces the placements of a dirty journal after repair, re-fsyncing.
    pub fn update_placements(
        &self,
        object_id: &str,
        placements: Vec<Placement>,
    ) -> Result<(), JournalError> {
        let _guard = self.write_lock.lock();
        let Some(mut journal) = self.read(object_id)? else {
            return Ok(());
        };
        journal.placements = placements;
        self.write_fsynced(&journal)
    }

    /// Adds one straggler placement to a dirty journal if the index is not
    /// already recorded. Merges into the current on-disk journal so it never
    /// clobbers a concurrent repair's placements.
    pub fn add_placement(&self, object_id: &str, placement: Placement) -> Result<(), JournalError> {
        let _guard = self.write_lock.lock();
        let Some(mut journal) = self.read(object_id)? else {
            return Ok(());
        };
        if journal
            .placements
            .iter()
            .any(|p| p.index == placement.index)
        {
            return Ok(());
        }
        journal.placements.push(placement);
        self.write_fsynced(&journal)
    }

    /// Deletes a journal from disk and from the index.
    pub fn delete(&self, object_id: &str) -> Result<(), JournalError> {
        if let Some(journal) = self.read(object_id)? {
            self.remove_index(&journal);
        }
        let path = self.path(object_id);
        match fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(JournalError(format!("delete {}: {e}", path.display()))),
        }
    }

    /// Looks up the dirty object id for `(bucket, key)`, or `None`. Cheap when
    /// nothing is dirty: one relaxed atomic load and no lock.
    pub fn find_dirty(&self, bucket: &str, key: &str) -> Option<String> {
        if self.dirty_count.load(Ordering::Relaxed) == 0 {
            return None;
        }
        let guard = self.dirty.read().ok()?;
        guard.get(&(bucket.to_owned(), key.to_owned())).cloned()
    }

    /// Returns true when no object is currently dirty (nothing to read back).
    pub fn is_idle(&self) -> bool {
        self.dirty_count.load(Ordering::Relaxed) == 0
    }

    /// The object ids currently dirty, for propagation replay and repair.
    pub fn dirty_object_ids(&self) -> Vec<String> {
        self.dirty
            .read()
            .map(|g| g.values().cloned().collect())
            .unwrap_or_default()
    }

    /// Reads one journal by object id, `None` if absent.
    pub fn read(&self, object_id: &str) -> Result<Option<Journal>, JournalError> {
        let path = self.path(object_id);
        match fs::read(&path) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map(Some)
                .map_err(|e| JournalError(format!("decode {}: {e}", path.display()))),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(JournalError(format!("read {}: {e}", path.display()))),
        }
    }

    /// Lists every journal on disk.
    pub fn list(&self) -> Result<Vec<Journal>, JournalError> {
        let entries = match fs::read_dir(&self.dir) {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(JournalError(format!("list {}: {e}", self.dir.display()))),
        };
        let mut journals = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|e| JournalError(format!("dir entry: {e}")))?;
            if entry.path().extension().and_then(|x| x.to_str()) != Some("json") {
                continue;
            }
            let bytes = fs::read(entry.path())
                .map_err(|e| JournalError(format!("read {}: {e}", entry.path().display())))?;
            let journal = serde_json::from_slice::<Journal>(&bytes)
                .map_err(|e| JournalError(format!("decode {}: {e}", entry.path().display())))?;
            journals.push(journal);
        }
        journals.sort_by(|a, b| a.object_id.cmp(&b.object_id));
        Ok(journals)
    }

    /// Adds a dirty journal to the index, bumping the count only for a new key.
    fn insert_index(&self, journal: &Journal) {
        if let Ok(mut guard) = self.dirty.write()
            && guard
                .insert(journal.index_key(), journal.object_id.clone())
                .is_none()
        {
            self.dirty_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Removes a journal from the index if it is the current holder of its key.
    fn remove_index(&self, journal: &Journal) {
        if let Ok(mut guard) = self.dirty.write() {
            let index_key = journal.index_key();
            if guard.get(&index_key) == Some(&journal.object_id)
                && guard.remove(&index_key).is_some()
            {
                self.dirty_count.fetch_sub(1, Ordering::Relaxed);
            }
        }
    }

    /// The JSON path for an object id (hex-encoded so keys are path-safe).
    fn path(&self, object_id: &str) -> PathBuf {
        self.dir.join(format!("{}.json", hex(object_id)))
    }

    /// Writes the journal atomically and fsyncs the file and directory. The
    /// temp file name is unique per write so concurrent rewriters (a straggler
    /// collector and a repair pass touching the same journal) never collide.
    fn write_fsynced(&self, journal: &Journal) -> Result<(), JournalError> {
        let path = self.path(&journal.object_id);
        let bytes = serde_json::to_vec_pretty(journal)
            .map_err(|e| JournalError(format!("encode journal: {e}")))?;
        static TMP_SEQ: AtomicUsize = AtomicUsize::new(0);
        let nonce = TMP_SEQ.fetch_add(1, Ordering::Relaxed);
        let tmp = path.with_extension(format!("{}.{nonce}.tmp", std::process::id()));
        {
            let mut file = File::create(&tmp)
                .map_err(|e| JournalError(format!("create {}: {e}", tmp.display())))?;
            file.write_all(&bytes)
                .map_err(|e| JournalError(format!("write {}: {e}", tmp.display())))?;
            file.sync_all()
                .map_err(|e| JournalError(format!("fsync {}: {e}", tmp.display())))?;
        }
        fs::rename(&tmp, &path)
            .map_err(|e| JournalError(format!("rename into {}: {e}", path.display())))?;
        File::open(&self.dir)
            .and_then(|f| f.sync_all())
            .map_err(|e| JournalError(format!("fsync dir {}: {e}", self.dir.display())))
    }
}

/// A journal store error.
#[derive(Debug, thiserror::Error)]
#[error("writeback journal: {0}")]
pub struct JournalError(pub String);

/// Hex-encodes an object id so it is a safe filename component.
fn hex(input: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(input.len() * 2);
    for &b in input.as_bytes() {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A minimal dirty journal for `(bucket, key)`.
    fn dirty(object_id: &str, bucket: &str, key: &str) -> Journal {
        Journal {
            object_id: object_id.to_owned(),
            bucket: bucket.to_owned(),
            key: key.to_owned(),
            metadata: StoredMetadata::default(),
            object_len: 10,
            k: 2,
            m: 1,
            chunk: 8,
            etag: "wb-etag".to_owned(),
            created_ms: now_unix_ms(),
            state: JournalState::Dirty,
            propagated_ms: None,
            placements: vec![],
        }
    }

    /// A dirty journal is found by (bucket,key); clean removes it; idle is cheap.
    #[test]
    fn dirty_index_tracks_state() {
        let dir = tempfile::tempdir().expect("tmp");
        let store = JournalStore::open(dir.path()).expect("open");
        assert!(store.is_idle());
        store.put(&dirty("obj-1", "bkt", "k")).expect("put");
        assert!(!store.is_idle());
        assert_eq!(store.find_dirty("bkt", "k").as_deref(), Some("obj-1"));
        store.mark_clean("obj-1").expect("clean");
        assert!(store.is_idle());
        assert_eq!(store.find_dirty("bkt", "k"), None);
    }

    /// The dirty index is rebuilt from disk on reopen (crash replay).
    #[test]
    fn reopen_rebuilds_dirty_index() {
        let dir = tempfile::tempdir().expect("tmp");
        {
            let store = JournalStore::open(dir.path()).expect("open");
            store.put(&dirty("obj-1", "bkt", "k")).expect("put");
        }
        let store = JournalStore::open(dir.path()).expect("reopen");
        assert!(!store.is_idle());
        assert_eq!(store.find_dirty("bkt", "k").as_deref(), Some("obj-1"));
    }
}
