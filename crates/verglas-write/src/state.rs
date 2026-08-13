//! Immutable transaction state carried by the fragment log.
//!
//! The local index in this module is intentionally volatile.  A transaction is
//! acknowledged only after its encoded state has been appended (and fdatasync'd)
//! on the same `w` members as its data records.  Restart recovery repopulates
//! this projection by listing and loading `tx-state:` fragment records.

use std::collections::HashMap;
use std::sync::RwLock;
use std::sync::atomic::{AtomicUsize, Ordering};

use serde::{Deserialize, Serialize};

use crate::meta::StoredMetadata;

/// Whether a revision retains fragments or releases them after origin drain.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransactionState {
    Dirty,
    /// A quorum-proven delete.  It deliberately stays in the fragment log
    /// until the origin has observed the delete, so a restarted pod cannot
    /// resurrect a key that was already acknowledged absent.
    Tombstone,
    Released,
}

/// A fragment's durable placement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Placement {
    pub index: usize,
    pub node: String,
}

/// A self-describing immutable transaction revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransactionRecord {
    pub object_id: String,
    pub storage_binding_id: String,
    pub bucket: String,
    pub key: String,
    pub metadata: StoredMetadata,
    pub object_len: u64,
    pub k: usize,
    pub m: usize,
    /// Number of distinct durable fragment/state records required for this transaction.
    pub w: usize,
    pub chunk: usize,
    pub etag: String,
    pub created_ms: u64,
    pub state: TransactionState,
    pub propagated_ms: Option<u64>,
    pub placements: Vec<Placement>,
    /// Higher revisions supersede older immutable records during recovery.
    #[serde(default)]
    pub revision: u64,
    /// Multipart upload owning this record, when this is an upload manifest or
    /// an internal part object.  Kept in the immutable record rather than a
    /// process-local bookkeeping so restart recovery has the same authority.
    #[serde(default)]
    pub upload_id: Option<String>,
    /// Client part number for an internal multipart part.
    #[serde(default)]
    pub part_number: Option<u16>,
    /// A multipart upload manifest has no EC body of its own.  Its state
    /// replicas still form the durable create/abort/complete transaction.
    #[serde(default)]
    pub multipart_manifest: bool,
}

impl TransactionRecord {
    fn index_key(&self) -> (String, String, String) {
        (
            self.storage_binding_id.clone(),
            self.bucket.clone(),
            self.key.clone(),
        )
    }
}

/// Disposable in-memory projection of quorum-proven transaction records.
pub struct StateIndex {
    records: RwLock<HashMap<String, TransactionRecord>>,
    dirty: RwLock<HashMap<(String, String, String), String>>,
    tombstones: RwLock<HashMap<(String, String, String), String>>,
    dirty_count: AtomicUsize,
}

impl Default for StateIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl StateIndex {
    pub fn new() -> Self {
        Self {
            records: RwLock::new(HashMap::new()),
            dirty: RwLock::new(HashMap::new()),
            tombstones: RwLock::new(HashMap::new()),
            dirty_count: AtomicUsize::new(0),
        }
    }

    /// Installs a state record that the coordinator has already proven durable.
    pub fn install(&self, state: TransactionRecord) {
        let mut records = self.records.write().expect("state index poisoned");
        if records
            .get(&state.object_id)
            .is_some_and(|old| old.revision > state.revision)
        {
            return;
        }
        if let Some(old) = records.insert(state.object_id.clone(), state.clone()) {
            self.remove_index(&old);
        }
        if state.state == TransactionState::Dirty
            && !state.multipart_manifest
            && state.part_number.is_none()
        {
            self.insert_index(&state);
        } else if state.state == TransactionState::Tombstone
            && let Ok(mut tombstones) = self.tombstones.write()
        {
            tombstones.insert(state.index_key(), state.object_id.clone());
        }
    }

    pub fn read(&self, object_id: &str) -> Option<TransactionRecord> {
        self.records.read().ok()?.get(object_id).cloned()
    }

    pub fn find_dirty(&self, binding: &str, bucket: &str, key: &str) -> Option<String> {
        if self.dirty_count.load(Ordering::Relaxed) == 0 {
            return None;
        }
        self.dirty
            .read()
            .ok()?
            .get(&(binding.to_owned(), bucket.to_owned(), key.to_owned()))
            .cloned()
    }

    pub fn is_idle(&self) -> bool {
        self.dirty_count.load(Ordering::Relaxed) == 0
    }

    /// A tombstone wins over the origin until its background delete completes.
    pub fn is_tombstoned(&self, binding: &str, bucket: &str, key: &str) -> bool {
        self.tombstones
            .read()
            .is_ok_and(|m| m.contains_key(&(binding.to_owned(), bucket.to_owned(), key.to_owned())))
    }

    pub fn dirty_object_ids(&self) -> Vec<String> {
        self.dirty
            .read()
            .map(|m| m.values().cloned().collect())
            .unwrap_or_default()
    }

    /// Tombstones are separate from dirty data: recovery must resume their
    /// origin deletion even though they are intentionally absent to readers.
    pub fn tombstone_object_ids(&self) -> Vec<String> {
        self.tombstones
            .read()
            .map(|m| m.values().cloned().collect())
            .unwrap_or_default()
    }

    /// Finds the quorum-proven manifest for an in-flight multipart upload.
    pub fn multipart_upload(
        &self,
        binding: &str,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Option<TransactionRecord> {
        self.records
            .read()
            .ok()?
            .values()
            .find(|record| {
                record.state == TransactionState::Dirty
                    && record.multipart_manifest
                    && record.storage_binding_id == binding
                    && record.bucket == bucket
                    && record.key == key
                    && record.upload_id.as_deref() == Some(upload_id)
            })
            .cloned()
    }

    /// Returns the current immutable records for an upload's parts, sorted as
    /// S3 requires.  Superseded revisions never appear because `records` is
    /// keyed by object id and install only retains its highest revision.
    pub fn multipart_parts(&self, upload_id: &str) -> Vec<TransactionRecord> {
        let mut parts: Vec<_> = self
            .records
            .read()
            .map(|records| {
                records
                    .values()
                    .filter(|record| {
                        record.state == TransactionState::Dirty
                            && !record.multipart_manifest
                            && record.upload_id.as_deref() == Some(upload_id)
                            && record.part_number.is_some()
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        parts.sort_by_key(|record| record.part_number);
        parts
    }

    fn insert_index(&self, state: &TransactionRecord) {
        if let Ok(mut dirty) = self.dirty.write()
            && dirty
                .insert(state.index_key(), state.object_id.clone())
                .is_none()
        {
            self.dirty_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn remove_index(&self, state: &TransactionRecord) {
        if let Ok(mut dirty) = self.dirty.write() {
            let key = state.index_key();
            if dirty.get(&key) == Some(&state.object_id) && dirty.remove(&key).is_some() {
                self.dirty_count.fetch_sub(1, Ordering::Relaxed);
            }
        }
        if state.state == TransactionState::Tombstone
            && let Ok(mut tombstones) = self.tombstones.write()
        {
            let key = state.index_key();
            if tombstones.get(&key) == Some(&state.object_id) {
                tombstones.remove(&key);
            }
        }
    }
}
