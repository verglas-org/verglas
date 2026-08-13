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
        if state.state == TransactionState::Dirty {
            self.insert_index(&state);
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

    pub fn dirty_object_ids(&self) -> Vec<String> {
        self.dirty
            .read()
            .map(|m| m.values().cloned().collect())
            .unwrap_or_default()
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
    }
}
