//! The cluster-local shadow store for index blobs.
//!
//! Issue #95 specifies a shadow artifact store — "everything Verglas produces
//! autonomously lives in the Verglas-controlled shadow store; Verglas never
//! writes to customer tables" — with layout `<shadow-prefix>/<table-uuid>/
//! <snapshot-id>/...`. That store is not yet built in `verglas-cache`, so this
//! defines the minimal cluster-local blob store the vector index needs, behind a
//! [`ShadowBlobStore`] trait so placement is injected and the engine stays
//! testable. The extension point is explicit: when `verglas-cache` grows the
//! real shadow store (a sibling of `writeback-journals/` under `cache.dir`,
//! shared into the one NVMe budget), the daemon swaps its `ShadowBlobStore`
//! implementation and nothing else changes.
//!
//! Keying (per the operator constraint): an index blob is addressed by
//! `(target, field, reflected-snapshot)` where `target` is a stable logical id
//! of the source table or graph (e.g. `tbl:default.docs` or `graph:kg`), `field`
//! is the embedding column, and the snapshot is the source watermark the blob
//! reflects. This is enough for a blob to know what it indexes while staying
//! local — no catalog entry, no source-snapshot binding.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use async_trait::async_trait;

use crate::error::{Result, VectorError};

/// Addresses an index within the shadow store, minus the snapshot version.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IndexKey {
    /// A stable logical id for the indexed table or graph, e.g. `tbl:ns.table`
    /// or `graph:namespace`.
    pub target: String,
    /// The embedding field (column) the index is built over.
    pub field: String,
}

impl IndexKey {
    /// Key for a plain table's embedding field.
    pub fn table(ident: &str, field: &str) -> Self {
        IndexKey {
            target: format!("tbl:{ident}"),
            field: field.to_owned(),
        }
    }
    /// Key for a graph's node-table embedding field.
    pub fn graph(namespace: &str, field: &str) -> Self {
        IndexKey {
            target: format!("graph:{namespace}"),
            field: field.to_owned(),
        }
    }
    /// A filesystem-safe rendering of the key (each component sanitized).
    fn path_parts(&self) -> (String, String) {
        (sanitize(&self.target), sanitize(&self.field))
    }
}

/// A blob resolved from the store: where it lives, which snapshot it reflects,
/// and its bytes.
#[derive(Debug, Clone)]
pub struct StoredBlob {
    /// The store location (a path or logical handle).
    pub location: String,
    /// The source snapshot this blob reflects.
    pub snapshot: i64,
    /// The Puffin file bytes.
    pub bytes: Vec<u8>,
}

/// A cluster-local blob store for derived index artifacts. Implementations must
/// be crash-consistent enough that a `put` which returns `Ok` is durable before
/// the maintenance MV advances its watermark (commit-before-ack).
#[async_trait]
pub trait ShadowBlobStore: Send + Sync {
    /// Persists `bytes` for `(key, snapshot)`, returning the stored location.
    /// Overwrites any existing blob at the same `(key, snapshot)`.
    async fn put(&self, key: &IndexKey, snapshot: i64, bytes: Vec<u8>) -> Result<StoredBlob>;

    /// The most recently WRITTEN blob for `key`, or `None` if none. Ordered by
    /// write recency, not snapshot id (Iceberg snapshot ids are not monotonic).
    async fn latest(&self, key: &IndexKey) -> Result<Option<StoredBlob>>;

    /// The blob for an exact `(key, snapshot)`, or `None`.
    async fn get(&self, key: &IndexKey, snapshot: i64) -> Result<Option<StoredBlob>>;

    /// The snapshots for which a blob exists under `key`, ascending.
    async fn snapshots(&self, key: &IndexKey) -> Result<Vec<i64>>;
}

/// One stored version: a monotonic write sequence, the reflected snapshot, and
/// the bytes. `latest` is the entry with the highest `seq` — deliberately NOT
/// the highest snapshot id, because Iceberg snapshot ids are random i64 and are
/// not monotonic, so "most recent index" must be tracked by write recency.
#[derive(Debug, Clone)]
struct Version {
    seq: u64,
    snapshot: i64,
    bytes: Vec<u8>,
}

/// An in-memory shadow store. The daemon's default until the NVMe-backed store
/// exists; also the hermetic-test backing.
#[derive(Debug, Default)]
pub struct InMemoryShadowStore {
    inner: Mutex<HashMap<IndexKey, Vec<Version>>>,
    seq: std::sync::atomic::AtomicU64,
}

impl InMemoryShadowStore {
    /// A fresh empty store.
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl ShadowBlobStore for InMemoryShadowStore {
    async fn put(&self, key: &IndexKey, snapshot: i64, bytes: Vec<u8>) -> Result<StoredBlob> {
        let location = format!("mem://{}/{}/{snapshot}", key.target, key.field);
        let seq = self.seq.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut guard = self
            .inner
            .lock()
            .map_err(|e| VectorError::Store(e.to_string()))?;
        let versions = guard.entry(key.clone()).or_default();
        versions.retain(|v| v.snapshot != snapshot);
        versions.push(Version {
            seq,
            snapshot,
            bytes: bytes.clone(),
        });
        Ok(StoredBlob {
            location,
            snapshot,
            bytes,
        })
    }

    async fn latest(&self, key: &IndexKey) -> Result<Option<StoredBlob>> {
        let guard = self
            .inner
            .lock()
            .map_err(|e| VectorError::Store(e.to_string()))?;
        Ok(guard.get(key).and_then(|versions| {
            versions.iter().max_by_key(|v| v.seq).map(|v| StoredBlob {
                location: format!("mem://{}/{}/{}", key.target, key.field, v.snapshot),
                snapshot: v.snapshot,
                bytes: v.bytes.clone(),
            })
        }))
    }

    async fn get(&self, key: &IndexKey, snapshot: i64) -> Result<Option<StoredBlob>> {
        let guard = self
            .inner
            .lock()
            .map_err(|e| VectorError::Store(e.to_string()))?;
        Ok(guard.get(key).and_then(|versions| {
            versions
                .iter()
                .find(|v| v.snapshot == snapshot)
                .map(|v| StoredBlob {
                    location: format!("mem://{}/{}/{snapshot}", key.target, key.field),
                    snapshot,
                    bytes: v.bytes.clone(),
                })
        }))
    }

    async fn snapshots(&self, key: &IndexKey) -> Result<Vec<i64>> {
        let guard = self
            .inner
            .lock()
            .map_err(|e| VectorError::Store(e.to_string()))?;
        let mut out: Vec<i64> = guard
            .get(key)
            .map(|versions| versions.iter().map(|v| v.snapshot).collect())
            .unwrap_or_default();
        out.sort_unstable();
        Ok(out)
    }
}

/// A directory-backed shadow store under `cache.dir`, the placement a daemon
/// uses today. Layout mirrors #95:
/// `<root>/<target>/<field>/<seq:020>-<snapshot>.puffin`, where `seq` is a
/// monotonic write sequence (so `latest` orders by recency) and `snapshot` is
/// the reflected source snapshot.
/// This is deliberately a plain directory, not the foyer device — index blobs
/// are whole-artifact, write-once, and read whole (#90's whole-shard residency),
/// so they do not belong in the block cache.
#[derive(Debug)]
pub struct LocalDirShadowStore {
    root: PathBuf,
}

impl LocalDirShadowStore {
    /// Roots the store at `<cache_dir>/vector-index`.
    pub fn under_cache_dir(cache_dir: impl AsRef<Path>) -> Self {
        LocalDirShadowStore {
            root: cache_dir.as_ref().join("vector-index"),
        }
    }

    /// Roots the store at an explicit directory (tests).
    pub fn at(root: impl AsRef<Path>) -> Self {
        LocalDirShadowStore {
            root: root.as_ref().to_path_buf(),
        }
    }

    fn dir_for(&self, key: &IndexKey) -> PathBuf {
        let (t, f) = key.path_parts();
        self.root.join(t).join(f)
    }

    /// Lists `(seq, snapshot, filename)` for every blob under `key`. Filenames
    /// are `<seq:020>-<snapshot>.puffin`, so the sequence orders by write
    /// recency independently of the (non-monotonic) snapshot id.
    async fn versions(&self, key: &IndexKey) -> Result<Vec<(u64, i64, String)>> {
        let dir = self.dir_for(key);
        let mut out = Vec::new();
        let mut rd = match tokio::fs::read_dir(&dir).await {
            Ok(rd) => rd,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(out),
            Err(e) => return Err(e.into()),
        };
        while let Some(entry) = rd.next_entry().await? {
            let name = entry.file_name().to_string_lossy().into_owned();
            if let Some(stem) = name.strip_suffix(".puffin")
                && let Some((seq_s, snap_s)) = stem.split_once('-')
                && let Ok(seq) = seq_s.parse::<u64>()
                && let Ok(snap) = snap_s.parse::<i64>()
            {
                out.push((seq, snap, name));
            }
        }
        Ok(out)
    }
}

#[async_trait]
impl ShadowBlobStore for LocalDirShadowStore {
    async fn put(&self, key: &IndexKey, snapshot: i64, bytes: Vec<u8>) -> Result<StoredBlob> {
        let dir = self.dir_for(key);
        tokio::fs::create_dir_all(&dir).await?;
        // Next write sequence = one past the highest on disk (monotonic across
        // restarts because it is derived from the persisted filenames).
        let next_seq = self
            .versions(key)
            .await?
            .iter()
            .map(|(seq, _, _)| *seq)
            .max()
            .map_or(0, |m| m + 1);
        let filename = format!("{next_seq:020}-{snapshot}.puffin");
        let path = dir.join(&filename);
        // Write to a temp sibling then rename, so a crash mid-write never leaves
        // a half-written blob that the loader would treat as corrupt.
        let tmp = dir.join(format!(".{filename}.tmp"));
        tokio::fs::write(&tmp, &bytes).await?;
        tokio::fs::rename(&tmp, &path).await?;
        Ok(StoredBlob {
            location: path.to_string_lossy().into_owned(),
            snapshot,
            bytes,
        })
    }

    async fn latest(&self, key: &IndexKey) -> Result<Option<StoredBlob>> {
        let versions = self.versions(key).await?;
        let Some((_, snapshot, name)) = versions.into_iter().max_by_key(|(seq, _, _)| *seq) else {
            return Ok(None);
        };
        let path = self.dir_for(key).join(name);
        let bytes = tokio::fs::read(&path).await?;
        Ok(Some(StoredBlob {
            location: path.to_string_lossy().into_owned(),
            snapshot,
            bytes,
        }))
    }

    async fn get(&self, key: &IndexKey, snapshot: i64) -> Result<Option<StoredBlob>> {
        // The most recently written blob for this snapshot.
        let versions = self.versions(key).await?;
        let Some((_, _, name)) = versions
            .into_iter()
            .filter(|(_, snap, _)| *snap == snapshot)
            .max_by_key(|(seq, _, _)| *seq)
        else {
            return Ok(None);
        };
        let path = self.dir_for(key).join(name);
        let bytes = tokio::fs::read(&path).await?;
        Ok(Some(StoredBlob {
            location: path.to_string_lossy().into_owned(),
            snapshot,
            bytes,
        }))
    }

    async fn snapshots(&self, key: &IndexKey) -> Result<Vec<i64>> {
        let mut out: Vec<i64> = self
            .versions(key)
            .await?
            .into_iter()
            .map(|(_, snap, _)| snap)
            .collect();
        out.sort_unstable();
        out.dedup();
        Ok(out)
    }
}

/// Keeps `[A-Za-z0-9._-]`, replaces every other byte with `_`. Enough to turn a
/// logical target/field into a safe path component.
fn sanitize(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
                c
            } else {
                '_'
            }
        })
        .collect()
}
