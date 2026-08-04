//! The cache-managed shadow store for Verglas-derived Puffin artifacts (#95).
//!
//! Issue #95's one placement, one rule: "everything Verglas produces
//! autonomously lives in the Verglas-controlled shadow store; Verglas never
//! writes to customer tables." This is that store, owned by the cache cluster
//! and resident on the same NVMe as the block cache (under `cache.dir`). It is
//! Verglas-owned derived state only — its single sink is a local directory
//! rooted under the cache dir, so no code path here can address a customer
//! bucket or table. The vector index is its first tenant; graph adjacency
//! indexes and the heat/prewarm/pruning rollups of #95 are future tenants, so
//! the key space carries an [`ArtifactKind`] even though only
//! [`ArtifactKind::VectorIndex`] is exercised today.
//!
//! # Key space
//! An artifact is addressed by `(kind, target, id)` plus the reflected source
//! snapshot as its version. `target` is a stable logical id of the source table
//! or graph (e.g. `tbl:default.docs` or `graph:kg`); `id` is the sub-address
//! within that target (the embedding column, for a vector index). The layout on
//! disk is self-describing:
//! `<root>/<kind>/<target>/<id>/<seq:020>-<snapshot>.puffin`, where `root` is
//! `<cache.dir>/shadow`, `seq` is a store-global monotonic write sequence, and
//! `snapshot` is the reflected source snapshot.
//!
//! # Latest by write sequence, not snapshot
//! "Most recent artifact" is the highest `seq`, deliberately NOT the highest
//! snapshot id: Iceberg snapshot ids are random i64 and are not monotonic, so
//! only write recency orders versions. The sequence is store-global (across all
//! keys) so it also totally orders the budget's eviction queue.
//!
//! # Budget (a hard ceiling)
//! The store holds a byte ceiling for all shadow artifacts, mirroring the
//! cache's `capacity_bytes` style. It is a hard ceiling per the standing
//! invariant: a [`put`](ShadowStore::put) that would exceed the ceiling first
//! evicts the least-recently-written artifacts (smallest `seq`) until the new
//! one fits; a single artifact larger than the whole ceiling is refused
//! ([`ShadowError::TooLarge`]) rather than admitted unbounded.
//!
//! # Durability
//! Every `put` writes its blob with a temp-then-rename so a crash mid-write
//! never leaves a half-written blob, and the write lands before the entry is
//! made visible. On [`open`](ShadowStore::open) the store rebuilds its in-memory
//! index (per-key versions, total bytes, next sequence) by scanning the on-disk
//! tree, so it survives restart.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use thiserror::Error;

/// What kind of derived artifact a blob is. The key space carries this so the
/// store can hold every #95 artifact type; only [`ArtifactKind::VectorIndex`] is
/// exercised today, the rest are declared so their slugs are reserved and the
/// layout is fixed before their tenants land.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ArtifactKind {
    /// A Vamana vector index blob (#90/#91) — the first real tenant.
    VectorIndex,
    /// A graph adjacency index blob (future tenant).
    GraphAdjacency,
    /// A heat-ledger checkpoint (`verglas-heat-v1`, #51; future tenant).
    Heat,
    /// A prewarm manifest (`verglas-prewarm-v1`, #56; future tenant).
    Prewarm,
    /// A pruning bloom/zone-map rollup (`verglas-bloom-rollup-v1`; future
    /// tenant).
    BloomRollup,
}

impl ArtifactKind {
    /// The stable path slug for this kind. Reserved for every kind so a future
    /// tenant cannot silently collide with the vector index's tree.
    fn slug(self) -> &'static str {
        match self {
            ArtifactKind::VectorIndex => "vector-index",
            ArtifactKind::GraphAdjacency => "graph-adjacency",
            ArtifactKind::Heat => "heat",
            ArtifactKind::Prewarm => "prewarm",
            ArtifactKind::BloomRollup => "bloom-rollup",
        }
    }
}

/// Addresses an artifact within the shadow store, minus the snapshot version.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ArtifactKey {
    /// The artifact kind (fixes the top-level slug).
    pub kind: ArtifactKind,
    /// A stable logical id for the indexed table or graph, e.g. `tbl:ns.table`.
    pub target: String,
    /// The sub-address within the target (the embedding field, for a vector
    /// index).
    pub id: String,
}

impl ArtifactKey {
    /// A key for a kind over a target's sub-id.
    pub fn new(kind: ArtifactKind, target: impl Into<String>, id: impl Into<String>) -> Self {
        ArtifactKey {
            kind,
            target: target.into(),
            id: id.into(),
        }
    }

    /// The filesystem-safe path components `(kind, target, id)` for this key.
    fn slugs(&self) -> DirKey {
        DirKey(
            self.kind.slug().to_owned(),
            sanitize(&self.target),
            sanitize(&self.id),
        )
    }
}

/// The sanitized, filesystem-level rendering of an [`ArtifactKey`]: the directory
/// tuple a blob actually lives under. The in-memory index is keyed by this
/// (exactly what a directory scan reconstructs on `open`), so two keys that
/// sanitize alike share one tree — the same lossy-but-deterministic property the
/// on-disk layout already has.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct DirKey(String, String, String);

impl DirKey {
    /// The directory this key's blobs live in, under `root`.
    fn dir(&self, root: &Path) -> PathBuf {
        root.join(&self.0).join(&self.1).join(&self.2)
    }
}

/// A blob resolved from the store: where it lives, which snapshot it reflects,
/// and its bytes.
#[derive(Debug, Clone)]
pub struct ShadowBlob {
    /// The store location (the on-disk path).
    pub location: String,
    /// The source snapshot this blob reflects.
    pub snapshot: i64,
    /// The Puffin file bytes.
    pub bytes: Vec<u8>,
}

/// One stored version's metadata: its write sequence, reflected snapshot, byte
/// size, and filename. Bytes stay on disk — only metadata is held in memory.
#[derive(Debug, Clone)]
struct Entry {
    seq: u64,
    snapshot: i64,
    size: u64,
    filename: String,
}

/// The mutable index guarded by one lock: per-key versions, the running total of
/// on-disk bytes (for the budget), and the next write sequence.
#[derive(Debug, Default)]
struct State {
    entries: HashMap<DirKey, Vec<Entry>>,
    total_bytes: u64,
    next_seq: u64,
}

/// Everything that can go wrong in the shadow store.
#[derive(Debug, Error)]
pub enum ShadowError {
    /// A single artifact is larger than the whole budget ceiling; admitting it
    /// would blow the ceiling no matter what is evicted, so it is refused.
    #[error("artifact of {bytes} bytes exceeds the shadow-store budget of {budget} bytes")]
    TooLarge {
        /// The artifact's size.
        bytes: u64,
        /// The configured budget ceiling.
        budget: u64,
    },
    /// Local IO to the shadow store failed.
    #[error("shadow store io: {0}")]
    Io(#[from] std::io::Error),
    /// The index lock was poisoned by a panic in another holder.
    #[error("shadow store lock poisoned")]
    Poisoned,
}

/// The result type for shadow-store operations.
pub type Result<T> = std::result::Result<T, ShadowError>;

/// A cache-managed, NVMe-resident store for Verglas-derived Puffin artifacts.
/// Cluster-local, budget-bounded, and durable across restart. Its only sink is
/// the directory `root` under the cache dir — it holds no bucket, catalog, or
/// origin handle, so no operation can reach a customer table or bucket.
#[derive(Debug)]
pub struct ShadowStore {
    root: PathBuf,
    budget_bytes: u64,
    state: Mutex<State>,
}

impl ShadowStore {
    /// Roots the store at `<cache_dir>/shadow` with the given byte budget,
    /// rebuilding its in-memory index from any blobs already on disk.
    pub fn under_cache_dir(cache_dir: impl AsRef<Path>, budget_bytes: u64) -> Result<Self> {
        Self::open(cache_dir.as_ref().join("shadow"), budget_bytes)
    }

    /// Opens (or creates) a store rooted at `root` with the given byte budget.
    /// Scans the on-disk tree to restore per-key versions, the total byte usage,
    /// and the next write sequence, so a reopened store continues where it left
    /// off.
    pub fn open(root: impl AsRef<Path>, budget_bytes: u64) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        let state = Self::scan(&root)?;
        Ok(ShadowStore {
            root,
            budget_bytes,
            state: Mutex::new(state),
        })
    }

    /// Rebuilds the index by walking `<root>/<kind>/<target>/<id>/*.puffin`.
    fn scan(root: &Path) -> Result<State> {
        let mut state = State::default();
        let mut max_seq: Option<u64> = None;
        // Three fixed levels: kind / target / id. A missing root is an empty
        // store, not an error.
        let read = |p: &Path| -> Result<Vec<PathBuf>> {
            let mut out = Vec::new();
            match std::fs::read_dir(p) {
                Ok(rd) => {
                    for e in rd {
                        out.push(e?.path());
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
            Ok(out)
        };
        for kind_dir in read(root)? {
            let Some(kind) = leaf(&kind_dir) else {
                continue;
            };
            for target_dir in read(&kind_dir)? {
                let Some(target) = leaf(&target_dir) else {
                    continue;
                };
                for id_dir in read(&target_dir)? {
                    let Some(id) = leaf(&id_dir) else {
                        continue;
                    };
                    let dk = DirKey(kind.clone(), target.clone(), id.clone());
                    for blob in read(&id_dir)? {
                        let Some(name) = leaf(&blob) else {
                            continue;
                        };
                        let Some((seq, snapshot)) = parse_name(&name) else {
                            continue;
                        };
                        let size = std::fs::metadata(&blob)?.len();
                        state.total_bytes += size;
                        max_seq = Some(max_seq.map_or(seq, |m| m.max(seq)));
                        state.entries.entry(dk.clone()).or_default().push(Entry {
                            seq,
                            snapshot,
                            size,
                            filename: name,
                        });
                    }
                }
            }
        }
        state.next_seq = max_seq.map_or(0, |m| m + 1);
        Ok(state)
    }

    /// Persists `bytes` for `(key, snapshot)`, returning the stored blob.
    /// Overwrites any existing blob at the same `(key, snapshot)`. Enforces the
    /// budget: evicts the least-recently-written artifacts until the new one
    /// fits, or refuses it with [`ShadowError::TooLarge`] if it alone exceeds the
    /// ceiling. The blob is written to disk before it becomes visible in the
    /// index (durability before visibility).
    pub async fn put(
        &self,
        key: &ArtifactKey,
        snapshot: i64,
        bytes: Vec<u8>,
    ) -> Result<ShadowBlob> {
        let len = bytes.len() as u64;
        if len > self.budget_bytes {
            return Err(ShadowError::TooLarge {
                bytes: len,
                budget: self.budget_bytes,
            });
        }
        let dk = key.slugs();
        let dir = dk.dir(&self.root);
        tokio::fs::create_dir_all(&dir).await?;

        // Reserve a monotonic write sequence, then write the blob to disk before
        // it is visible in the index.
        let seq = {
            let mut st = self.lock()?;
            let s = st.next_seq;
            st.next_seq += 1;
            s
        };
        let filename = format!("{seq:020}-{snapshot}.puffin");
        let path = dir.join(&filename);
        let tmp = dir.join(format!(".{filename}.tmp"));
        tokio::fs::write(&tmp, &bytes).await?;
        tokio::fs::rename(&tmp, &path).await?;

        // Commit into the index: replace any same-snapshot version, add the new
        // one, and plan budget evictions. No await under the lock.
        let (stale, evict) = {
            let mut st = self.lock()?;
            let mut stale = None;
            if let Some(versions) = st.entries.get_mut(&dk)
                && let Some(pos) = versions.iter().position(|e| e.snapshot == snapshot)
            {
                let old = versions.remove(pos);
                st.total_bytes -= old.size;
                stale = Some(dir.join(old.filename));
            }
            st.entries.entry(dk.clone()).or_default().push(Entry {
                seq,
                snapshot,
                size: len,
                filename: filename.clone(),
            });
            st.total_bytes += len;

            let mut evict = Vec::new();
            while st.total_bytes > self.budget_bytes {
                // The globally least-recently-written entry. It cannot be the one
                // just added (that has the highest seq), and since `len` fits the
                // budget, evicting older entries always makes room.
                let victim = st
                    .entries
                    .iter()
                    .filter_map(|(k, v)| {
                        v.iter()
                            .min_by_key(|e| e.seq)
                            .map(|e| (k.clone(), e.seq, e.size, e.filename.clone()))
                    })
                    .min_by_key(|(_, s, _, _)| *s);
                let Some((vk, vseq, vsize, vname)) = victim else {
                    break;
                };
                if let Some(versions) = st.entries.get_mut(&vk) {
                    versions.retain(|e| e.seq != vseq);
                    if versions.is_empty() {
                        st.entries.remove(&vk);
                    }
                }
                st.total_bytes -= vsize;
                evict.push(vk.dir(&self.root).join(vname));
            }
            (stale, evict)
        };

        // Delete replaced/evicted files off the lock. A missing file is fine
        // (already gone); other IO errors surface.
        if let Some(p) = stale {
            remove_if_present(&p).await?;
        }
        for p in evict {
            remove_if_present(&p).await?;
        }

        Ok(ShadowBlob {
            location: path.to_string_lossy().into_owned(),
            snapshot,
            bytes,
        })
    }

    /// The most recently WRITTEN blob for `key`, or `None`. Ordered by write
    /// sequence, not snapshot id.
    pub async fn latest(&self, key: &ArtifactKey) -> Result<Option<ShadowBlob>> {
        let dk = key.slugs();
        let found = {
            let st = self.lock()?;
            st.entries
                .get(&dk)
                .and_then(|v| v.iter().max_by_key(|e| e.seq))
                .map(|e| (e.snapshot, e.filename.clone()))
        };
        self.read_blob(&dk, found).await
    }

    /// The blob for an exact `(key, snapshot)`, or `None`. If the snapshot was
    /// written more than once, the most recently written wins.
    pub async fn get(&self, key: &ArtifactKey, snapshot: i64) -> Result<Option<ShadowBlob>> {
        let dk = key.slugs();
        let found = {
            let st = self.lock()?;
            st.entries.get(&dk).and_then(|v| {
                v.iter()
                    .filter(|e| e.snapshot == snapshot)
                    .max_by_key(|e| e.seq)
                    .map(|e| (e.snapshot, e.filename.clone()))
            })
        };
        self.read_blob(&dk, found).await
    }

    /// The snapshots for which a blob exists under `key`, ascending and deduped.
    pub async fn snapshots(&self, key: &ArtifactKey) -> Result<Vec<i64>> {
        let dk = key.slugs();
        let st = self.lock()?;
        let mut out: Vec<i64> = st
            .entries
            .get(&dk)
            .map(|v| v.iter().map(|e| e.snapshot).collect())
            .unwrap_or_default();
        out.sort_unstable();
        out.dedup();
        Ok(out)
    }

    /// The current on-disk byte usage across all artifacts (for tests and
    /// diagnostics). Always at or below the budget.
    pub fn used_bytes(&self) -> Result<u64> {
        Ok(self.lock()?.total_bytes)
    }

    /// The configured hard ceiling.
    pub fn budget_bytes(&self) -> u64 {
        self.budget_bytes
    }

    /// Reads the bytes for a resolved `(snapshot, filename)`, if any.
    async fn read_blob(
        &self,
        dk: &DirKey,
        found: Option<(i64, String)>,
    ) -> Result<Option<ShadowBlob>> {
        let Some((snapshot, filename)) = found else {
            return Ok(None);
        };
        let path = dk.dir(&self.root).join(filename);
        let bytes = tokio::fs::read(&path).await?;
        Ok(Some(ShadowBlob {
            location: path.to_string_lossy().into_owned(),
            snapshot,
            bytes,
        }))
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, State>> {
        self.state.lock().map_err(|_| ShadowError::Poisoned)
    }
}

/// Removes a file, treating "already absent" as success.
async fn remove_if_present(path: &Path) -> Result<()> {
    match tokio::fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e.into()),
    }
}

/// The final path component as an owned string, if valid UTF-8.
fn leaf(p: &Path) -> Option<String> {
    p.file_name().map(|n| n.to_string_lossy().into_owned())
}

/// Parses `<seq:020>-<snapshot>.puffin` into `(seq, snapshot)`.
fn parse_name(name: &str) -> Option<(u64, i64)> {
    let stem = name.strip_suffix(".puffin")?;
    let (seq_s, snap_s) = stem.split_once('-')?;
    let seq = seq_s.parse::<u64>().ok()?;
    let snapshot = snap_s.parse::<i64>().ok()?;
    Some((seq, snapshot))
}

/// Keeps `[A-Za-z0-9._-]`, replaces every other byte with `_` — enough to turn a
/// logical target/id into a safe path component.
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
