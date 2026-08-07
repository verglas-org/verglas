//! The durable append-log manifest: the fsynced record that makes an acked but
//! not-yet-flushed append real, and that a restart rebuilds the tail from.
//!
//! One JSON file holds the whole log's state — the writer epoch, the ordered
//! segments and their appends (with the exact fragment placements), and the
//! flush and truncation watermarks. It is rewritten and fsynced before an append
//! is acked, so a crash after ack replays it: the surviving fragments named in
//! it reconstruct the tail, and the flushed-segment records point recovery at
//! S3 for everything below the flush watermark.
//!
//! Rewriting the whole file per append is O(segments-in-flight); appends are
//! serialized and flush drops the flushed segments' per-append placements, so
//! the file stays small. The
//! extension point for a higher append rate is a per-append journal file plus a
//! small watermark manifest (the object write-back tier's shape) — noted, not
//! built, per the prototype rules.

use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::contract::{AppendError, Epoch, Lsn};

/// Where one fragment of one append lives.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Placement {
    /// Fragment index (`0..k` data, `k..k+m` parity).
    pub index: usize,
    /// The node id holding this fragment.
    pub node: String,
}

/// One acked append: the bytes it occupies and how to rebuild them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppendEntry {
    /// Globally unique fragment object id for this timeline append.
    pub object_id: String,
    /// Per-append sequence within its segment, part of the fragment object id.
    pub seq: u64,
    /// First LSN of this append.
    pub start: Lsn,
    /// One past its last byte.
    pub end: Lsn,
    /// Reed-Solomon data fragments used.
    pub k: usize,
    /// Reed-Solomon parity fragments used.
    pub m: usize,
    /// Per-fragment stripe chunk the codec chose.
    pub chunk: usize,
    /// True object length before stripe padding.
    pub object_len: u64,
    /// Where the fragments landed (at least `w` at ack, more with stragglers).
    pub placements: Vec<Placement>,
}

impl AppendEntry {
    /// The fragment object id for this append within `segment_id`: stable, and
    /// unique across the log so fragments never collide.
    pub fn object_id(&self) -> &str {
        &self.object_id
    }
}

/// The lifecycle state of a segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SegmentState {
    /// Still accepting appends into the EC buffer.
    Open,
    /// Flushed to S3 and its local fragments dropped.
    Flushed,
}

/// A contiguous LSN run of appends.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentEntry {
    /// Monotonic segment id.
    pub id: u64,
    /// First LSN of the segment.
    pub start: Lsn,
    /// One past its last acked byte (grows while `Open`).
    pub end: Lsn,
    /// Lifecycle state.
    pub state: SegmentState,
    /// The S3 key the segment object was written to, once `Flushed`.
    pub s3_key: Option<String>,
    /// The appends in this segment, in LSN order.
    pub appends: Vec<AppendEntry>,
}

/// The whole durable state of one append log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    /// Monotonic state revision replicated through the fragment ring.
    pub revision: u64,
    /// Neon membership generation accepted by this timeline.
    pub generation: u32,
    /// PostgreSQL system identifier learned from the proposer greeting.
    pub system_id: u64,
    /// PostgreSQL server version in `PG_VERSION_NUM` form.
    pub pg_version: u32,
    /// PostgreSQL WAL segment size.
    pub wal_segment_size: u32,
    /// Current writer fencing token.
    pub epoch: Epoch,
    /// Quorum commit watermark learned from walproposer.
    pub commit_lsn: Lsn,
    /// Oldest WAL retained for proposer/pageserver recovery.
    pub truncate_lsn: Lsn,
    /// Oldest checkpoint that the pageserver reports durable in remote storage.
    /// Zero means no pageserver durability feedback has been received yet.
    #[serde(default)]
    pub remote_consistent_lsn: Lsn,
    /// Ordered `(term, first_lsn)` boundaries selected during elections.
    pub term_history: Vec<(u64, Lsn)>,
    /// The stream's base LSN (appends below this were truncated away).
    pub base: Lsn,
    /// One past the last acked byte across the whole log.
    pub tail: Lsn,
    /// LSN through which the log is durable in S3 and local fragments dropped.
    pub flushed_through: Lsn,
    /// Id the next new segment will take.
    pub next_segment_id: u64,
    /// Segments in LSN order.
    pub segments: Vec<SegmentEntry>,
}

impl Manifest {
    /// A fresh, empty log at LSN 0, epoch 0.
    fn empty() -> Self {
        Self {
            revision: 0,
            generation: 0,
            system_id: 0,
            pg_version: 0,
            wal_segment_size: 0,
            epoch: Epoch(0),
            commit_lsn: Lsn(0),
            truncate_lsn: Lsn(0),
            remote_consistent_lsn: Lsn(0),
            term_history: Vec::new(),
            base: Lsn(0),
            tail: Lsn(0),
            flushed_through: Lsn(0),
            next_segment_id: 0,
            segments: Vec::new(),
        }
    }
}

/// The manifest file plus its on-disk location.
pub struct ManifestStore {
    /// The JSON manifest path.
    path: PathBuf,
    /// The directory holding it, fsynced after each rename.
    dir: PathBuf,
}

impl ManifestStore {
    /// Opens (creating the directory if needed) the manifest under `dir`,
    /// loading any existing state so a restart recovers the log; a fresh log
    /// starts empty.
    pub fn open(dir: impl AsRef<Path>) -> Result<(Self, Manifest), AppendError> {
        let dir = dir.as_ref().join("safekeeper");
        fs::create_dir_all(&dir)
            .map_err(|e| AppendError::Manifest(format!("create {}: {e}", dir.display())))?;
        let path = dir.join("manifest.json");
        let manifest = match fs::read(&path) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map_err(|e| AppendError::Manifest(format!("decode {}: {e}", path.display())))?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Manifest::empty(),
            Err(e) => {
                return Err(AppendError::Manifest(format!(
                    "read {}: {e}",
                    path.display()
                )));
            }
        };
        Ok((Self { path, dir }, manifest))
    }

    /// Atomically writes and fsyncs `manifest` (temp file, fsync, rename, fsync
    /// dir) so it is durable before the caller acks. Same discipline as the
    /// object write-back journal.
    pub fn persist(&self, manifest: &Manifest) -> Result<(), AppendError> {
        let bytes = serde_json::to_vec_pretty(manifest)
            .map_err(|e| AppendError::Manifest(format!("encode: {e}")))?;
        let tmp = self
            .path
            .with_extension(format!("{}.tmp", std::process::id()));
        {
            let mut file = File::create(&tmp)
                .map_err(|e| AppendError::Manifest(format!("create {}: {e}", tmp.display())))?;
            file.write_all(&bytes)
                .map_err(|e| AppendError::Manifest(format!("write {}: {e}", tmp.display())))?;
            file.sync_all()
                .map_err(|e| AppendError::Manifest(format!("fsync {}: {e}", tmp.display())))?;
        }
        fs::rename(&tmp, &self.path)
            .map_err(|e| AppendError::Manifest(format!("rename {}: {e}", self.path.display())))?;
        File::open(&self.dir)
            .and_then(|f| f.sync_all())
            .map_err(|e| AppendError::Manifest(format!("fsync dir {}: {e}", self.dir.display())))
    }
}
