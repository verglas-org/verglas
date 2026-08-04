//! The pinned append-log API: the [`AppendLog`] trait and the types that cross
//! the substrate<->pageserver boundary. The prose contract is in the crate root
//! doc; this module is its executable form.

use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// A position in the logical WAL byte stream: a `u64` byte offset, the same
/// shape as a Postgres LSN. Dense and monotonic; the stream only grows at the
/// tail.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
pub struct Lsn(pub u64);

impl Lsn {
    /// The LSN `bytes` further along the stream.
    pub fn advance(self, bytes: u64) -> Lsn {
        Lsn(self.0 + bytes)
    }
}

impl std::fmt::Display for Lsn {
    /// Hex, the conventional Postgres LSN rendering (`0/0` style is not needed
    /// here — a bare hex offset is unambiguous for one stream).
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:X}", self.0)
    }
}

/// A writer fencing token. A new writer installs a strictly greater epoch to
/// fence the previous one; appends must present the current epoch.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
pub struct Epoch(pub u64);

impl Epoch {
    /// The next epoch, for a writer taking over.
    pub fn next(self) -> Epoch {
        Epoch(self.0 + 1)
    }
}

impl std::fmt::Display for Epoch {
    /// The bare epoch number.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// The erasure geometry and ack quorum of an append log. This is the entire
/// tuning surface: `k` data + `m` parity fragments, ack after `w` distinct-node
/// fsyncs. A one-node deployment overrides this to the degenerate `(1, 0, 1)`
/// internally (see the crate contract, §7).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppendGeometry {
    /// Reed-Solomon data fragments; any `k` reconstruct.
    pub k: usize,
    /// Reed-Solomon parity fragments.
    pub m: usize,
    /// Fragments that must be fsynced on distinct live nodes to ack. `k <= w <= k + m`.
    pub w: usize,
}

impl AppendGeometry {
    /// A geometry, rejecting the combinations the ack rule cannot honour:
    /// `k >= 1`, and `k <= w <= k + m` (an ack must be reconstructable on its
    /// own, and cannot demand more fragments than exist).
    pub fn new(k: usize, m: usize, w: usize) -> Result<Self, AppendError> {
        if k == 0 || w < k || w > k + m {
            return Err(AppendError::Geometry { k, m, w });
        }
        Ok(Self { k, m, w })
    }

    /// Total fragments `n = k + m`.
    pub fn total(&self) -> usize {
        self.k + self.m
    }
}

/// The half-open byte range a successful append was assigned. `start` is the
/// old tail, `end` the new one; `end - start` equals the appended byte count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Appended {
    /// First LSN of the appended bytes (the pre-append tail).
    pub start: Lsn,
    /// One past the last appended byte (the new tail).
    pub end: Lsn,
}

/// Why an append-log operation could not complete. Every variant is a clean
/// failure that leaves the log consistent — never a partial or sub-quorum ack.
#[derive(Debug, thiserror::Error)]
pub enum AppendError {
    /// The presented epoch is not the current one: the writer has been fenced by
    /// a later one (or is ahead of an install). The append is rejected and the
    /// tail does not move.
    #[error("append fenced: presented epoch {presented} but current is {current}")]
    Fenced {
        /// The epoch the log currently accepts.
        current: Epoch,
        /// The epoch the rejected append presented.
        presented: Epoch,
    },
    /// Fewer than `w` fragments could be placed on distinct live nodes, so the
    /// append is not quorum-durable. The tail does not move; the WAL is not lost,
    /// only un-acked, and the writer retries. Never a sub-quorum ack.
    #[error("append not durable: placed {placed} of the required w={needed} fragments")]
    QuorumUnavailable {
        /// Fragments the ack rule requires (`w`).
        needed: usize,
        /// Fragments that actually reached distinct nodes.
        placed: usize,
    },
    /// A read or truncate named a range the log cannot serve: past the tail,
    /// or (for read) partly below the truncation point.
    #[error("range [{from}, {to}) is out of the log's live range")]
    OutOfRange {
        /// Requested start.
        from: Lsn,
        /// Requested end.
        to: Lsn,
    },
    /// A truncate asked to drop bytes not yet flushed to S3 — refused so the
    /// buffer never forgets bytes that are not durable at the origin.
    #[error("cannot truncate to {up_to}: only {flushed_through} is flushed to S3")]
    TruncateBeyondFlush {
        /// Where the caller asked to truncate to.
        up_to: Lsn,
        /// How far S3 durability currently reaches.
        flushed_through: Lsn,
    },
    /// A fence asked for an epoch that is not strictly greater than the current
    /// one — a stale or replayed handoff, refused.
    #[error("fence must advance: presented {presented}, current {current}")]
    StaleFence {
        /// The current epoch.
        current: Epoch,
        /// The non-advancing epoch that was presented.
        presented: Epoch,
    },
    /// Bytes could not be reconstructed from the surviving fragments (more than
    /// the codec's erasure budget lost or corrupt). Fails loudly, never returns
    /// reconstructed garbage.
    #[error("reassembly failed: {0}")]
    Reassembly(String),
    /// The durable manifest could not be read or fsynced.
    #[error("manifest: {0}")]
    Manifest(String),
    /// The fragment transport failed.
    #[error(transparent)]
    Transport(#[from] crate::TransportError),
    /// The erasure codec rejected an encode or a geometry.
    #[error("codec: {0}")]
    Codec(String),
    /// The S3 origin failed on flush or on a flushed-range read-back.
    #[error("origin: {0}")]
    Origin(String),
    /// An invalid geometry: `k == 0`, or `w` outside `k..=k+m`.
    #[error("invalid geometry k={k} m={m} w={w}: need k>=1 and k<=w<=k+m")]
    Geometry {
        /// Data fragments.
        k: usize,
        /// Parity fragments.
        m: usize,
        /// Ack quorum.
        w: usize,
    },
}

/// The durable append log: the substrate side of the pg-engine boundary.
///
/// The prose contract (append semantics, ordering, tail read-back, flush
/// lifecycle, truncation/GC, epoch fencing, and the failure model) is the crate
/// root doc. Implementations honour it exactly. [`crate::EcAppendLog`] is the
/// erasure-coded quorum implementation; the pageserver fork holds this as
/// `Arc<dyn AppendLog>`.
#[async_trait]
pub trait AppendLog: Send + Sync {
    /// Synchronously append `records` under `epoch`, returning the assigned
    /// range once the bytes are quorum-durable (§1). Rejects a mismatched epoch
    /// ([`AppendError::Fenced`]) and a shortfall below `w`
    /// ([`AppendError::QuorumUnavailable`]) without moving the tail.
    async fn append(&self, epoch: Epoch, records: Bytes) -> Result<Appended, AppendError>;

    /// Read the exact bytes of `[from, to)` (§3), served from the EC tail or
    /// from S3 for already-flushed ranges. Fails on an out-of-range or
    /// truncated request rather than fabricating bytes.
    async fn read(&self, from: Lsn, to: Lsn) -> Result<Bytes, AppendError>;

    /// Seal and flush every un-flushed segment up to the tail to S3, then drop
    /// their local fragments and advance the flush watermark (§4). Returns the
    /// new [`AppendLog::flushed_through`].
    async fn flush(&self) -> Result<Lsn, AppendError>;

    /// Drop everything strictly below `up_to` (§5). Refused above the flush
    /// watermark ([`AppendError::TruncateBeyondFlush`]).
    async fn truncate(&self, up_to: Lsn) -> Result<(), AppendError>;

    /// Install `new_epoch` as the writer fencing token (§6). It must be strictly
    /// greater than the current epoch, else [`AppendError::StaleFence`].
    async fn fence(&self, new_epoch: Epoch) -> Result<(), AppendError>;

    /// The LSN one past the last acked byte — the start the next append receives.
    fn tail(&self) -> Lsn;

    /// The LSN through which the log is durable in S3 and its local fragments
    /// dropped. Everything below is Verglas-independent.
    fn flushed_through(&self) -> Lsn;

    /// The current writer epoch.
    fn epoch(&self) -> Epoch;
}
