//! Internal WAL segment archival with verified-object checkpoint ordering.
//!
//! The archiver is deliberately outside Neon wire operations. It can release
//! local bytes only after a verified immutable object is named by an applied
//! timeline checkpoint.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use sha2::{Digest, Sha256};
use verglas_consensus::{ConsensusGroup, GroupError, WalArchiveSegment};

/// Immutable object identity computed from the exact committed segment bytes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArchiveObject {
    /// Object key containing the timeline name, LSN range, and content hash.
    pub key: String,
    /// SHA-256 identity of the exact uploaded segment bytes.
    pub hash: [u8; 32],
    /// Logical segment byte count.
    pub length: u64,
}

impl ArchiveObject {
    /// Rebuilds an object identity only from the committed archive checkpoint record.
    pub fn from_segment(segment: &WalArchiveSegment) -> Result<Self, ArchiveError> {
        let object = Self {
            key: segment.key().to_owned(),
            hash: segment.hash(),
            length: segment.length(),
        };
        if object.length == 0 {
            return Err(ArchiveError::InvalidRange);
        }
        Ok(object)
    }

    /// Rejects a read whose visible bytes differ from the committed immutable identity.
    pub fn verify_bytes(&self, bytes: &[u8]) -> Result<(), ArchiveError> {
        if bytes.len() as u64 != self.length || <[u8; 32]>::from(Sha256::digest(bytes)) != self.hash
        {
            return Err(ArchiveError::ObjectStore(
                "archive object does not match its committed identity".to_owned(),
            ));
        }
        Ok(())
    }
}

/// A checkpoint-gated boundary through which local WAL representations may be released.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SegmentRelease {
    through_lsn: u64,
}

impl SegmentRelease {
    /// Builds a release token only for an already-applied checkpoint boundary.
    pub fn checkpointed(through_lsn: u64) -> Self {
        Self { through_lsn }
    }

    /// Returns the exclusive LSN that local persistent representations may release through.
    pub fn through_lsn(self) -> u64 {
        self.through_lsn
    }
}

/// Object storage capable of making and then validating immutable segment objects.
#[async_trait]
pub trait ImmutableSegmentStore: Send + Sync {
    /// Uploads bytes under the supplied immutable identity without overwriting another identity.
    async fn upload(&self, object: &ArchiveObject, bytes: Bytes) -> Result<(), ArchiveError>;

    /// Verifies that the visible object still matches its requested immutable identity.
    async fn verify(&self, object: &ArchiveObject) -> Result<(), ArchiveError>;

    /// Reads the complete immutable object named by a committed checkpoint identity.
    async fn read(&self, object: &ArchiveObject) -> Result<Bytes, ArchiveError>;
}

/// Reads one checkpoint-derived prefix interval, rejecting every identity or coverage mismatch.
pub async fn read_archived_wal_prefix(
    store: &dyn ImmutableSegmentStore,
    segments: &[WalArchiveSegment],
    from: u64,
    to: u64,
) -> Result<Bytes, ArchiveError> {
    if from >= to {
        return Err(ArchiveError::InvalidRange);
    }
    let mut cursor = from;
    let mut output = Vec::with_capacity((to - from) as usize);
    for segment in segments {
        if segment.end_lsn() <= cursor || segment.start_lsn() >= to {
            continue;
        }
        if segment.start_lsn() > cursor || segment.end_lsn() <= cursor {
            return Err(ArchiveError::ArchivedCoverage);
        }
        let object = ArchiveObject::from_segment(segment)?;
        let bytes = store.read(&object).await?;
        object.verify_bytes(&bytes)?;
        let take_to = to.min(segment.end_lsn());
        let start = (cursor - segment.start_lsn()) as usize;
        let end = (take_to - segment.start_lsn()) as usize;
        output.extend_from_slice(
            bytes
                .get(start..end)
                .ok_or(ArchiveError::ArchivedCoverage)?,
        );
        cursor = take_to;
        if cursor == to {
            return Ok(Bytes::from(output));
        }
    }
    Err(ArchiveError::ArchivedCoverage)
}

/// Timeline operations the internal archiver needs, excluding every Neon-facing control.
#[async_trait]
pub trait ArchiveTimeline: Send + Sync {
    /// Reads an exact committed WAL interval after the supplied applied-index fence.
    async fn read_committed_wal(
        &self,
        from: u64,
        to: u64,
        minimum_index: u64,
    ) -> Result<Bytes, ArchiveError>;

    /// Applies the archive checkpoint naming the already-verified immutable object.
    async fn commit_archive_checkpoint(
        &self,
        request: u128,
        end_lsn: u64,
        object: &ArchiveObject,
    ) -> Result<(), ArchiveError>;

    /// Returns a release token only when the committed checkpoint covers `end_lsn`.
    async fn release_through(&self, end_lsn: u64) -> Result<SegmentRelease, ArchiveError>;
}

/// Consensus-backed implementation used by the archive worker for one timeline.
pub struct ConsensusArchiveTimeline {
    group: Arc<ConsensusGroup>,
    holders: Vec<u64>,
}

impl ConsensusArchiveTimeline {
    /// Binds a background archive worker to one group and its committed complete-entry holders.
    pub fn new(group: Arc<ConsensusGroup>, holders: Vec<u64>) -> Result<Self, ArchiveError> {
        if holders.is_empty() {
            return Err(ArchiveError::InvalidConfiguration);
        }
        Ok(Self { group, holders })
    }
}

#[async_trait]
impl ArchiveTimeline for ConsensusArchiveTimeline {
    async fn read_committed_wal(
        &self,
        from: u64,
        to: u64,
        minimum_index: u64,
    ) -> Result<Bytes, ArchiveError> {
        self.group
            .read_wal(from, to, minimum_index)
            .await
            .map_err(ArchiveError::Consensus)
    }

    async fn commit_archive_checkpoint(
        &self,
        request: u128,
        end_lsn: u64,
        object: &ArchiveObject,
    ) -> Result<(), ArchiveError> {
        self.group
            .archive_checkpoint(
                verglas_consensus::RequestId::from_u128(request),
                end_lsn,
                &object.key,
                &self.holders,
            )
            .await
            .map(|_| ())
            .map_err(ArchiveError::Consensus)
    }

    async fn release_through(&self, end_lsn: u64) -> Result<SegmentRelease, ArchiveError> {
        self.group
            .release_wal_through(end_lsn)
            .await
            .map(|_| SegmentRelease::checkpointed(end_lsn))
            .map_err(ArchiveError::Consensus)
    }
}

/// One internal worker that archives exact, complete committed WAL segments.
pub struct SegmentArchiver {
    timeline: Arc<dyn ArchiveTimeline>,
    store: Arc<dyn ImmutableSegmentStore>,
    prefix: String,
}

impl SegmentArchiver {
    /// Creates an archiver whose object keys stay under one non-empty timeline prefix.
    pub fn new(
        timeline: Arc<dyn ArchiveTimeline>,
        store: Arc<dyn ImmutableSegmentStore>,
        prefix: impl Into<String>,
    ) -> Result<Self, ArchiveError> {
        let prefix = prefix.into();
        if prefix.is_empty() {
            return Err(ArchiveError::InvalidConfiguration);
        }
        Ok(Self {
            timeline,
            store,
            prefix,
        })
    }

    /// Uploads, verifies, checkpoints, then exposes release for one committed complete segment.
    pub async fn archive(
        &self,
        request: u128,
        start_lsn: u64,
        end_lsn: u64,
        minimum_index: u64,
    ) -> Result<SegmentRelease, ArchiveError> {
        if start_lsn >= end_lsn {
            return Err(ArchiveError::InvalidRange);
        }
        let bytes = self
            .timeline
            .read_committed_wal(start_lsn, end_lsn, minimum_index)
            .await?;
        if bytes.len() as u64 != end_lsn - start_lsn {
            return Err(ArchiveError::InvalidRange);
        }
        let object = self.object(start_lsn, end_lsn, &bytes);
        self.store.upload(&object, bytes).await?;
        self.store.verify(&object).await?;
        self.timeline
            .commit_archive_checkpoint(request, end_lsn, &object)
            .await?;
        self.timeline.release_through(end_lsn).await
    }

    /// Archives a final partial tail after an explicit lifecycle flush request.
    ///
    /// The caller supplies the committed tail observed behind its fence. Empty
    /// ranges are rejected so a lifecycle checkpoint cannot manufacture progress.
    pub async fn archive_tail(
        &self,
        request: u128,
        start_lsn: u64,
        committed_end: u64,
        minimum_index: u64,
    ) -> Result<SegmentRelease, ArchiveError> {
        self.archive(request, start_lsn, committed_end, minimum_index)
            .await
    }

    /// Forms a content-addressed immutable object key for one exact LSN interval.
    fn object(&self, start_lsn: u64, end_lsn: u64, bytes: &[u8]) -> ArchiveObject {
        let hash: [u8; 32] = Sha256::digest(bytes).into();
        ArchiveObject {
            key: format!(
                "{}/{start_lsn:016x}-{end_lsn:016x}-{}",
                self.prefix,
                hex(&hash)
            ),
            hash,
            length: bytes.len() as u64,
        }
    }
}

/// Encodes a hash without adding a second object-key dependency.
fn hex(bytes: &[u8; 32]) -> String {
    let mut output = String::with_capacity(64);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(&mut output, "{byte:02x}");
    }
    output
}

/// Archive worker failures that must leave local WAL fully retained.
#[derive(Debug, thiserror::Error)]
pub enum ArchiveError {
    /// The segment range or worker configuration is not meaningful.
    #[error("invalid archive configuration or WAL range")]
    InvalidConfiguration,
    /// The requested WAL range is empty or reversed.
    #[error("archive WAL range is empty or reversed")]
    InvalidRange,
    /// Archive checkpoints fail to cover the exact requested WAL interval.
    #[error("archive checkpoint objects do not cover the requested WAL interval")]
    ArchivedCoverage,
    /// Object storage failed to publish or verify the immutable object.
    #[error("archive object store failed: {0}")]
    ObjectStore(String),
    /// The consensus group refused the read, checkpoint, or release boundary.
    #[error(transparent)]
    Consensus(GroupError),
}
