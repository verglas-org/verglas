//! Deterministic coded-Raft safety model used by the durable write protocols.
//!
//! The model records immutable entry headers and durable acknowledgements.  It
//! deliberately models the proof obligations around quorum intersection rather
//! than transport or disk implementation details.

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use reed_solomon_simd::{ReedSolomonDecoder, ReedSolomonEncoder};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

mod catalog;
mod durable;
mod group;
mod payload;
mod raft;
mod raft_store;

pub use durable::{DurableGroup, FileReplica};
pub use group::{
    CatalogExport, ConsensusGroup, GroupError, GroupRequest, GroupResponse, WalArchiveState,
};
pub use payload::{
    DistributedPayloadStore, FilePayloadReplica, PayloadError, PayloadRepresentation, PayloadSet,
    PayloadStore, ReconstructRequest, ReleaseRequest, RepairRequest, RepresentationTransport,
    SealRequest, StagedPayload,
};
pub use raft::{
    AppliedOutcome, EntryHeader, EntryMetadata, PayloadCertificate, RaftCommand, RaftResponse,
    VerglasRaftConfig, WalArchiveSegment,
};
pub use raft_store::{PersistentLogStore, PersistentStateMachine};

/// A member identity in a consensus voter configuration.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct NodeId(u64);

impl From<u64> for NodeId {
    /// Makes a node identity from its stable numeric identifier.
    fn from(value: u64) -> Self {
        Self(value)
    }
}

/// A stable name for an independent Raft group.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct GroupId(String);

impl From<&str> for GroupId {
    /// Makes a group identity from its durable routing name.
    fn from(value: &str) -> Self {
        Self(value.to_owned())
    }
}

/// An exact client retry identity.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct RequestId(u128);

impl RequestId {
    /// Makes an opaque retry identity from its wire representation.
    pub fn from_u128(value: u128) -> Self {
        Self(value)
    }
    /// Returns the canonical retry identity bits for durable headers.
    pub fn as_u128(self) -> u128 {
        self.0
    }
}

/// The deterministic state-machine command family.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum CommandKind {
    /// Establishes the immutable initial PostgreSQL WAL position.
    TimelineOpen,
    /// Catalog mutation.
    Catalog,
    /// Tenant-root warehouse registration.
    TenantRoot,
    /// WAL mutation.
    Wal,
    /// Exclusive WAL writer-lease transition.
    WriterLease,
    /// Verified WAL segment archive watermark.
    ArchiveCheckpoint,
    /// Verified object-store checkpoint for a linearizable catalog export.
    CatalogCheckpoint,
    /// Explicitly releases the current WAL writer lease.
    ReleaseWriter,
    /// Publishes one immutable object's durable placement certificate.
    Object,
}

/// A client command before the leader assigns its immutable log header.
#[derive(Clone, Debug)]
pub struct Proposal {
    request: RequestId,
    kind: CommandKind,
    payload: Bytes,
    writer_epoch: Option<u64>,
}

impl Proposal {
    /// Makes a proposal with no writer fencing requirement.
    pub fn new(request: RequestId, kind: CommandKind, payload: Bytes) -> Self {
        Self {
            request,
            kind,
            payload,
            writer_epoch: None,
        }
    }
    /// Binds this proposal to a previously committed writer epoch.
    pub fn with_writer_epoch(mut self, epoch: u64) -> Self {
        self.writer_epoch = Some(epoch);
        self
    }
    /// Returns the immutable identity used to make retries exact.
    pub(crate) fn request(&self) -> RequestId {
        self.request
    }
    /// Returns the command family bound into the durable header.
    pub(crate) fn kind(&self) -> CommandKind {
        self.kind
    }
    /// Returns the command bytes whose hash is bound into the header.
    pub(crate) fn payload(&self) -> &Bytes {
        &self.payload
    }
    /// Returns the optional WAL writer fence bound into the header.
    pub(crate) fn writer_epoch(&self) -> Option<u64> {
        self.writer_epoch
    }
}

/// The storage representation chosen by the committed entry.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum ReplicationMode {
    /// One Reed-Solomon shard per durable voter.
    Coded,
    /// A whole logical body on each quorum voter.
    Complete,
}

/// An immutable committed entry header and its representation certificate.
#[derive(Clone, Debug)]
pub struct CommittedEntry {
    index: u64,
    mode: ReplicationMode,
    kind: CommandKind,
    payload: Bytes,
    hash: [u8; 32],
    fragments: BTreeMap<NodeId, Bytes>,
}

impl CommittedEntry {
    /// Returns the assigned monotonic log index.
    pub fn index(&self) -> u64 {
        self.index
    }
    /// Returns the representation selected by quorum acknowledgement.
    pub fn mode(&self) -> ReplicationMode {
        self.mode
    }
    /// Returns the immutable command type bound into this entry's header.
    pub fn kind(&self) -> CommandKind {
        self.kind
    }
}

/// The committed voter set and coding width for a group.
#[derive(Clone, Debug)]
pub struct GroupConfig {
    id: GroupId,
    voters: Vec<NodeId>,
    k: usize,
}

impl GroupConfig {
    /// Validates a voter configuration whose coding width can fit its voters.
    pub fn new(id: GroupId, mut voters: Vec<NodeId>, k: usize) -> Result<Self, ConsensusError> {
        voters.sort_unstable();
        voters.dedup();
        if voters.is_empty() || k == 0 || k > majority(voters.len()) {
            return Err(ConsensusError::InvalidConfiguration);
        }
        Ok(Self { id, voters, k })
    }
    /// Returns the durable routing identity for this independent group.
    pub fn id(&self) -> &GroupId {
        &self.id
    }
    /// Returns voters in their canonical fragment-placement order.
    pub(crate) fn voter_slice(&self) -> &[NodeId] {
        &self.voters
    }
    /// Returns the number of original shards required for reconstruction.
    pub(crate) fn coding_width(&self) -> usize {
        self.k
    }
}

/// A durable writer lease returned after its fencing command commits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WriterLease {
    epoch: u64,
}

impl WriterLease {
    /// Returns the fencing epoch that must accompany WAL appends.
    pub fn epoch(&self) -> u64 {
        self.epoch
    }
}

/// Consensus failures that refuse acknowledgement without a safety proof.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ConsensusError {
    /// The requested coding geometry cannot be safe.
    #[error("invalid group configuration")]
    InvalidConfiguration,
    /// The caller did not provide a current leader quorum.
    #[error("election quorum unavailable")]
    ElectionQuorumUnavailable,
    /// A mutation was sent through an ingress other than the current leader.
    #[error("not leader; current leader is {leader:?}")]
    NotLeader {
        /// Current elected leader.
        leader: NodeId,
    },
    /// Coded durability cannot intersect every legal successor quorum in k shards.
    #[error("coded quorum unavailable: require {required}, got {durable}")]
    CodedQuorumUnavailable {
        /// Required durable voters.
        required: usize,
        /// Certified voters.
        durable: usize,
    },
    /// The regular Raft majority was absent.
    #[error("write quorum unavailable")]
    WriteQuorumUnavailable,
    /// A retry identity names different immutable content.
    #[error("conflicting retry")]
    ConflictingRetry(RequestId),
    /// A WAL append names an older writer epoch.
    #[error("stale writer epoch")]
    StaleWriterEpoch {
        /// Latest committed epoch.
        expected: u64,
        /// Proposal epoch.
        actual: u64,
    },
    /// A ReadIndex quorum fence could not be obtained.
    #[error("read quorum unavailable")]
    ReadQuorumUnavailable {
        /// Required voters.
        required: usize,
        /// Reached voters.
        reached: usize,
    },
    /// The joint configuration lacks the required acknowledgement certificate.
    #[error("joint quorum unavailable")]
    JointQuorumUnavailable,
    /// Available fragments cannot reconstruct the immutable payload.
    #[error("insufficient or invalid fragments")]
    ReconstructionUnavailable,
    /// A voter could not durably write or validate its immutable record.
    #[error("durable replica unavailable")]
    DurabilityUnavailable,
}

/// In-memory executable model of one group; production transport persists the same certificates.
pub struct SimulatedGroup {
    config: GroupConfig,
    term: u64,
    leader: Option<NodeId>,
    commit: u64,
    entries: Vec<CommittedEntry>,
    retries: BTreeMap<RequestId, ([u8; 32], Option<u64>, u64)>,
    writer_epoch: u64,
    joint: Option<Vec<NodeId>>,
    former_leaders: BTreeSet<NodeId>,
}

impl SimulatedGroup {
    /// Starts an empty group in term zero.
    pub fn new(config: GroupConfig) -> Self {
        Self {
            config,
            term: 0,
            leader: None,
            commit: 0,
            entries: Vec::new(),
            retries: BTreeMap::new(),
            writer_epoch: 0,
            joint: None,
            former_leaders: BTreeSet::new(),
        }
    }
    /// Elects `leader` only after a majority of current voters grants its term.
    pub fn elect(&mut self, leader: NodeId, reachable: &[NodeId]) -> Result<u64, ConsensusError> {
        if !self.quorum(&self.config.voters, reachable) || !self.config.voters.contains(&leader) {
            return Err(ConsensusError::ElectionQuorumUnavailable);
        }
        if let Some(previous) = self.leader {
            self.former_leaders.insert(previous);
        }
        self.term += 1;
        self.leader = Some(leader);
        Ok(self.term)
    }
    /// Returns the last committed index.
    pub fn commit_index(&self) -> u64 {
        self.commit
    }
    /// Returns the active committed voter configuration.
    pub fn voters(&self) -> Vec<NodeId> {
        self.config.voters.clone()
    }
    /// Commits a Reed-Solomon representation only with a future-quorum certificate.
    pub fn propose(
        &mut self,
        ingress: NodeId,
        proposal: Proposal,
        durable: &[NodeId],
    ) -> Result<CommittedEntry, ConsensusError> {
        self.append(ingress, proposal, durable, ReplicationMode::Coded)
    }
    /// Commits a whole-entry representation when the normal majority is durable.
    pub fn propose_complete(
        &mut self,
        ingress: NodeId,
        proposal: Proposal,
        durable: &[NodeId],
    ) -> Result<CommittedEntry, ConsensusError> {
        self.append(ingress, proposal, durable, ReplicationMode::Complete)
    }
    /// Commits the next monotonically increasing writer epoch through the leader.
    pub fn acquire_writer(
        &mut self,
        ingress: NodeId,
        request: RequestId,
        writer: &str,
        durable: &[NodeId],
    ) -> Result<WriterLease, ConsensusError> {
        self.require_leader(ingress)?;
        let payload = Bytes::copy_from_slice(writer.as_bytes());
        let hash: [u8; 32] = Sha256::digest(&payload).into();
        if let Some((old_hash, epoch, _)) = self.retries.get(&request) {
            if old_hash != &hash {
                return Err(ConsensusError::ConflictingRetry(request));
            }
            return Ok(WriterLease {
                epoch: epoch.ok_or(ConsensusError::ConflictingRetry(request))?,
            });
        }
        let next = self.writer_epoch + 1;
        let proposal =
            Proposal::new(request, CommandKind::WriterLease, payload).with_writer_epoch(next);
        let _committed = self.append(ingress, proposal, durable, ReplicationMode::Complete)?;
        self.writer_epoch = next;
        Ok(WriterLease { epoch: next })
    }
    /// Obtains a current-term ReadIndex fence after a majority confirms the leader.
    pub fn read_index(
        &self,
        _ingress: NodeId,
        reachable: &[NodeId],
    ) -> Result<u64, ConsensusError> {
        let required = majority(self.config.voters.len());
        let reached = distinct_members(&self.config.voters, reachable);
        if self.leader.is_none() || reached < required {
            return Err(ConsensusError::ReadQuorumUnavailable { required, reached });
        }
        Ok(self.commit)
    }
    /// Enters joint consensus after both old and new voter majorities acknowledge it.
    pub fn begin_membership_change(
        &mut self,
        mut new_voters: Vec<NodeId>,
        durable: &[NodeId],
    ) -> Result<(), ConsensusError> {
        new_voters.sort_unstable();
        new_voters.dedup();
        if new_voters.is_empty()
            || !self.quorum(&self.config.voters, durable)
            || !self.quorum(&new_voters, durable)
        {
            return Err(ConsensusError::JointQuorumUnavailable);
        }
        self.joint = Some(new_voters);
        Ok(())
    }
    /// Finalizes joint consensus after both quorums and one newly introduced voter certify the final set.
    pub fn finish_membership_change(&mut self, durable: &[NodeId]) -> Result<(), ConsensusError> {
        let new_voters = self
            .joint
            .clone()
            .ok_or(ConsensusError::JointQuorumUnavailable)?;
        let introduced = new_voters
            .iter()
            .any(|node| !self.config.voters.contains(node) && durable.contains(node));
        if !self.quorum(&self.config.voters, durable)
            || !self.quorum(&new_voters, durable)
            || !introduced
        {
            return Err(ConsensusError::JointQuorumUnavailable);
        }
        self.config.voters = new_voters;
        self.joint = None;
        Ok(())
    }
    /// Reconstructs a committed entry from complete bodies or any `k` valid coded fragments.
    pub fn reconstruct(&self, index: u64, available: &[NodeId]) -> Result<Vec<u8>, ConsensusError> {
        let entry = self
            .entries
            .iter()
            .find(|entry| entry.index == index)
            .ok_or(ConsensusError::ReconstructionUnavailable)?;
        if entry.mode == ReplicationMode::Complete {
            return entry
                .fragments
                .iter()
                .find(|(node, _)| available.contains(node))
                .map(|(_, body)| body.to_vec())
                .ok_or(ConsensusError::ReconstructionUnavailable);
        }
        let mut parts: Vec<(usize, Bytes)> = entry
            .fragments
            .iter()
            .filter_map(|(node, bytes)| {
                available.contains(node).then_some((
                    self.config.voters.iter().position(|v| v == node)?,
                    bytes.clone(),
                ))
            })
            .collect();
        parts.sort_by_key(|(part, _)| *part);
        if parts.len() < self.config.k {
            return Err(ConsensusError::ReconstructionUnavailable);
        }
        decode(
            self.config.k,
            self.config.voters.len() - self.config.k,
            entry.payload.len(),
            &parts,
        )
        .and_then(|body| {
            if Sha256::digest(&body).as_slice() == entry.hash {
                Ok(body)
            } else {
                Err(ConsensusError::ReconstructionUnavailable)
            }
        })
    }
    /// Appends only after retry identity, fencing, Raft quorum, and representation certificate checks.
    fn append(
        &mut self,
        ingress: NodeId,
        proposal: Proposal,
        durable: &[NodeId],
        mode: ReplicationMode,
    ) -> Result<CommittedEntry, ConsensusError> {
        self.require_leader(ingress)?;
        let hash: [u8; 32] = Sha256::digest(&proposal.payload).into();
        if let Some((old_hash, old_epoch, index)) = self.retries.get(&proposal.request) {
            if old_hash != &hash || old_epoch != &proposal.writer_epoch {
                return Err(ConsensusError::ConflictingRetry(proposal.request));
            }
            return self
                .entries
                .iter()
                .find(|entry| entry.index == *index)
                .cloned()
                .ok_or(ConsensusError::ConflictingRetry(proposal.request));
        }
        if proposal.kind == CommandKind::Wal
            && let Some(actual) = proposal.writer_epoch
            && actual != self.writer_epoch
        {
            return Err(ConsensusError::StaleWriterEpoch {
                expected: self.writer_epoch,
                actual,
            });
        }
        if !self.quorum(&self.config.voters, durable) {
            return Err(ConsensusError::WriteQuorumUnavailable);
        }
        let needed = self.config.voters.len() - majority(self.config.voters.len()) + self.config.k;
        let certified = distinct_members(&self.config.voters, durable);
        if mode == ReplicationMode::Coded && certified < needed {
            return Err(ConsensusError::CodedQuorumUnavailable {
                required: needed,
                durable: certified,
            });
        }
        let holders: Vec<NodeId> = self
            .config
            .voters
            .iter()
            .copied()
            .filter(|node| durable.contains(node))
            .collect();
        let fragments = if mode == ReplicationMode::Complete {
            holders
                .into_iter()
                .map(|node| (node, proposal.payload.clone()))
                .collect()
        } else {
            encode(
                self.config.k,
                self.config.voters.len() - self.config.k,
                &proposal.payload,
            )?
            .into_iter()
            .enumerate()
            .filter_map(|(slot, bytes)| {
                self.config
                    .voters
                    .get(slot)
                    .filter(|node| durable.contains(node))
                    .map(|node| (*node, bytes))
            })
            .collect()
        };
        self.commit += 1;
        let entry = CommittedEntry {
            index: self.commit,
            mode,
            kind: proposal.kind,
            payload: proposal.payload,
            hash,
            fragments,
        };
        self.retries
            .insert(proposal.request, (hash, proposal.writer_epoch, entry.index));
        self.entries.push(entry.clone());
        Ok(entry)
    }
    /// Ensures mutations are ordered solely by the elected leader.
    fn require_leader(&self, ingress: NodeId) -> Result<(), ConsensusError> {
        match self.leader {
            Some(leader) if self.former_leaders.contains(&ingress) && ingress != leader => {
                Err(ConsensusError::NotLeader { leader })
            }
            Some(_) => Ok(()),
            None => Err(ConsensusError::ElectionQuorumUnavailable),
        }
    }
    /// Tests a majority certificate against a specific voter set.
    fn quorum(&self, voters: &[NodeId], reachable: &[NodeId]) -> bool {
        distinct_members(voters, reachable) >= majority(voters.len())
    }
}

/// Returns the smallest Raft majority for `count` voters.
fn majority(count: usize) -> usize {
    count / 2 + 1
}
/// Counts distinct reachable identities that belong to `voters`.
fn distinct_members(voters: &[NodeId], reachable: &[NodeId]) -> usize {
    reachable
        .iter()
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .filter(|node| voters.contains(node))
        .count()
}
/// Reed-Solomon encodes a padded one-stripe logical body into all voter shards.
fn encode(k: usize, m: usize, body: &[u8]) -> Result<Vec<Bytes>, ConsensusError> {
    let chunk = body.len().div_ceil(k).max(2).next_multiple_of(2);
    let mut padded = body.to_vec();
    padded.resize(k * chunk, 0);
    let mut encoder = ReedSolomonEncoder::new(k, m, chunk)
        .map_err(|_| ConsensusError::ReconstructionUnavailable)?;
    let mut result = Vec::new();
    for chunk_bytes in padded.chunks_exact(chunk) {
        encoder
            .add_original_shard(chunk_bytes)
            .map_err(|_| ConsensusError::ReconstructionUnavailable)?;
        result.push(Bytes::copy_from_slice(chunk_bytes));
    }
    let coded = encoder
        .encode()
        .map_err(|_| ConsensusError::ReconstructionUnavailable)?;
    result.extend(coded.recovery_iter().map(Bytes::copy_from_slice));
    Ok(result)
}
/// Reed-Solomon decodes one padded stripe and restores its original logical length.
fn decode(
    k: usize,
    m: usize,
    length: usize,
    parts: &[(usize, Bytes)],
) -> Result<Vec<u8>, ConsensusError> {
    let chunk = parts
        .first()
        .ok_or(ConsensusError::ReconstructionUnavailable)?
        .1
        .len();
    let mut decoder = ReedSolomonDecoder::new(k, m, chunk)
        .map_err(|_| ConsensusError::ReconstructionUnavailable)?;
    for (slot, data) in parts {
        if *slot < k {
            decoder
                .add_original_shard(*slot, data)
                .map_err(|_| ConsensusError::ReconstructionUnavailable)?;
        } else {
            decoder
                .add_recovery_shard(*slot - k, data)
                .map_err(|_| ConsensusError::ReconstructionUnavailable)?;
        }
    }
    let decoded = decoder
        .decode()
        .map_err(|_| ConsensusError::ReconstructionUnavailable)?;
    let restored: BTreeMap<usize, &[u8]> = decoded.restored_original_iter().collect();
    let mut body = Vec::with_capacity(k * chunk);
    for slot in 0..k {
        if let Some((_, data)) = parts.iter().find(|(found, _)| *found == slot) {
            body.extend_from_slice(data);
        } else {
            body.extend_from_slice(
                restored
                    .get(&slot)
                    .ok_or(ConsensusError::ReconstructionUnavailable)?,
            );
        }
    }
    body.truncate(length);
    Ok(body)
}
pub use catalog::{
    CatalogAction, CatalogBatch, CatalogBatchError, CatalogEntity, CatalogRequirement,
};
