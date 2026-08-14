//! Crash-safe per-voter persistence for immutable coded consensus entries.
//!
//! Each replica writes a canonical header and one verified representation before
//! acknowledgement. Recovery accepts only quorum-agreed headers and verified data.

use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use bytes::Bytes;
use crc32c::crc32c;
use sha2::{Digest, Sha256};

use crate::{
    CommandKind, ConsensusError, GroupConfig, NodeId, Proposal, ReplicationMode, RequestId, decode,
    distinct_members, encode, majority,
};

/// A single voter's crash-safe directory of immutable entry files.
pub struct FileReplica {
    node: NodeId,
    root: PathBuf,
}

impl FileReplica {
    /// Opens a replica directory, creating its durable entry namespace if absent.
    pub fn open(node: NodeId, root: PathBuf) -> Result<Self, ConsensusError> {
        fs::create_dir_all(root.join("entries"))
            .map_err(|_| ConsensusError::DurabilityUnavailable)?;
        Ok(Self { node, root })
    }
    /// Returns the voter identity that owns this directory.
    fn node(&self) -> NodeId {
        self.node
    }
    /// Writes one header and fragment with temp-file replacement and directory sync.
    fn persist(&self, header: &Header, slot: usize, fragment: &[u8]) -> Result<(), ConsensusError> {
        let entry = self.root.join("entries").join(header.index.to_string());
        fs::create_dir_all(&entry).map_err(|_| ConsensusError::DurabilityUnavailable)?;
        atomic_write(&entry.join("header"), &header.encode())?;
        let mut representation = (slot as u64).to_be_bytes().to_vec();
        representation.extend_from_slice(fragment);
        atomic_write(&entry.join("fragment"), &checksummed(&representation))?;
        sync_dir(&entry)?;
        sync_dir(&self.root.join("entries"))
    }
    /// Reads and validates a complete durable record for `index`.
    fn load(&self, index: u64) -> Result<(Header, Bytes), ConsensusError> {
        let entry = self.root.join("entries").join(index.to_string());
        let header = Header::decode(&read_file(&entry.join("header"))?)?;
        let fragment = validate_checksummed(&read_file(&entry.join("fragment"))?)?;
        if fragment.len() < 8 {
            return Err(ConsensusError::DurabilityUnavailable);
        }
        Ok((header, Bytes::from(fragment)))
    }
}

/// Durable group state reconstructed entirely from voter files.
pub struct DurableGroup {
    config: GroupConfig,
    replicas: BTreeMap<NodeId, Arc<FileReplica>>,
    leader: Option<NodeId>,
    term: u64,
    commit: u64,
    retries: BTreeMap<RequestId, ([u8; 32], Option<u64>, u64)>,
    writer_epoch: u64,
}

impl DurableGroup {
    /// Creates an empty group after checking that every configured voter has one replica.
    pub fn bootstrap(
        config: GroupConfig,
        replicas: Vec<Arc<FileReplica>>,
    ) -> Result<Self, ConsensusError> {
        Ok(Self {
            replicas: replica_map(&config, replicas)?,
            config,
            leader: None,
            term: 0,
            commit: 0,
            retries: BTreeMap::new(),
            writer_epoch: 0,
        })
    }
    /// Recovers the committed prefix from a legal election quorum of durable voter files.
    pub fn recover(
        config: GroupConfig,
        replicas: Vec<Arc<FileReplica>>,
        reachable: &[NodeId],
    ) -> Result<Self, ConsensusError> {
        if distinct_members(config.voter_slice(), reachable) < majority(config.voter_slice().len())
        {
            return Err(ConsensusError::ElectionQuorumUnavailable);
        }
        let replica_map: BTreeMap<_, _> = replicas
            .into_iter()
            .map(|replica| (replica.node(), replica))
            .collect();
        let mut group = Self {
            config,
            replicas: replica_map,
            leader: None,
            term: 0,
            commit: 0,
            retries: BTreeMap::new(),
            writer_epoch: 0,
        };
        loop {
            let index = group.commit + 1;
            let records: Vec<_> = reachable
                .iter()
                .filter_map(|node| {
                    group
                        .replicas
                        .get(node)
                        .and_then(|replica| replica.load(index).ok())
                })
                .collect();
            if records.is_empty() {
                break;
            }
            let header = agreed_header(&records, majority(group.config.voter_slice().len()))?;
            let body = reconstruct(&group.config, &header, &records)?;
            if Sha256::digest(&body).as_slice() != header.hash {
                return Err(ConsensusError::ReconstructionUnavailable);
            }
            group
                .retries
                .insert(header.request, (header.hash, header.writer_epoch, index));
            if header.kind == CommandKind::WriterLease {
                group.writer_epoch = header
                    .writer_epoch
                    .ok_or(ConsensusError::ReconstructionUnavailable)?;
            }
            group.commit = index;
        }
        Ok(group)
    }
    /// Elects a current leader after a regular voter majority is reachable.
    pub fn elect(&mut self, leader: NodeId, reachable: &[NodeId]) -> Result<u64, ConsensusError> {
        if !self.replicas.contains_key(&leader)
            || distinct_members(self.config.voter_slice(), reachable)
                < majority(self.config.voter_slice().len())
        {
            return Err(ConsensusError::ElectionQuorumUnavailable);
        }
        self.term += 1;
        self.leader = Some(leader);
        Ok(self.term)
    }
    /// Persists and commits a coded command once its durable certificate is safe.
    pub fn propose(
        &mut self,
        ingress: NodeId,
        proposal: Proposal,
        durable: &[NodeId],
    ) -> Result<crate::CommittedEntry, ConsensusError> {
        self.append(ingress, proposal, durable, ReplicationMode::Coded)
    }
    /// Persists a complete logical entry on a regular Raft majority.
    pub fn propose_complete(
        &mut self,
        ingress: NodeId,
        proposal: Proposal,
        durable: &[NodeId],
    ) -> Result<crate::CommittedEntry, ConsensusError> {
        self.append(ingress, proposal, durable, ReplicationMode::Complete)
    }
    /// Commits the next exclusive WAL writer epoch in this group's durable log.
    pub fn acquire_writer(
        &mut self,
        ingress: NodeId,
        request: RequestId,
        writer: &str,
        durable: &[NodeId],
    ) -> Result<crate::WriterLease, ConsensusError> {
        let payload = Bytes::copy_from_slice(writer.as_bytes());
        let hash: [u8; 32] = Sha256::digest(&payload).into();
        if let Some((old_hash, epoch, _)) = self.retries.get(&request) {
            if old_hash != &hash {
                return Err(ConsensusError::ConflictingRetry(request));
            }
            return Ok(crate::WriterLease {
                epoch: epoch.ok_or(ConsensusError::ConflictingRetry(request))?,
            });
        }
        let next = self.writer_epoch + 1;
        let proposal =
            Proposal::new(request, CommandKind::WriterLease, payload).with_writer_epoch(next);
        let _entry = self.append(ingress, proposal, durable, ReplicationMode::Complete)?;
        self.writer_epoch = next;
        Ok(crate::WriterLease { epoch: next })
    }
    /// Returns the last recovered or committed index.
    pub fn commit_index(&self) -> u64 {
        self.commit
    }
    /// Reconstructs and verifies a committed logical body before exposing it.
    pub fn read_committed(&self, index: u64) -> Result<Vec<u8>, ConsensusError> {
        if index == 0 || index > self.commit {
            return Err(ConsensusError::ReconstructionUnavailable);
        }
        let records: Vec<_> = self
            .replicas
            .values()
            .filter_map(|replica| replica.load(index).ok())
            .collect();
        let header = agreed_header(&records, majority(self.config.voter_slice().len()))?;
        reconstruct(&self.config, &header, &records)
    }
    /// Persists each voter record before advancing the in-memory commit result.
    fn append(
        &mut self,
        _ingress: NodeId,
        proposal: Proposal,
        durable: &[NodeId],
        mode: ReplicationMode,
    ) -> Result<crate::CommittedEntry, ConsensusError> {
        if self.leader.is_none() {
            return Err(ConsensusError::ElectionQuorumUnavailable);
        }
        let hash: [u8; 32] = Sha256::digest(proposal.payload()).into();
        if let Some((old, epoch, _)) = self.retries.get(&proposal.request()) {
            if old != &hash || epoch != &proposal.writer_epoch() {
                return Err(ConsensusError::ConflictingRetry(proposal.request()));
            }
            return Ok(crate::CommittedEntry {
                index: self.retries[&proposal.request()].2,
                mode,
                kind: proposal.kind(),
                payload: proposal.payload().clone(),
                hash,
                fragments: BTreeMap::new(),
            });
        }
        if proposal.kind() == CommandKind::Wal
            && let Some(actual) = proposal.writer_epoch()
            && actual != self.writer_epoch
        {
            return Err(ConsensusError::StaleWriterEpoch {
                expected: self.writer_epoch,
                actual,
            });
        }
        let voters = self.config.voter_slice();
        let certified = distinct_members(voters, durable);
        if certified < majority(voters.len()) {
            return Err(ConsensusError::WriteQuorumUnavailable);
        }
        let needed = voters.len() - majority(voters.len()) + self.config.coding_width();
        if mode == ReplicationMode::Coded && certified < needed {
            return Err(ConsensusError::CodedQuorumUnavailable {
                required: needed,
                durable: certified,
            });
        }
        let header = Header {
            index: self.commit + 1,
            term: self.term,
            mode,
            kind: proposal.kind(),
            length: proposal.payload().len(),
            hash,
            request: proposal.request(),
            writer_epoch: proposal.writer_epoch(),
            k: self.config.coding_width(),
            n: voters.len(),
        };
        let parts = if mode == ReplicationMode::Coded {
            encode(header.k, header.n - header.k, proposal.payload())?
        } else {
            voters.iter().map(|_| proposal.payload().clone()).collect()
        };
        for (slot, node) in voters.iter().enumerate() {
            if durable.contains(node) {
                self.replicas
                    .get(node)
                    .ok_or(ConsensusError::WriteQuorumUnavailable)?
                    .persist(&header, slot, &parts[slot])?;
            }
        }
        self.retries
            .insert(header.request, (hash, header.writer_epoch, header.index));
        self.commit = header.index;
        Ok(crate::CommittedEntry {
            index: header.index,
            mode: header.mode,
            kind: header.kind,
            payload: proposal.payload().clone(),
            hash,
            fragments: BTreeMap::new(),
        })
    }
}

#[derive(Clone, Eq, PartialEq)]
struct Header {
    index: u64,
    term: u64,
    mode: ReplicationMode,
    kind: CommandKind,
    length: usize,
    hash: [u8; 32],
    request: RequestId,
    writer_epoch: Option<u64>,
    k: usize,
    n: usize,
}
impl Header {
    /// Encodes fields in one fixed-endian schema followed by a checksum.
    fn encode(&self) -> Vec<u8> {
        let mut b = Vec::new();
        b.extend_from_slice(&self.index.to_be_bytes());
        b.extend_from_slice(&self.term.to_be_bytes());
        b.push(if self.mode == ReplicationMode::Coded {
            0
        } else {
            1
        });
        b.push(match self.kind {
            CommandKind::TimelineOpen => 7,
            CommandKind::WalArchiveBinding => 9,
            CommandKind::Catalog => 0,
            CommandKind::Wal => 1,
            CommandKind::WriterLease => 2,
            CommandKind::ArchiveCheckpoint => 3,
            CommandKind::ReleaseWriter => 4,
            CommandKind::Object => 5,
            CommandKind::TenantRoot => 6,
            CommandKind::CatalogCheckpoint => 8,
        });
        b.extend_from_slice(&(self.length as u64).to_be_bytes());
        b.extend_from_slice(&self.hash);
        b.extend_from_slice(&self.request.as_u128().to_be_bytes());
        b.push(self.writer_epoch.is_some() as u8);
        b.extend_from_slice(&self.writer_epoch.unwrap_or_default().to_be_bytes());
        b.extend_from_slice(&(self.k as u64).to_be_bytes());
        b.extend_from_slice(&(self.n as u64).to_be_bytes());
        checksummed(&b)
    }
    /// Decodes only a complete checksummed canonical header.
    fn decode(bytes: &[u8]) -> Result<Self, ConsensusError> {
        let b = validate_checksummed(bytes)?;
        if b.len() != 99 {
            return Err(ConsensusError::DurabilityUnavailable);
        }
        let index = u64::from_be_bytes(
            b[0..8]
                .try_into()
                .map_err(|_| ConsensusError::DurabilityUnavailable)?,
        );
        let term = u64::from_be_bytes(
            b[8..16]
                .try_into()
                .map_err(|_| ConsensusError::DurabilityUnavailable)?,
        );
        let mode = match b[16] {
            0 => ReplicationMode::Coded,
            1 => ReplicationMode::Complete,
            _ => return Err(ConsensusError::DurabilityUnavailable),
        };
        let kind = match b[17] {
            0 => CommandKind::Catalog,
            1 => CommandKind::Wal,
            2 => CommandKind::WriterLease,
            3 => CommandKind::ArchiveCheckpoint,
            4 => CommandKind::ReleaseWriter,
            5 => CommandKind::Object,
            6 => CommandKind::TenantRoot,
            7 => CommandKind::TimelineOpen,
            8 => CommandKind::CatalogCheckpoint,
            9 => CommandKind::WalArchiveBinding,
            _ => return Err(ConsensusError::DurabilityUnavailable),
        };
        let length = u64::from_be_bytes(
            b[18..26]
                .try_into()
                .map_err(|_| ConsensusError::DurabilityUnavailable)?,
        ) as usize;
        let mut hash = [0; 32];
        hash.copy_from_slice(&b[26..58]);
        let request = RequestId::from_u128(u128::from_be_bytes(
            b[58..74]
                .try_into()
                .map_err(|_| ConsensusError::DurabilityUnavailable)?,
        ));
        let writer_epoch = match b[74] {
            0 => None,
            1 => Some(u64::from_be_bytes(
                b[75..83]
                    .try_into()
                    .map_err(|_| ConsensusError::DurabilityUnavailable)?,
            )),
            _ => return Err(ConsensusError::DurabilityUnavailable),
        };
        let k = u64::from_be_bytes(
            b[83..91]
                .try_into()
                .map_err(|_| ConsensusError::DurabilityUnavailable)?,
        ) as usize;
        let n = u64::from_be_bytes(
            b[91..99]
                .try_into()
                .map_err(|_| ConsensusError::DurabilityUnavailable)?,
        ) as usize;
        Ok(Self {
            index,
            term,
            mode,
            kind,
            length,
            hash,
            request,
            writer_epoch,
            k,
            n,
        })
    }
}

/// Maps configured voter identities to their single owned storage directories.
fn replica_map(
    config: &GroupConfig,
    replicas: Vec<Arc<FileReplica>>,
) -> Result<BTreeMap<NodeId, Arc<FileReplica>>, ConsensusError> {
    let map: BTreeMap<_, _> = replicas.into_iter().map(|r| (r.node(), r)).collect();
    if config.voter_slice().iter().all(|v| map.contains_key(v)) {
        Ok(map)
    } else {
        Err(ConsensusError::DurabilityUnavailable)
    }
}
/// Selects one header only if the required records have byte-for-byte agreement.
fn agreed_header(records: &[(Header, Bytes)], required: usize) -> Result<Header, ConsensusError> {
    let mut counts = BTreeMap::<Vec<u8>, usize>::new();
    for (h, _) in records {
        *counts.entry(h.encode()).or_default() += 1;
    }
    let (raw, count) = counts
        .into_iter()
        .max_by_key(|(_, count)| *count)
        .ok_or(ConsensusError::ReconstructionUnavailable)?;
    if count < required {
        return Err(ConsensusError::ReconstructionUnavailable);
    }
    Header::decode(&raw)
}
/// Reconstructs an entry using only fragments whose headers match exactly.
fn reconstruct(
    config: &GroupConfig,
    header: &Header,
    records: &[(Header, Bytes)],
) -> Result<Vec<u8>, ConsensusError> {
    if header.k != config.coding_width() || header.n != config.voter_slice().len() {
        return Err(ConsensusError::ReconstructionUnavailable);
    }
    let matched: Vec<_> = records
        .iter()
        .filter(|(h, _)| h.index == header.index && h.hash == header.hash && h.mode == header.mode)
        .collect();
    if header.mode == ReplicationMode::Complete {
        let raw = &matched
            .first()
            .ok_or(ConsensusError::ReconstructionUnavailable)?
            .1;
        let body = raw[8..].to_vec();
        return if body.len() == header.length && Sha256::digest(&body).as_slice() == header.hash {
            Ok(body)
        } else {
            Err(ConsensusError::ReconstructionUnavailable)
        };
    }
    let parts: Vec<_> = matched
        .into_iter()
        .filter_map(|(_, b)| {
            let slot = u64::from_be_bytes(b[..8].try_into().ok()?) as usize;
            (slot < header.n).then(|| (slot, Bytes::copy_from_slice(&b[8..])))
        })
        .collect();
    let body = decode(
        config.coding_width(),
        config.voter_slice().len() - config.coding_width(),
        header.length,
        &parts,
    )?;
    if Sha256::digest(&body).as_slice() == header.hash {
        Ok(body)
    } else {
        Err(ConsensusError::ReconstructionUnavailable)
    }
}
/// Writes one file durably before atomically replacing its predecessor.
fn atomic_write(path: &Path, bytes: &[u8]) -> Result<(), ConsensusError> {
    let temp = path.with_extension("tmp");
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&temp)
        .map_err(|_| ConsensusError::DurabilityUnavailable)?;
    file.write_all(bytes)
        .map_err(|_| ConsensusError::DurabilityUnavailable)?;
    file.sync_all()
        .map_err(|_| ConsensusError::DurabilityUnavailable)?;
    fs::rename(temp, path).map_err(|_| ConsensusError::DurabilityUnavailable)?;
    Ok(())
}
/// Syncs a directory so prior rename metadata survives a crash.
fn sync_dir(path: &Path) -> Result<(), ConsensusError> {
    File::open(path)
        .map_err(|_| ConsensusError::DurabilityUnavailable)?
        .sync_all()
        .map_err(|_| ConsensusError::DurabilityUnavailable)
}
/// Reads an entire small immutable representation file.
fn read_file(path: &Path) -> Result<Vec<u8>, ConsensusError> {
    let mut b = Vec::new();
    File::open(path)
        .map_err(|_| ConsensusError::DurabilityUnavailable)?
        .read_to_end(&mut b)
        .map_err(|_| ConsensusError::DurabilityUnavailable)?;
    Ok(b)
}
/// Appends a CRC32C over canonical bytes.
fn checksummed(bytes: &[u8]) -> Vec<u8> {
    let mut out = bytes.to_vec();
    out.extend_from_slice(&crc32c(bytes).to_be_bytes());
    out
}
/// Verifies and removes the trailing representation checksum.
fn validate_checksummed(bytes: &[u8]) -> Result<Vec<u8>, ConsensusError> {
    if bytes.len() < 4 {
        return Err(ConsensusError::DurabilityUnavailable);
    }
    let (body, crc) = bytes.split_at(bytes.len() - 4);
    let found = u32::from_be_bytes(
        crc.try_into()
            .map_err(|_| ConsensusError::DurabilityUnavailable)?,
    );
    if crc32c(body) != found {
        return Err(ConsensusError::DurabilityUnavailable);
    }
    Ok(body.to_vec())
}
