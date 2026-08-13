//! Durable payload representations staged before their small Raft header commits.
//!
//! A certificate is returned only after every named representation has reached
//! a voter file and its containing directory. Raft later decides visibility.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use bytes::Bytes;
use crc32c::crc32c;
use sha2::{Digest, Sha256};

use crate::{PayloadCertificate, ReplicationMode, RequestId, decode, encode};

/// One self-describing representation transferred to a committed voter.
#[derive(Clone, Debug)]
pub struct PayloadRepresentation {
    /// OpenRaft term assigned after the allocation commits; zero is unsealed.
    pub term: u64,
    /// OpenRaft index assigned after the allocation commits; zero is unsealed.
    pub index: u64,
    /// Group that allocated this representation.
    pub group: String,
    /// Configuration generation that selected its holder slots.
    pub configuration_generation: u64,
    /// Immutable representation mode.
    pub mode: ReplicationMode,
    /// Logical content hash used as the durable object identity.
    pub hash: [u8; 32],
    /// Exact retry identity bound to this staged body.
    pub request: RequestId,
    /// Original logical byte length before coding padding.
    pub length: usize,
    /// Deterministic shard slot in the committed voter ordering.
    pub slot: usize,
    /// One coded shard or one complete logical copy.
    pub bytes: Bytes,
}

/// Immutable committed payload identity and target allocation for one repair.
pub struct RepairRequest<'a> {
    pub hash: [u8; 32],
    pub group: &'a str,
    pub source_configuration_generation: u64,
    pub target_configuration_generation: u64,
    pub request: RequestId,
    pub length: u64,
    /// Exact committed source term used to validate source representations.
    pub source_term: u64,
    /// Exact committed source index used to validate source representations.
    pub source_index: u64,
    pub certificate: &'a PayloadCertificate,
    pub target_voters: Vec<u64>,
}

/// Exact allocation identity rewritten with a committed Raft seal.
pub struct SealRequest<'a> {
    pub hash: [u8; 32],
    pub group: &'a str,
    pub configuration_generation: u64,
    pub request: RequestId,
    pub term: u64,
    pub index: u64,
    pub certificate: &'a PayloadCertificate,
}

/// Exact immutable identity required to reconstruct one committed allocation.
pub struct ReconstructRequest<'a> {
    pub hash: [u8; 32],
    pub group: &'a str,
    pub configuration_generation: u64,
    pub request: RequestId,
    pub length: u64,
    pub term: u64,
    pub index: u64,
    pub certificate: &'a PayloadCertificate,
}

/// Exact committed allocation identity that may be physically reclaimed.
pub struct ReleaseRequest<'a> {
    pub hash: [u8; 32],
    pub group: &'a str,
    pub configuration_generation: u64,
    pub request: RequestId,
    pub length: u64,
    pub term: u64,
    pub index: u64,
    pub certificate: &'a PayloadCertificate,
}

/// Peer transport whose successful store means the target voter fsynced bytes.
#[async_trait::async_trait]
pub trait RepresentationTransport: Send + Sync {
    /// Persists one representation on its explicitly configured voter.
    async fn store(
        &self,
        voter: u64,
        representation: PayloadRepresentation,
    ) -> Result<(), PayloadError>;

    /// Loads one checksum-verified representation from a voter.
    async fn load(
        &self,
        voter: u64,
        hash: [u8; 32],
        group: &str,
        configuration_generation: u64,
        request: RequestId,
    ) -> Result<Option<PayloadRepresentation>, PayloadError>;

    /// Removes one exact representation after its committed header is no longer retained.
    async fn delete(
        &self,
        voter: u64,
        hash: [u8; 32],
        group: &str,
        configuration_generation: u64,
        request: RequestId,
        slot: usize,
    ) -> Result<(), PayloadError>;
}

/// Distributed payload store over the cache ring's durable peer transport.
pub struct DistributedPayloadStore {
    k: usize,
    m: usize,
    voters: std::sync::RwLock<Vec<u64>>,
    transport: Arc<dyn RepresentationTransport>,
}

impl DistributedPayloadStore {
    /// Binds coding slots to one explicit committed voter ordering.
    pub fn new(
        k: usize,
        m: usize,
        voters: Vec<u64>,
        transport: Arc<dyn RepresentationTransport>,
    ) -> Result<Self, PayloadError> {
        let distinct = voters.iter().copied().collect::<BTreeSet<_>>();
        if k == 0
            || m == 0
            || k.checked_add(m) != Some(voters.len())
            || distinct.len() != voters.len()
            || voters.contains(&0)
        {
            return Err(PayloadError::InvalidGeometry);
        }
        Ok(Self {
            k,
            m,
            voters: std::sync::RwLock::new(voters),
            transport,
        })
    }
}

#[async_trait::async_trait]
impl PayloadStore for DistributedPayloadStore {
    async fn stage(
        &self,
        request: RequestId,
        group: &str,
        configuration_generation: u64,
        mode: ReplicationMode,
        body: &[u8],
        holders: &[u64],
    ) -> Result<StagedPayload, PayloadError> {
        let certificate = PayloadCertificate::with_voters(
            mode,
            self.k,
            self.m,
            self.voters
                .read()
                .map_err(|_| PayloadError::Transport("payload voters are poisoned".to_owned()))?
                .clone(),
            holders.to_vec(),
        )
        .map_err(|error| PayloadError::InsufficientDurability {
            required: error.required(),
            durable: holders.iter().copied().collect::<BTreeSet<_>>().len(),
        })?;
        let hash: [u8; 32] = Sha256::digest(body).into();
        let voters = self
            .voters
            .read()
            .map_err(|_| PayloadError::Transport("payload voters are poisoned".to_owned()))?
            .clone();
        let representations = match mode {
            ReplicationMode::Coded => {
                encode(self.k, self.m, body).map_err(|_| PayloadError::ReconstructionUnavailable)?
            }
            ReplicationMode::Complete => voters
                .iter()
                .map(|_| Bytes::copy_from_slice(body))
                .collect(),
        };
        for voter in certificate.holders() {
            let slot = voters
                .iter()
                .position(|candidate| candidate == voter)
                .ok_or(PayloadError::UnknownHolder)?;
            self.transport
                .store(
                    *voter,
                    PayloadRepresentation {
                        term: 0,
                        index: 0,
                        group: group.to_owned(),
                        configuration_generation,
                        mode,
                        hash,
                        request,
                        length: body.len(),
                        slot,
                        bytes: representations[slot].clone(),
                    },
                )
                .await?;
        }
        Ok(StagedPayload {
            hash,
            length: body.len() as u64,
            certificate,
        })
    }

    async fn reconstruct(&self, read: ReconstructRequest<'_>) -> Result<Bytes, PayloadError> {
        let ReconstructRequest {
            hash,
            group,
            configuration_generation,
            request,
            length,
            term,
            index,
            certificate,
        } = read;
        let mut records = Vec::new();
        for voter in certificate.holders() {
            if let Some(record) = self
                .transport
                .load(*voter, hash, group, configuration_generation, request)
                .await?
            {
                let expected_slot = certificate
                    .voters()
                    .iter()
                    .position(|candidate| candidate == voter)
                    .ok_or(PayloadError::UnknownHolder)?;
                if record.group != group
                    || record.configuration_generation != configuration_generation
                    || record.mode != certificate.mode()
                    || record.hash != hash
                    || record.request != request
                    || record.length as u64 != length
                    || record.slot != expected_slot
                    || record.term != term
                    || record.index != index
                {
                    return Err(PayloadError::CorruptRepresentation);
                }
                records.push(record);
            }
        }
        let first = records
            .first()
            .ok_or(PayloadError::ReconstructionUnavailable)?;
        if first.hash != hash
            || first.mode != certificate.mode()
            || records.iter().any(|record| {
                record.group != first.group
                    || record.configuration_generation != first.configuration_generation
                    || record.mode != first.mode
                    || record.hash != first.hash
                    || record.request != first.request
                    || record.length != first.length
            })
        {
            return Err(PayloadError::CorruptRepresentation);
        }
        let body = match certificate.mode() {
            ReplicationMode::Complete => first.bytes.to_vec(),
            ReplicationMode::Coded => decode(
                certificate.k(),
                certificate.m(),
                first.length,
                &records
                    .iter()
                    .map(|record| (record.slot, record.bytes.clone()))
                    .collect::<Vec<_>>(),
            )
            .map_err(|_| PayloadError::ReconstructionUnavailable)?,
        };
        if body.len() != first.length || <[u8; 32]>::from(Sha256::digest(&body)) != hash {
            return Err(PayloadError::CorruptRepresentation);
        }
        Ok(Bytes::from(body))
    }

    async fn set_voters(&self, voters: Vec<u64>) -> Result<(), PayloadError> {
        if voters.len() != self.k + self.m
            || voters.iter().copied().collect::<BTreeSet<_>>().len() != voters.len()
        {
            return Err(PayloadError::InvalidGeometry);
        }
        *self
            .voters
            .write()
            .map_err(|_| PayloadError::Transport("payload voters are poisoned".to_owned()))? =
            voters;
        Ok(())
    }

    async fn repair(&self, repair: RepairRequest<'_>) -> Result<PayloadCertificate, PayloadError> {
        let RepairRequest {
            hash,
            group,
            source_configuration_generation,
            target_configuration_generation,
            request,
            length,
            source_term,
            source_index,
            certificate,
            target_voters,
        } = repair;
        if target_voters.len() != self.k + self.m
            || target_voters.iter().copied().collect::<BTreeSet<_>>().len() != target_voters.len()
        {
            return Err(PayloadError::InvalidGeometry);
        }
        let body = self
            .reconstruct(ReconstructRequest {
                hash,
                group,
                configuration_generation: source_configuration_generation,
                request,
                length,
                term: source_term,
                index: source_index,
                certificate,
            })
            .await?;
        let repaired = PayloadCertificate::with_voters(
            certificate.mode(),
            self.k,
            self.m,
            target_voters.clone(),
            target_voters.clone(),
        )
        .map_err(|_| PayloadError::InvalidGeometry)?;
        let representations = match certificate.mode() {
            ReplicationMode::Coded => encode(self.k, self.m, &body)
                .map_err(|_| PayloadError::ReconstructionUnavailable)?,
            ReplicationMode::Complete => target_voters
                .iter()
                .map(|_| Bytes::copy_from_slice(&body))
                .collect(),
        };
        for (slot, voter) in target_voters.iter().enumerate() {
            self.transport
                .store(
                    *voter,
                    PayloadRepresentation {
                        term: 0,
                        index: 0,
                        group: group.to_owned(),
                        configuration_generation: target_configuration_generation,
                        mode: certificate.mode(),
                        hash,
                        request,
                        length: length as usize,
                        slot,
                        bytes: representations[slot].clone(),
                    },
                )
                .await?;
        }
        Ok(repaired)
    }

    async fn seal(&self, seal: SealRequest<'_>) -> Result<(), PayloadError> {
        let SealRequest {
            hash,
            group,
            configuration_generation,
            request,
            term,
            index,
            certificate,
        } = seal;
        if term == 0 || index == 0 {
            return Err(PayloadError::CorruptRepresentation);
        }
        for voter in certificate.holders() {
            let mut record = self
                .transport
                .load(*voter, hash, group, configuration_generation, request)
                .await?
                .ok_or(PayloadError::ReconstructionUnavailable)?;
            if record.request != request
                || record.group != group
                || record.configuration_generation != configuration_generation
                || (record.term != 0 && (record.term != term || record.index != index))
            {
                return Err(PayloadError::CorruptRepresentation);
            }
            if record.term == term && record.index == index {
                continue;
            }
            record.term = term;
            record.index = index;
            self.transport.store(*voter, record).await?;
        }
        Ok(())
    }

    async fn release(&self, release: ReleaseRequest<'_>) -> Result<(), PayloadError> {
        let ReleaseRequest {
            hash,
            group,
            configuration_generation,
            request,
            length,
            term,
            index,
            certificate,
        } = release;
        for voter in certificate.holders() {
            let slot = certificate
                .voters()
                .iter()
                .position(|candidate| candidate == voter)
                .ok_or(PayloadError::UnknownHolder)?;
            let record = self
                .transport
                .load(*voter, hash, group, configuration_generation, request)
                .await?;
            if record.is_some_and(|record| {
                record.group != group
                    || record.configuration_generation != configuration_generation
                    || record.mode != certificate.mode()
                    || record.hash != hash
                    || record.request != request
                    || record.length as u64 != length
                    || record.slot != slot
                    || record.term != term
                    || record.index != index
            }) {
                return Err(PayloadError::CorruptRepresentation);
            }
            self.transport
                .delete(*voter, hash, group, configuration_generation, request, slot)
                .await?;
        }
        Ok(())
    }
}

/// Durable payload operations used by a group independently of its transport.
#[async_trait::async_trait]
pub trait PayloadStore: Send + Sync {
    /// Publishes the uniform committed voter allocation for subsequent staging.
    async fn set_voters(&self, voters: Vec<u64>) -> Result<(), PayloadError>;
    /// Stages representations before returning a certificate eligible for Raft.
    async fn stage(
        &self,
        request: RequestId,
        group: &str,
        configuration_generation: u64,
        mode: ReplicationMode,
        body: &[u8],
        holders: &[u64],
    ) -> Result<StagedPayload, PayloadError>;

    /// Reconstructs and verifies a committed logical body.
    async fn reconstruct(&self, read: ReconstructRequest<'_>) -> Result<Bytes, PayloadError>;

    /// Re-encodes one verified immutable body into the exact target voter allocation.
    ///
    /// The caller commits the returned certificate before changing membership.
    async fn repair(&self, repair: RepairRequest<'_>) -> Result<PayloadCertificate, PayloadError>;

    /// Rewrites every certified allocation record with its committed Raft identity.
    async fn seal(&self, seal: SealRequest<'_>) -> Result<(), PayloadError>;

    /// Deletes every holder copy for one exact committed allocation idempotently.
    async fn release(&self, release: ReleaseRequest<'_>) -> Result<(), PayloadError>;
}

/// One voter's persistent payload-representation namespace.
pub struct FilePayloadReplica {
    node: u64,
    root: PathBuf,
}

impl FilePayloadReplica {
    /// Opens the payload directory owned by one stable consensus voter.
    pub fn open(node: u64, root: PathBuf) -> Result<Self, PayloadError> {
        if node == 0 {
            return Err(PayloadError::InvalidGeometry);
        }
        fs::create_dir_all(root.join("payloads"))?;
        sync_dir(&root)?;
        Ok(Self { node, root })
    }

    /// Returns this replica's stable voter identity.
    fn node(&self) -> u64 {
        self.node
    }

    /// Persists a canonical representation before acknowledging its holder.
    fn store(&self, representation: &PayloadRepresentation) -> Result<(), PayloadError> {
        let PayloadRepresentation {
            hash,
            group,
            configuration_generation,
            mode,
            request,
            length,
            slot,
            term,
            index,
            bytes,
        } = representation;
        let directory = self
            .root
            .join("payloads")
            .join(hex_hash(*hash))
            .join(hex_hash(Sha256::digest(group.as_bytes()).into()))
            .join(configuration_generation.to_string())
            .join(format!("{:032x}", request.as_u128()));
        fs::create_dir_all(&directory)?;
        let group_bytes = group.as_bytes();
        let group_length =
            u32::try_from(group_bytes.len()).map_err(|_| PayloadError::CorruptRepresentation)?;
        let mut record = Vec::with_capacity(93 + group_bytes.len() + bytes.len());
        record.extend_from_slice(&group_length.to_be_bytes());
        record.extend_from_slice(group_bytes);
        record.extend_from_slice(hash);
        record.extend_from_slice(&configuration_generation.to_be_bytes());
        record.push(match mode {
            ReplicationMode::Coded => 0,
            ReplicationMode::Complete => 1,
        });
        record.extend_from_slice(&request.as_u128().to_be_bytes());
        record.extend_from_slice(&(*length as u64).to_be_bytes());
        record.extend_from_slice(&(*slot as u64).to_be_bytes());
        record.extend_from_slice(&term.to_be_bytes());
        record.extend_from_slice(&index.to_be_bytes());
        record.extend_from_slice(bytes);
        let mut framed = crc32c(&record).to_be_bytes().to_vec();
        framed.extend_from_slice(&record);
        atomic_write(&directory.join(self.node.to_string()), &framed)?;
        sync_dir(&directory)?;
        sync_dir(&self.root.join("payloads"))
    }

    /// Loads one checksummed representation and its encoded slot.
    fn load(
        &self,
        hash: [u8; 32],
        group: &str,
        configuration_generation: u64,
        request: RequestId,
    ) -> Result<StoredRepresentation, PayloadError> {
        let path = self
            .root
            .join("payloads")
            .join(hex_hash(hash))
            .join(hex_hash(Sha256::digest(group.as_bytes()).into()))
            .join(configuration_generation.to_string())
            .join(format!("{:032x}", request.as_u128()))
            .join(self.node.to_string());
        let mut file = OpenOptions::new().read(true).open(path)?;
        let mut framed = Vec::new();
        file.read_to_end(&mut framed)?;
        if framed.len() < 97 {
            return Err(PayloadError::CorruptRepresentation);
        }
        let checksum = u32::from_be_bytes(
            framed[..4]
                .try_into()
                .map_err(|_| PayloadError::CorruptRepresentation)?,
        );
        let record = &framed[4..];
        if crc32c(record) != checksum {
            return Err(PayloadError::CorruptRepresentation);
        }
        let group_length = u32::from_be_bytes(
            record[0..4]
                .try_into()
                .map_err(|_| PayloadError::CorruptRepresentation)?,
        ) as usize;
        let fixed_start = 4usize
            .checked_add(group_length)
            .ok_or(PayloadError::CorruptRepresentation)?;
        let payload_start = fixed_start
            .checked_add(89)
            .ok_or(PayloadError::CorruptRepresentation)?;
        if record.len() < payload_start {
            return Err(PayloadError::CorruptRepresentation);
        }
        let group = std::str::from_utf8(&record[4..fixed_start])
            .map_err(|_| PayloadError::CorruptRepresentation)?
            .to_owned();
        let hash: [u8; 32] = record[fixed_start..fixed_start + 32]
            .try_into()
            .map_err(|_| PayloadError::CorruptRepresentation)?;
        let configuration_generation = u64::from_be_bytes(
            record[fixed_start + 32..fixed_start + 40]
                .try_into()
                .map_err(|_| PayloadError::CorruptRepresentation)?,
        );
        let mode = match record[fixed_start + 40] {
            0 => ReplicationMode::Coded,
            1 => ReplicationMode::Complete,
            _ => return Err(PayloadError::CorruptRepresentation),
        };
        let request = RequestId::from_u128(u128::from_be_bytes(
            record[fixed_start + 41..fixed_start + 57]
                .try_into()
                .map_err(|_| PayloadError::CorruptRepresentation)?,
        ));
        let length = u64::from_be_bytes(
            record[fixed_start + 57..fixed_start + 65]
                .try_into()
                .map_err(|_| PayloadError::CorruptRepresentation)?,
        ) as usize;
        let slot = u64::from_be_bytes(
            record[fixed_start + 65..fixed_start + 73]
                .try_into()
                .map_err(|_| PayloadError::CorruptRepresentation)?,
        ) as usize;
        let term = u64::from_be_bytes(
            record[fixed_start + 73..fixed_start + 81]
                .try_into()
                .map_err(|_| PayloadError::CorruptRepresentation)?,
        );
        let index = u64::from_be_bytes(
            record[fixed_start + 81..fixed_start + 89]
                .try_into()
                .map_err(|_| PayloadError::CorruptRepresentation)?,
        );
        Ok(StoredRepresentation {
            group,
            configuration_generation,
            mode,
            hash,
            request,
            length,
            slot,
            term,
            index,
            bytes: Bytes::copy_from_slice(&record[payload_start..]),
        })
    }

    /// Deletes this voter's one exact representation, treating an absent file as released.
    fn delete(
        &self,
        hash: [u8; 32],
        group: &str,
        configuration_generation: u64,
        request: RequestId,
    ) -> Result<(), PayloadError> {
        let path = self
            .root
            .join("payloads")
            .join(hex_hash(hash))
            .join(hex_hash(Sha256::digest(group.as_bytes()).into()))
            .join(configuration_generation.to_string())
            .join(format!("{:032x}", request.as_u128()))
            .join(self.node.to_string());
        match fs::remove_file(&path) {
            Ok(()) => {
                let parent = path.parent().ok_or(PayloadError::CorruptRepresentation)?;
                sync_dir(parent)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(PayloadError::Io(error)),
        }
    }
}

/// A configured set of durable voter payload stores.
pub struct PayloadSet {
    k: usize,
    m: usize,
    replicas: BTreeMap<u64, FilePayloadReplica>,
}

impl PayloadSet {
    /// Builds a payload set whose replica count exactly matches `k + m`.
    pub fn new(
        k: usize,
        m: usize,
        replicas: Vec<FilePayloadReplica>,
    ) -> Result<Self, PayloadError> {
        let mapped: BTreeMap<_, _> = replicas
            .into_iter()
            .map(|replica| (replica.node(), replica))
            .collect();
        if k == 0 || m == 0 || k.checked_add(m) != Some(mapped.len()) {
            return Err(PayloadError::InvalidGeometry);
        }
        Ok(Self {
            k,
            m,
            replicas: mapped,
        })
    }

    /// Durably stages coded shards or complete copies and returns their proof.
    pub fn stage_local(
        &self,
        request: RequestId,
        group: &str,
        configuration_generation: u64,
        mode: ReplicationMode,
        body: &[u8],
        holders: &[u64],
    ) -> Result<StagedPayload, PayloadError> {
        let certificate = PayloadCertificate::with_voters(
            mode,
            self.k,
            self.m,
            self.replicas.keys().copied().collect(),
            holders.to_vec(),
        )
        .map_err(|error| PayloadError::InsufficientDurability {
            required: error.required(),
            durable: holders.iter().copied().collect::<BTreeSet<_>>().len(),
        })?;
        if !certificate
            .holders()
            .iter()
            .all(|node| self.replicas.contains_key(node))
        {
            return Err(PayloadError::UnknownHolder);
        }
        let hash: [u8; 32] = Sha256::digest(body).into();
        let representations = match mode {
            ReplicationMode::Coded => {
                encode(self.k, self.m, body).map_err(|_| PayloadError::ReconstructionUnavailable)?
            }
            ReplicationMode::Complete => self
                .replicas
                .keys()
                .map(|_| Bytes::copy_from_slice(body))
                .collect(),
        };
        for node in certificate.holders() {
            let slot = self
                .replicas
                .keys()
                .position(|candidate| candidate == node)
                .ok_or(PayloadError::UnknownHolder)?;
            self.replicas[node].store(&PayloadRepresentation {
                term: 0,
                index: 0,
                group: group.to_owned(),
                configuration_generation,
                mode,
                hash,
                request,
                length: body.len(),
                slot,
                bytes: representations[slot].clone(),
            })?;
        }
        Ok(StagedPayload {
            hash,
            length: body.len() as u64,
            certificate,
        })
    }

    /// Reconstructs from all currently readable certified holders.
    pub fn reconstruct_local(
        &self,
        hash: [u8; 32],
        group: &str,
        configuration_generation: u64,
        request: RequestId,
        certificate: &PayloadCertificate,
    ) -> Result<Bytes, PayloadError> {
        self.reconstruct_from(
            hash,
            group,
            configuration_generation,
            request,
            certificate,
            certificate.holders(),
        )
    }

    /// Reconstructs from a selected live subset without exposing corrupt bytes.
    pub fn reconstruct_from(
        &self,
        hash: [u8; 32],
        group: &str,
        configuration_generation: u64,
        request: RequestId,
        certificate: &PayloadCertificate,
        live: &[u64],
    ) -> Result<Bytes, PayloadError> {
        let records: Vec<_> = certificate
            .holders()
            .iter()
            .filter(|node| live.contains(node))
            .filter_map(|node| {
                self.replicas
                    .get(node)?
                    .load(hash, group, configuration_generation, request)
                    .ok()
            })
            .collect();
        let first = records
            .first()
            .ok_or(PayloadError::ReconstructionUnavailable)?;
        let body = match certificate.mode() {
            ReplicationMode::Complete => first.bytes.to_vec(),
            ReplicationMode::Coded => {
                let parts = records
                    .iter()
                    .map(|record| (record.slot, record.bytes.clone()))
                    .collect::<Vec<_>>();
                decode(certificate.k(), certificate.m(), first.length, &parts)
                    .map_err(|_| PayloadError::ReconstructionUnavailable)?
            }
        };
        if body.len() != first.length || <[u8; 32]>::from(Sha256::digest(&body)) != hash {
            return Err(PayloadError::CorruptRepresentation);
        }
        Ok(Bytes::from(body))
    }
}

#[async_trait::async_trait]
impl PayloadStore for PayloadSet {
    async fn set_voters(&self, voters: Vec<u64>) -> Result<(), PayloadError> {
        if voters.iter().copied().collect::<BTreeSet<_>>()
            != self.replicas.keys().copied().collect()
        {
            return Err(PayloadError::InvalidGeometry);
        }
        Ok(())
    }

    async fn stage(
        &self,
        request: RequestId,
        group: &str,
        configuration_generation: u64,
        mode: ReplicationMode,
        body: &[u8],
        holders: &[u64],
    ) -> Result<StagedPayload, PayloadError> {
        self.stage_local(
            request,
            group,
            configuration_generation,
            mode,
            body,
            holders,
        )
    }

    async fn reconstruct(&self, read: ReconstructRequest<'_>) -> Result<Bytes, PayloadError> {
        let ReconstructRequest {
            hash,
            group,
            configuration_generation,
            request,
            length,
            term,
            index,
            certificate,
        } = read;
        let records: Vec<_> = certificate
            .holders()
            .iter()
            .map(|node| {
                self.replicas
                    .get(node)
                    .ok_or(PayloadError::UnknownHolder)?
                    .load(hash, group, configuration_generation, request)
                    .map_err(|error| match error {
                        PayloadError::Io(io_error)
                            if io_error.kind() == std::io::ErrorKind::NotFound =>
                        {
                            PayloadError::ReconstructionUnavailable
                        }
                        other => other,
                    })
            })
            .collect::<Result<_, _>>()?;
        if records.iter().any(|record| {
            record.term != term
                || record.index != index
                || record.request != request
                || record.group != group
                || record.configuration_generation != configuration_generation
                || record.hash != hash
                || record.length as u64 != length
                || record.mode != certificate.mode()
        }) {
            return Err(PayloadError::ReconstructionUnavailable);
        }
        self.reconstruct_local(hash, group, configuration_generation, request, certificate)
    }

    async fn repair(&self, repair: RepairRequest<'_>) -> Result<PayloadCertificate, PayloadError> {
        let RepairRequest {
            hash,
            group,
            source_configuration_generation,
            target_configuration_generation,
            request,
            length,
            source_term,
            source_index,
            certificate,
            target_voters,
            ..
        } = repair;
        if target_voters
            .iter()
            .any(|node| !self.replicas.contains_key(node))
        {
            return Err(PayloadError::UnknownHolder);
        }
        let body = self
            .reconstruct(ReconstructRequest {
                hash,
                group,
                configuration_generation: source_configuration_generation,
                request,
                length,
                term: source_term,
                index: source_index,
                certificate,
            })
            .await?;
        self.stage_local(
            request,
            group,
            target_configuration_generation,
            certificate.mode(),
            &body,
            &target_voters,
        )
        .map(|staged| staged.certificate)
    }

    async fn seal(&self, seal: SealRequest<'_>) -> Result<(), PayloadError> {
        let SealRequest {
            hash,
            group,
            configuration_generation,
            request,
            term,
            index,
            certificate,
        } = seal;
        if term == 0 || index == 0 {
            return Err(PayloadError::CorruptRepresentation);
        }
        for node in certificate.holders() {
            let replica = self.replicas.get(node).ok_or(PayloadError::UnknownHolder)?;
            let record = replica
                .load(hash, group, configuration_generation, request)
                .map_err(|error| match error {
                    PayloadError::Io(io_error)
                        if io_error.kind() == std::io::ErrorKind::NotFound =>
                    {
                        PayloadError::ReconstructionUnavailable
                    }
                    other => other,
                })?;
            let slot = certificate
                .voters()
                .iter()
                .position(|voter| voter == node)
                .ok_or(PayloadError::UnknownHolder)?;
            if record.hash != hash
                || record.request != request
                || record.group != group
                || record.configuration_generation != configuration_generation
                || record.mode != certificate.mode()
                || record.slot != slot
                || (record.term != 0 && (record.term != term || record.index != index))
            {
                return Err(PayloadError::CorruptRepresentation);
            }
            if record.term == term && record.index == index {
                continue;
            }
            replica.store(&PayloadRepresentation {
                term,
                index,
                group: record.group,
                configuration_generation: record.configuration_generation,
                mode: record.mode,
                hash,
                request,
                length: record.length,
                slot: record.slot,
                bytes: record.bytes,
            })?;
        }
        Ok(())
    }

    async fn release(&self, release: ReleaseRequest<'_>) -> Result<(), PayloadError> {
        let ReleaseRequest {
            hash,
            group,
            configuration_generation,
            request,
            length,
            term,
            index,
            certificate,
        } = release;
        if certificate.k() != self.k || certificate.m() != self.m {
            return Err(PayloadError::CorruptRepresentation);
        }
        for node in certificate.holders() {
            let replica = self.replicas.get(node).ok_or(PayloadError::UnknownHolder)?;
            match replica.load(hash, group, configuration_generation, request) {
                Ok(record) => {
                    let slot = certificate
                        .voters()
                        .iter()
                        .position(|voter| voter == node)
                        .ok_or(PayloadError::UnknownHolder)?;
                    if record.group != group
                        || record.configuration_generation != configuration_generation
                        || record.mode != certificate.mode()
                        || record.hash != hash
                        || record.request != request
                        || record.length as u64 != length
                        || record.slot != slot
                        || record.term != term
                        || record.index != index
                    {
                        return Err(PayloadError::CorruptRepresentation);
                    }
                }
                Err(PayloadError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
            replica.delete(hash, group, configuration_generation, request)?;
        }
        Ok(())
    }
}

/// Immutable payload metadata ready to enter the Raft header log.
#[derive(Clone, Debug)]
pub struct StagedPayload {
    hash: [u8; 32],
    length: u64,
    certificate: PayloadCertificate,
}

impl StagedPayload {
    /// Returns the logical payload hash bound into the header.
    pub fn hash(&self) -> [u8; 32] {
        self.hash
    }

    /// Returns the exact logical payload length.
    pub fn length(&self) -> u64 {
        self.length
    }

    /// Returns the durable-holder proof that Raft may commit.
    pub fn certificate(&self) -> &PayloadCertificate {
        &self.certificate
    }
}

#[derive(Clone, Debug)]
struct StoredRepresentation {
    group: String,
    configuration_generation: u64,
    mode: ReplicationMode,
    hash: [u8; 32],
    request: RequestId,
    length: usize,
    slot: usize,
    term: u64,
    index: u64,
    bytes: Bytes,
}

/// A failure to create or consume a durable payload certificate.
#[derive(Debug, thiserror::Error)]
pub enum PayloadError {
    /// The configured voter/coding geometry is impossible.
    #[error("invalid payload coding geometry")]
    InvalidGeometry,
    /// The requested holder set cannot survive every legal successor election.
    #[error("payload requires {required} durable holders but has {durable}")]
    InsufficientDurability {
        /// Required distinct holder count.
        required: usize,
        /// Supplied distinct holder count.
        durable: usize,
    },
    /// A certificate named a voter outside the committed configuration.
    #[error("payload certificate names an unknown holder")]
    UnknownHolder,
    /// Too few verified representations remain to recover the body.
    #[error("payload cannot be reconstructed")]
    ReconstructionUnavailable,
    /// A representation failed its checksum, length, or logical hash check.
    #[error("payload representation is corrupt")]
    CorruptRepresentation,
    /// A peer transport refused or lost a representation operation.
    #[error("payload peer transport failed: {0}")]
    Transport(String),
    /// Durable local storage failed before acknowledgement.
    #[error("payload storage failed: {0}")]
    Io(#[from] std::io::Error),
}

impl PayloadError {
    /// Returns the durability threshold for an insufficient-holder error.
    pub fn required(&self) -> usize {
        match self {
            Self::InsufficientDurability { required, .. } => *required,
            _ => 0,
        }
    }
}

fn hex_hash(hash: [u8; 32]) -> String {
    hash.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn atomic_write(path: &Path, bytes: &[u8]) -> Result<(), PayloadError> {
    let temporary = path.with_extension("tmp");
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&temporary)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    fs::rename(&temporary, path)?;
    sync_dir(path.parent().ok_or(PayloadError::InvalidGeometry)?)
}

fn sync_dir(path: &Path) -> Result<(), PayloadError> {
    OpenOptions::new().read(true).open(path)?.sync_all()?;
    Ok(())
}
