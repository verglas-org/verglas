//! Optional single externally durable replica authority for early acknowledgement.

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::{CommitAuthority, CommitReceipt, Error, LeaseIdentity, Result, TransactionEnvelope};

/// One independently durable replica endpoint receiving exact canonical bytes.
#[async_trait]
pub trait ReplicaSink: Send + Sync {
    /// Persists one lease-fenced sequence before reporting success.
    async fn persist(
        &self,
        lease: &LeaseIdentity,
        sequence: u64,
        transaction_id: Uuid,
        canonical: &[u8],
    ) -> Result<()>;
}

/// Private Unix-socket client for one isolated replica microVM endpoint.
pub struct UnixReplicaSink {
    socket: PathBuf,
}

/// One exact canonical transaction returned for failover replay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaReplayEntry {
    sequence: u64,
    transaction_id: Uuid,
    canonical_envelope: Vec<u8>,
}

impl ReplicaReplayEntry {
    /// Returns the authority sequence persisted by the replica service.
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Returns the retry-stable transaction identity.
    pub fn transaction_id(&self) -> Uuid {
        self.transaction_id
    }

    /// Returns the exact canonical bytes needed for failover replay.
    pub fn canonical_envelope(&self) -> &[u8] {
        &self.canonical_envelope
    }
}

impl UnixReplicaSink {
    /// Creates a replica sink for one child-exclusive Unix socket.
    pub fn new(socket: impl Into<PathBuf>) -> Self {
        Self {
            socket: socket.into(),
        }
    }

    /// Retrieves a bounded exact transaction tail for worker failover.
    pub async fn replay(&self, after: u64, limit: usize) -> Result<Vec<ReplicaReplayEntry>> {
        let response = self
            .request(format!("REPLICA_REPLAY {after} {limit}\n"))
            .await?;
        if response.is_empty() {
            return Ok(Vec::new());
        }
        response
            .split(',')
            .map(|encoded| {
                let mut fields = encoded.splitn(3, ':');
                let sequence = fields
                    .next()
                    .and_then(|value| value.parse::<u64>().ok())
                    .ok_or_else(|| {
                        Error::Authority("invalid replica replay sequence".to_owned())
                    })?;
                let transaction_id = fields
                    .next()
                    .ok_or_else(|| Error::Authority("missing replica transaction ID".to_owned()))?
                    .parse::<Uuid>()
                    .map_err(|error| Error::Authority(error.to_string()))?;
                let canonical_envelope = hex::decode(fields.next().ok_or_else(|| {
                    Error::Authority("missing replica canonical envelope".to_owned())
                })?)
                .map_err(|error| Error::Authority(error.to_string()))?;
                Ok(ReplicaReplayEntry {
                    sequence,
                    transaction_id,
                    canonical_envelope,
                })
            })
            .collect()
    }

    /// Deletes a replica tail only after the worker explicitly supplies its lease and watermark.
    pub async fn clean(&self, lease: &LeaseIdentity, through: u64) -> Result<()> {
        self.request(format!(
            "REPLICA_CLEAN {} {} {}\n",
            lease.generation(),
            hex::encode(lease.token()),
            through
        ))
        .await?;
        Ok(())
    }

    /// Propagates managed archive and checkpoint coverage to the replica service.
    pub async fn cover(
        &self,
        lease: &LeaseIdentity,
        archived_through: u64,
        checkpointed_through: u64,
        checkpoint_identity: &str,
    ) -> Result<()> {
        self.request(format!(
            "REPLICA_COVER {} {} {} {} {}\n",
            lease.generation(),
            hex::encode(lease.token()),
            archived_through,
            checkpointed_through,
            hex::encode(checkpoint_identity),
        ))
        .await?;
        Ok(())
    }

    /// Executes one private replica command and strips its successful response framing.
    async fn request(&self, command: String) -> Result<String> {
        let mut stream = UnixStream::connect(&self.socket)
            .await
            .map_err(|error| Error::Authority(error.to_string()))?;
        stream
            .write_all(command.as_bytes())
            .await
            .map_err(|error| Error::Authority(error.to_string()))?;
        let mut response = String::new();
        BufReader::new(stream)
            .read_line(&mut response)
            .await
            .map_err(|error| Error::Authority(error.to_string()))?;
        if response == "OK\n" {
            return Ok(String::new());
        }
        if let Some(payload) = response
            .strip_prefix("OK ")
            .and_then(|value| value.strip_suffix('\n'))
        {
            return Ok(payload.to_owned());
        }
        Err(Error::Authority(format!(
            "replica endpoint rejected request: {}",
            response.trim()
        )))
    }
}

#[async_trait]
impl ReplicaSink for UnixReplicaSink {
    /// Sends one exact lease-fenced canonical envelope and waits for pager durability.
    async fn persist(
        &self,
        lease: &LeaseIdentity,
        sequence: u64,
        transaction_id: Uuid,
        canonical: &[u8],
    ) -> Result<()> {
        let mut stream = UnixStream::connect(&self.socket)
            .await
            .map_err(|error| Error::Authority(error.to_string()))?;
        let command = format!(
            "REPLICA_APPLY {} {} {} {} {}\n",
            lease.generation(),
            hex::encode(lease.token()),
            sequence,
            transaction_id,
            hex::encode(canonical)
        );
        stream
            .write_all(command.as_bytes())
            .await
            .map_err(|error| Error::Authority(error.to_string()))?;
        let mut response = String::new();
        BufReader::new(stream)
            .read_line(&mut response)
            .await
            .map_err(|error| Error::Authority(error.to_string()))?;
        if response == "OK\n" {
            Ok(())
        } else {
            Err(Error::Authority(format!(
                "replica endpoint rejected persistence: {}",
                response.trim()
            )))
        }
    }
}

/// Commits through one replica service whose own durability is deployment-defined.
pub struct ReplicaCommitAuthority {
    do_id: String,
    lease: LeaseIdentity,
    sequence: Mutex<u64>,
    replica: Arc<dyn ReplicaSink>,
}

impl ReplicaCommitAuthority {
    /// Binds one configured replica service at a recovered sequence.
    pub fn new(
        do_id: impl Into<String>,
        lease: LeaseIdentity,
        sequence: u64,
        replica: Arc<dyn ReplicaSink>,
    ) -> Self {
        Self {
            do_id: do_id.into(),
            lease,
            sequence: Mutex::new(sequence),
            replica,
        }
    }
}

#[async_trait]
impl CommitAuthority for ReplicaCommitAuthority {
    /// Sends exact bytes once and advances only after the replica service ACKs.
    async fn commit(&self, envelope: &TransactionEnvelope) -> Result<CommitReceipt> {
        if envelope.do_id() != self.do_id {
            return Err(Error::WrongDo {
                expected: self.do_id.clone(),
                actual: envelope.do_id().to_owned(),
            });
        }
        let canonical = envelope.canonical_bytes()?;
        let mut current = self.sequence.lock().await;
        if envelope.base_commit_sequence() != *current {
            return Err(Error::Authority(format!(
                "transaction base {} does not match replica sequence {}",
                envelope.base_commit_sequence(),
                *current
            )));
        }
        let sequence = current.saturating_add(1);
        self.replica
            .persist(&self.lease, sequence, envelope.transaction_id(), &canonical)
            .await?;
        *current = sequence;
        Ok(CommitReceipt::new(sequence, envelope.transaction_id()))
    }
}
