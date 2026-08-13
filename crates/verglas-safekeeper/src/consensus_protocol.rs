//! Thin Neon-facing WAL protocol over the universal Verglas consensus group.
//!
//! These messages contain no vote, donor, quorum, or election operations.
//! Compute supplies stable retry identities and writer epochs; Verglas returns
//! only results that the timeline group's Raft state machine has applied.

use std::sync::Arc;

use bytes::Bytes;
use verglas_consensus::{ConsensusGroup, GroupError, RequestId};

const ACQUIRE_WRITER: u8 = 1;
const APPEND: u8 = 2;
const READ_WAL: u8 = 3;
const RELEASE_WRITER: u8 = 4;
const STATUS: u8 = 5;
const OPEN_TIMELINE: u8 = 6;
const APPLIED: u8 = 1;
const WAL_BYTES: u8 = 2;
const WAL_STATUS: u8 = 3;

/// Reed-Solomon geometry and coded durability threshold for one timeline group.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AppendGeometry {
    /// Data shards required for reconstruction.
    pub k: usize,
    /// Parity shards stored beside the data shards.
    pub m: usize,
    /// Distinct durable holders required before a coded header may commit.
    pub w: usize,
}

impl AppendGeometry {
    /// Validates the basic coding and holder bounds.
    pub fn new(k: usize, m: usize, w: usize) -> Result<Self, WalProtocolError> {
        if k == 0 || w < k || w > k.saturating_add(m) {
            return Err(WalProtocolError::InvalidGeometry { k, m, w });
        }
        Ok(Self { k, m, w })
    }

    /// Returns the complete voter count `k + m`.
    pub fn total(self) -> usize {
        self.k + self.m
    }
}

/// A request accepted by any Verglas WAL ingress and routed to the group leader.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WalRequest {
    /// Establishes the immutable initial PostgreSQL WAL position.
    OpenTimeline {
        /// Exact retry identity.
        request_id: u128,
        /// First byte accepted by this timeline.
        start_lsn: u64,
    },
    /// Acquires the next exclusive writer epoch.
    AcquireWriter {
        /// Exact retry identity.
        request_id: u128,
        /// Stable compute identity for diagnostics.
        writer: String,
    },
    /// Appends one exact contiguous WAL byte range.
    Append {
        /// Exact retry identity.
        request_id: u128,
        /// Committed writer fence.
        writer_epoch: u64,
        /// First LSN represented by `payload`.
        start_lsn: u64,
        /// Raw PostgreSQL WAL bytes.
        payload: Vec<u8>,
    },
    /// Reads only committed WAL at or beyond an applied-index fence.
    ReadWal {
        /// Inclusive first LSN.
        from: u64,
        /// Exclusive final LSN.
        to: u64,
        /// Minimum group index the serving replica must have applied.
        minimum_index: u64,
    },
    /// Reads the committed timeline tail after a linearizable fence.
    Status {
        /// Minimum group index the serving replica must have applied.
        minimum_index: u64,
    },
    /// Explicitly releases one live writer epoch.
    ReleaseWriter {
        /// Exact retry identity.
        request_id: u128,
        /// Epoch being released.
        writer_epoch: u64,
    },
}

/// A consensus-applied WAL response returned to Neon or a pageserver.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WalResponse {
    /// A writer transition, append, or archive checkpoint committed.
    Applied {
        /// Raft group index that applied the command.
        index: u64,
        /// Current writer epoch when relevant.
        writer_epoch: Option<u64>,
        /// Applied WAL tail when relevant.
        wal_end: Option<u64>,
        /// Applied archive watermark when relevant.
        archive_lsn: Option<u64>,
    },
    /// Committed WAL bytes satisfying the requested read fence.
    WalBytes {
        /// Exact bytes in the requested half-open LSN range.
        payload: Vec<u8>,
    },
    /// Current consensus-applied WAL and archive boundaries.
    Status {
        /// Applied index that fenced this response.
        index: u64,
        /// Exclusive committed WAL tail.
        wal_end: u64,
        /// Exclusive verified archive watermark.
        archive_lsn: u64,
    },
}

impl WalRequest {
    /// Encodes one canonical, length-delimited request frame for the C client.
    pub fn encode(&self) -> Result<Vec<u8>, WalWireError> {
        let mut bytes = Vec::new();
        match self {
            Self::OpenTimeline {
                request_id,
                start_lsn,
            } => {
                bytes.push(OPEN_TIMELINE);
                bytes.extend_from_slice(&request_id.to_be_bytes());
                bytes.extend_from_slice(&start_lsn.to_be_bytes());
            }
            Self::AcquireWriter { request_id, writer } => {
                bytes.push(ACQUIRE_WRITER);
                bytes.extend_from_slice(&request_id.to_be_bytes());
                put_bytes(&mut bytes, writer.as_bytes())?;
            }
            Self::Append {
                request_id,
                writer_epoch,
                start_lsn,
                payload,
            } => {
                bytes.push(APPEND);
                bytes.extend_from_slice(&request_id.to_be_bytes());
                bytes.extend_from_slice(&writer_epoch.to_be_bytes());
                bytes.extend_from_slice(&start_lsn.to_be_bytes());
                put_bytes(&mut bytes, payload)?;
            }
            Self::ReadWal {
                from,
                to,
                minimum_index,
            } => {
                bytes.push(READ_WAL);
                bytes.extend_from_slice(&from.to_be_bytes());
                bytes.extend_from_slice(&to.to_be_bytes());
                bytes.extend_from_slice(&minimum_index.to_be_bytes());
            }
            Self::ReleaseWriter {
                request_id,
                writer_epoch,
            } => {
                bytes.push(RELEASE_WRITER);
                bytes.extend_from_slice(&request_id.to_be_bytes());
                bytes.extend_from_slice(&writer_epoch.to_be_bytes());
            }
            Self::Status { minimum_index } => {
                bytes.push(STATUS);
                bytes.extend_from_slice(&minimum_index.to_be_bytes());
            }
        }
        Ok(bytes)
    }

    /// Decodes one complete request and rejects non-canonical trailing data.
    pub fn decode(bytes: &[u8]) -> Result<Self, WalWireError> {
        let mut input = Reader::new(bytes);
        let request = match input.u8()? {
            OPEN_TIMELINE => Self::OpenTimeline {
                request_id: input.u128()?,
                start_lsn: input.u64()?,
            },
            ACQUIRE_WRITER => Self::AcquireWriter {
                request_id: input.u128()?,
                writer: String::from_utf8(input.bytes()?.to_vec())
                    .map_err(|_| WalWireError::InvalidUtf8)?,
            },
            APPEND => Self::Append {
                request_id: input.u128()?,
                writer_epoch: input.u64()?,
                start_lsn: input.u64()?,
                payload: input.bytes()?.to_vec(),
            },
            READ_WAL => Self::ReadWal {
                from: input.u64()?,
                to: input.u64()?,
                minimum_index: input.u64()?,
            },
            RELEASE_WRITER => Self::ReleaseWriter {
                request_id: input.u128()?,
                writer_epoch: input.u64()?,
            },
            STATUS => Self::Status {
                minimum_index: input.u64()?,
            },
            tag => return Err(WalWireError::UnknownTag(tag)),
        };
        input.finish()?;
        Ok(request)
    }
}

impl WalResponse {
    /// Encodes one consensus-applied result or committed WAL range.
    pub fn encode(&self) -> Result<Vec<u8>, WalWireError> {
        let mut bytes = Vec::new();
        match self {
            Self::Applied {
                index,
                writer_epoch,
                wal_end,
                archive_lsn,
            } => {
                bytes.push(APPLIED);
                bytes.extend_from_slice(&index.to_be_bytes());
                put_optional_u64(&mut bytes, *writer_epoch);
                put_optional_u64(&mut bytes, *wal_end);
                put_optional_u64(&mut bytes, *archive_lsn);
            }
            Self::WalBytes { payload } => {
                bytes.push(WAL_BYTES);
                put_bytes(&mut bytes, payload)?;
            }
            Self::Status {
                index,
                wal_end,
                archive_lsn,
            } => {
                bytes.push(WAL_STATUS);
                bytes.extend_from_slice(&index.to_be_bytes());
                bytes.extend_from_slice(&wal_end.to_be_bytes());
                bytes.extend_from_slice(&archive_lsn.to_be_bytes());
            }
        }
        Ok(bytes)
    }

    /// Decodes one complete response and rejects malformed optional values.
    pub fn decode(bytes: &[u8]) -> Result<Self, WalWireError> {
        let mut input = Reader::new(bytes);
        let response = match input.u8()? {
            APPLIED => Self::Applied {
                index: input.u64()?,
                writer_epoch: input.optional_u64()?,
                wal_end: input.optional_u64()?,
                archive_lsn: input.optional_u64()?,
            },
            WAL_BYTES => Self::WalBytes {
                payload: input.bytes()?.to_vec(),
            },
            WAL_STATUS => Self::Status {
                index: input.u64()?,
                wal_end: input.u64()?,
                archive_lsn: input.u64()?,
            },
            tag => return Err(WalWireError::UnknownTag(tag)),
        };
        input.finish()?;
        Ok(response)
    }
}

/// A malformed or unrepresentable Neon transport frame.
#[derive(Debug, Eq, PartialEq, thiserror::Error)]
pub enum WalWireError {
    /// The frame ended before its declared fields did.
    #[error("truncated WAL protocol frame")]
    Truncated,
    /// A frame used an operation tag not defined by the protocol.
    #[error("unknown WAL protocol tag {0}")]
    UnknownTag(u8),
    /// A length cannot be represented by the fixed-width wire field.
    #[error("WAL protocol field is too large")]
    FieldTooLarge,
    /// A string field was not valid UTF-8.
    #[error("WAL protocol string is not UTF-8")]
    InvalidUtf8,
    /// A boolean presence marker was neither zero nor one.
    #[error("invalid WAL protocol presence marker {0}")]
    InvalidPresence(u8),
    /// Extra bytes followed an otherwise complete operation.
    #[error("trailing bytes in WAL protocol frame")]
    TrailingBytes,
}

fn put_bytes(output: &mut Vec<u8>, value: &[u8]) -> Result<(), WalWireError> {
    let len = u32::try_from(value.len()).map_err(|_| WalWireError::FieldTooLarge)?;
    output.extend_from_slice(&len.to_be_bytes());
    output.extend_from_slice(value);
    Ok(())
}

fn put_optional_u64(output: &mut Vec<u8>, value: Option<u64>) {
    match value {
        Some(value) => {
            output.push(1);
            output.extend_from_slice(&value.to_be_bytes());
        }
        None => output.push(0),
    }
}

struct Reader<'a> {
    remaining: &'a [u8],
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { remaining: bytes }
    }

    fn take(&mut self, count: usize) -> Result<&'a [u8], WalWireError> {
        if self.remaining.len() < count {
            return Err(WalWireError::Truncated);
        }
        let (value, remaining) = self.remaining.split_at(count);
        self.remaining = remaining;
        Ok(value)
    }

    fn u8(&mut self) -> Result<u8, WalWireError> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, WalWireError> {
        let bytes: [u8; 4] = self
            .take(4)?
            .try_into()
            .map_err(|_| WalWireError::Truncated)?;
        Ok(u32::from_be_bytes(bytes))
    }

    fn u64(&mut self) -> Result<u64, WalWireError> {
        let bytes: [u8; 8] = self
            .take(8)?
            .try_into()
            .map_err(|_| WalWireError::Truncated)?;
        Ok(u64::from_be_bytes(bytes))
    }

    fn u128(&mut self) -> Result<u128, WalWireError> {
        let bytes: [u8; 16] = self
            .take(16)?
            .try_into()
            .map_err(|_| WalWireError::Truncated)?;
        Ok(u128::from_be_bytes(bytes))
    }

    fn bytes(&mut self) -> Result<&'a [u8], WalWireError> {
        let count = usize::try_from(self.u32()?).map_err(|_| WalWireError::FieldTooLarge)?;
        self.take(count)
    }

    fn optional_u64(&mut self) -> Result<Option<u64>, WalWireError> {
        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.u64()?)),
            marker => Err(WalWireError::InvalidPresence(marker)),
        }
    }

    fn finish(self) -> Result<(), WalWireError> {
        if self.remaining.is_empty() {
            Ok(())
        } else {
            Err(WalWireError::TrailingBytes)
        }
    }
}

/// One ingress adapter for a resolved timeline consensus group.
pub struct ConsensusTimeline {
    group: Arc<ConsensusGroup>,
    coded_holders: Vec<u64>,
    majority_holders: Vec<u64>,
}

impl ConsensusTimeline {
    /// Binds one timeline to its committed coded and complete holder sets.
    pub fn new(
        group: Arc<ConsensusGroup>,
        coded_holders: Vec<u64>,
        majority_holders: Vec<u64>,
    ) -> Result<Self, WalProtocolError> {
        if coded_holders.is_empty() || majority_holders.is_empty() {
            return Err(WalProtocolError::MissingHolders);
        }
        Ok(Self {
            group,
            coded_holders,
            majority_holders,
        })
    }

    /// Routes one Neon operation through the sole timeline agreement boundary.
    pub async fn handle(&self, request: WalRequest) -> Result<WalResponse, WalProtocolError> {
        match request {
            WalRequest::OpenTimeline {
                request_id,
                start_lsn,
            } => {
                let response = self
                    .group
                    .open_timeline(
                        RequestId::from_u128(request_id),
                        start_lsn,
                        &self.majority_holders,
                    )
                    .await?;
                Ok(applied(response))
            }
            WalRequest::AcquireWriter { request_id, writer } => {
                let response = self
                    .group
                    .acquire_writer(
                        RequestId::from_u128(request_id),
                        &writer,
                        &self.majority_holders,
                    )
                    .await?;
                Ok(applied(response))
            }
            WalRequest::Append {
                request_id,
                writer_epoch,
                start_lsn,
                payload,
            } => {
                let response = self
                    .group
                    .append_wal(
                        RequestId::from_u128(request_id),
                        writer_epoch,
                        start_lsn,
                        Bytes::from(payload),
                        &self.coded_holders,
                    )
                    .await?;
                Ok(applied(response))
            }
            WalRequest::ReadWal {
                from,
                to,
                minimum_index,
            } => Ok(WalResponse::WalBytes {
                payload: self.group.read_wal(from, to, minimum_index).await?.to_vec(),
            }),
            WalRequest::Status { minimum_index } => {
                let state = self.group.wal_archive_state().await?;
                if state.applied_index() < minimum_index {
                    return Err(WalProtocolError::Consensus(GroupError::NotCommitted(
                        minimum_index,
                    )));
                }
                Ok(WalResponse::Status {
                    index: state.applied_index(),
                    wal_end: state.committed_end(),
                    archive_lsn: state.checkpointed_end(),
                })
            }
            WalRequest::ReleaseWriter {
                request_id,
                writer_epoch,
            } => {
                let response = self
                    .group
                    .release_writer(
                        RequestId::from_u128(request_id),
                        writer_epoch,
                        &self.majority_holders,
                    )
                    .await?;
                Ok(applied(response))
            }
        }
    }
}

/// A Neon WAL operation refused by consensus or malformed ingress wiring.
#[derive(Debug, thiserror::Error)]
pub enum WalProtocolError {
    /// Coding or durability threshold cannot be satisfied.
    #[error("invalid WAL geometry k={k}, m={m}, w={w}")]
    InvalidGeometry { k: usize, m: usize, w: usize },
    /// Timeline placement was not resolved from committed membership.
    #[error("timeline has no committed holder configuration")]
    MissingHolders,
    /// Consensus refused the request before acknowledgement.
    #[error(transparent)]
    Consensus(#[from] GroupError),
}

fn applied(response: verglas_consensus::RaftResponse) -> WalResponse {
    WalResponse::Applied {
        index: response.index,
        writer_epoch: response.writer_epoch,
        wal_end: response.wal_end,
        archive_lsn: response.archive_lsn,
    }
}
