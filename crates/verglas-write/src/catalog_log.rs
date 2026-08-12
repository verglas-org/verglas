//! Quorum-durable catalog mutation log over the existing EC fragment plane.
//!
//! Lakekeeper supplies the authoritative order. Verglas persists each compact
//! pointer mutation across the cache ring and exposes a quorum-read committed
//! tail so query nodes can catch up or fail closed before serving strong reads.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use verglas_cache::writeback_codec::{Fragment, Geometry, encode, reassemble};
pub use verglas_catalog::CatalogMutation;
use verglas_cluster::fragments::{FragmentKey, FragmentRecord};
use verglas_core::CacheKey;
use verglas_core::node::NodeId;
use verglas_core::ring::rendezvous_hash;

use crate::{FragmentTransport, LiveMembership};

const MINIMUM_RING_NODES: usize = 3;
const TAIL_INDEX: usize = 0;

/// Proof returned after the EC fragments and committed-tail copies are durable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogMutationAck {
    /// Applied outbox sequence.
    pub sequence: u64,
    /// Applied idempotency id.
    pub event_id: String,
}

/// A catalog-log append or quorum-read failure.
#[derive(Debug, thiserror::Error)]
pub enum CatalogLogError {
    /// Too few distinct ring members accepted or returned matching durable state.
    #[error("catalog quorum unavailable: required {required}, available {available}")]
    QuorumUnavailable {
        /// Distinct nodes required by the operation.
        required: usize,
        /// Distinct successful or agreeing nodes observed.
        available: usize,
    },
    /// Delivery attempted to regress the committed sequence.
    #[error("stale catalog sequence {received}; committed sequence is {committed}")]
    StaleSequence {
        /// Sequence supplied by Lakekeeper.
        received: u64,
        /// Current quorum-committed sequence.
        committed: u64,
    },
    /// One sequence or event id was reused with different mutation content.
    #[error("catalog event conflicts with committed sequence {sequence}")]
    Conflict {
        /// Conflicting committed sequence.
        sequence: u64,
    },
    /// JSON framing of a durable record failed.
    #[error("catalog log encoding failed: {0}")]
    Encoding(String),
    /// Reed–Solomon encoding or reconstruction failed.
    #[error("catalog log EC failed: {0}")]
    Codec(String),
    /// A durable record was malformed or violated log ordering.
    #[error("catalog log is corrupt: {0}")]
    Corrupt(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Placement {
    /// Fragment index in the EC geometry.
    index: usize,
    /// Ring node holding the fragment.
    node: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CommitRef {
    /// Mutation sequence represented by this commit.
    sequence: u64,
    /// Mutation event id represented by this commit.
    event_id: String,
    /// Deterministic fragment object id.
    object_id: String,
    /// Data-fragment count.
    k: usize,
    /// Parity-fragment count.
    m: usize,
    /// Per-fragment stripe chunk.
    chunk: usize,
    /// Unpadded JSON record length.
    object_len: u64,
    /// Distinct fragment placements.
    placements: Vec<Placement>,
}

#[derive(Debug, Serialize, Deserialize)]
struct StoredMutation {
    /// The mutation committed at this log position.
    mutation: CatalogMutation,
    /// Previous committed position, forming a replayable backward chain.
    previous: Option<CommitRef>,
}

/// Append-only EC catalog log shared by one tenant cache ring.
pub struct EcCatalogLog {
    scope: String,
    transport: Arc<dyn FragmentTransport>,
    membership: Arc<dyn LiveMembership>,
    append_lock: Mutex<()>,
}

impl EcCatalogLog {
    /// Builds a catalog log over the cache node's existing fragment plane.
    pub fn new(
        scope: impl Into<String>,
        transport: Arc<dyn FragmentTransport>,
        membership: Arc<dyn LiveMembership>,
    ) -> Self {
        Self {
            scope: scope.into(),
            transport,
            membership,
            append_lock: Mutex::new(()),
        }
    }

    /// Appends one ordered mutation and returns only after the ring commit.
    pub async fn append(
        &self,
        mutation: CatalogMutation,
    ) -> Result<CatalogMutationAck, CatalogLogError> {
        let _guard = self.append_lock.lock().await;
        let nodes = self.ring_nodes()?;
        let previous = self.latest_ref(&nodes).await?;
        if let Some(committed) = &previous {
            if mutation.sequence == committed.sequence && mutation.event_id == committed.event_id {
                let stored = self.read_commit(committed).await?;
                if stored.mutation != mutation {
                    return Err(CatalogLogError::Conflict {
                        sequence: mutation.sequence,
                    });
                }
                return Ok(CatalogMutationAck {
                    sequence: mutation.sequence,
                    event_id: mutation.event_id,
                });
            }
            if mutation.sequence <= committed.sequence {
                return Err(CatalogLogError::StaleSequence {
                    received: mutation.sequence,
                    committed: committed.sequence,
                });
            }
        }

        let stored = StoredMutation {
            mutation: mutation.clone(),
            previous,
        };
        let body = serde_json::to_vec(&stored)
            .map_err(|error| CatalogLogError::Encoding(error.to_string()))?;
        let (k, m, write_quorum) = ring_geometry(nodes.len());
        let encoded =
            encode(k, m, &body).map_err(|error| CatalogLogError::Codec(error.to_string()))?;
        let object_id = self.object_id(&mutation);
        let ordered = placement_order(&object_id, &nodes);
        let mut placements = Vec::with_capacity(encoded.fragments.len());
        for (fragment, node) in encoded.fragments.iter().zip(&ordered) {
            if !self
                .transport
                .has_headroom(node, fragment.bytes.len() as u64)
                .await
            {
                break;
            }
            let record = FragmentRecord::new(
                FragmentKey {
                    object_id: object_id.clone(),
                    index: fragment.index,
                },
                fragment.bytes.clone(),
            );
            if self.transport.place(node, record).await.is_err() {
                break;
            }
            placements.push(Placement {
                index: fragment.index,
                node: node.as_str().to_owned(),
            });
        }
        if placements.len() < write_quorum {
            self.delete_placements(&object_id, &placements).await;
            return Err(CatalogLogError::QuorumUnavailable {
                required: write_quorum,
                available: placements.len(),
            });
        }

        let commit = CommitRef {
            sequence: mutation.sequence,
            event_id: mutation.event_id.clone(),
            object_id,
            k: encoded.geometry.k,
            m: encoded.geometry.m,
            chunk: encoded.geometry.chunk,
            object_len: encoded.object_len,
            placements,
        };
        self.publish_tail(&nodes, &commit).await?;
        Ok(CatalogMutationAck {
            sequence: mutation.sequence,
            event_id: mutation.event_id,
        })
    }

    /// Returns the newest sequence that a read quorum agrees is committed.
    pub async fn committed_sequence(&self) -> Result<u64, CatalogLogError> {
        let nodes = self.ring_nodes()?;
        Ok(self
            .latest_ref(&nodes)
            .await?
            .map_or(0, |commit| commit.sequence))
    }

    /// Reconstructs every committed mutation newer than `sequence`, in order.
    pub async fn read_after(&self, sequence: u64) -> Result<Vec<CatalogMutation>, CatalogLogError> {
        let nodes = self.ring_nodes()?;
        let mut cursor = self.latest_ref(&nodes).await?;
        let mut reversed = Vec::new();
        while let Some(commit) = cursor {
            if commit.sequence <= sequence {
                break;
            }
            let stored = self.read_commit(&commit).await?;
            if stored.mutation.sequence != commit.sequence
                || stored.mutation.event_id != commit.event_id
            {
                return Err(CatalogLogError::Corrupt(format!(
                    "commit reference does not match event {}",
                    commit.event_id
                )));
            }
            if let Some(previous) = &stored.previous
                && previous.sequence >= commit.sequence
            {
                return Err(CatalogLogError::Corrupt(format!(
                    "sequence {} points backward to {}",
                    commit.sequence, previous.sequence
                )));
            }
            reversed.push(stored.mutation);
            cursor = stored.previous;
        }
        reversed.reverse();
        Ok(reversed)
    }

    /// Requires a production-sized ring and returns a stable node ordering.
    fn ring_nodes(&self) -> Result<Vec<NodeId>, CatalogLogError> {
        let mut nodes = self.membership.live_nodes();
        nodes.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        nodes.dedup();
        if nodes.len() < MINIMUM_RING_NODES {
            return Err(CatalogLogError::QuorumUnavailable {
                required: MINIMUM_RING_NODES,
                available: nodes.len(),
            });
        }
        Ok(nodes)
    }

    /// Reads the replicated tail and accepts only a matching read quorum.
    async fn latest_ref(&self, nodes: &[NodeId]) -> Result<Option<CommitRef>, CatalogLogError> {
        let (read_quorum, _, _) = ring_geometry(nodes.len());
        let key = self.tail_key();
        let mut successful_reads = 0usize;
        let mut candidates: HashMap<Vec<u8>, (usize, CommitRef)> = HashMap::new();
        for node in nodes {
            let loaded = match self.transport.load(node, &key).await {
                Ok(loaded) => {
                    successful_reads += 1;
                    loaded
                }
                Err(_) => continue,
            };
            let Some(loaded) = loaded.filter(|fragment| fragment.is_healthy()) else {
                continue;
            };
            let commit: CommitRef = match serde_json::from_slice(&loaded.bytes) {
                Ok(commit) => commit,
                Err(_) => continue,
            };
            let entry = candidates
                .entry(loaded.bytes.to_vec())
                .or_insert((0, commit));
            entry.0 += 1;
        }
        let observed_candidates = !candidates.is_empty();
        let most_agreement = candidates
            .values()
            .map(|(count, _)| *count)
            .max()
            .unwrap_or(0);
        let agreed = candidates
            .into_values()
            .filter(|(count, _)| *count >= read_quorum)
            .max_by_key(|(_, commit)| commit.sequence)
            .map(|(_, commit)| commit);
        if agreed.is_some() {
            return Ok(agreed);
        }
        if successful_reads >= read_quorum && !observed_candidates {
            return Ok(None);
        }
        Err(CatalogLogError::QuorumUnavailable {
            required: read_quorum,
            available: most_agreement,
        })
    }

    /// Replicates the new committed tail to every fragment holder.
    async fn publish_tail(
        &self,
        nodes: &[NodeId],
        commit: &CommitRef,
    ) -> Result<(), CatalogLogError> {
        let bytes = serde_json::to_vec(commit)
            .map(Bytes::from)
            .map_err(|error| CatalogLogError::Encoding(error.to_string()))?;
        let key = self.tail_key();
        for (accepted, node) in nodes.iter().enumerate() {
            let record = FragmentRecord::new(key.clone(), bytes.clone());
            if self.transport.place(node, record).await.is_err() {
                return Err(CatalogLogError::QuorumUnavailable {
                    required: nodes.len(),
                    available: accepted,
                });
            }
        }
        Ok(())
    }

    /// Reconstructs and decodes one committed EC record.
    async fn read_commit(&self, commit: &CommitRef) -> Result<StoredMutation, CatalogLogError> {
        let mut fragments = Vec::new();
        for placement in &commit.placements {
            let key = FragmentKey {
                object_id: commit.object_id.clone(),
                index: placement.index,
            };
            let node = NodeId::new(placement.node.as_str());
            let loaded = match self.transport.load(&node, &key).await {
                Ok(Some(loaded)) if loaded.is_healthy() => loaded,
                _ => continue,
            };
            fragments.push(Fragment {
                index: placement.index,
                bytes: loaded.bytes,
                checksum: loaded.checksum,
            });
        }
        let geometry = Geometry {
            k: commit.k,
            m: commit.m,
            chunk: commit.chunk,
        };
        let body = reassemble(geometry, commit.object_len, &fragments)
            .map_err(|error| CatalogLogError::Codec(error.to_string()))?;
        serde_json::from_slice(&body).map_err(|error| CatalogLogError::Corrupt(error.to_string()))
    }

    /// Deletes fragments placed before a failed write quorum.
    async fn delete_placements(&self, object_id: &str, placements: &[Placement]) {
        for placement in placements {
            let key = FragmentKey {
                object_id: object_id.to_owned(),
                index: placement.index,
            };
            let node = NodeId::new(placement.node.as_str());
            let _ = self.transport.delete(&node, &key).await;
        }
    }

    /// Fixed full-copy marker key read from each ring node.
    fn tail_key(&self) -> FragmentKey {
        FragmentKey {
            object_id: format!("catalog-log/{}/tail", self.scope),
            index: TAIL_INDEX,
        }
    }

    /// Stable EC object id for idempotent retry placement.
    fn object_id(&self, mutation: &CatalogMutation) -> String {
        format!(
            "catalog-log/{}/records/{:020}/{}",
            self.scope, mutation.sequence, mutation.event_id
        )
    }
}

/// Uses `RS(n-1, 1)` and persists every fragment before acknowledgment.
fn ring_geometry(nodes: usize) -> (usize, usize, usize) {
    (nodes - 1, 1, nodes)
}

/// Orders distinct fragment holders by rendezvous score for this record.
fn placement_order(seed: &str, nodes: &[NodeId]) -> Vec<NodeId> {
    let key = CacheKey {
        // The catalog log is an internal ring namespace, not an object-provider
        // binding. Give it an explicit reserved identity so its rendezvous key
        // cannot collide with customer data that happens to use the same bucket
        // and object strings.
        storage_binding_id: "verglas-catalog-log".to_owned(),
        bucket: "catalog-log".to_owned(),
        key: seed.to_owned(),
    };
    let mut scored: Vec<(u64, NodeId)> = nodes
        .iter()
        .map(|node| (rendezvous_hash(&key, node), node.clone()))
        .collect();
    scored.sort_by(|(left_score, left), (right_score, right)| {
        right_score
            .cmp(left_score)
            .then_with(|| left.as_str().cmp(right.as_str()))
    });
    scored.into_iter().map(|(_, node)| node).collect()
}
