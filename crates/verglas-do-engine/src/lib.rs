//! DataFusion-backed Durable Object transactions over Verglas consensus.
//!
//! DataFusion owns SQL planning and execution. This crate owns explicit snapshot
//! fences, private write sets, canonical Arrow mutation envelopes, and atomic
//! application after the container's single commit authority acknowledges.

mod cas;
mod checkpoint;
mod endpoint;
mod error;
mod graph;
mod lakehouse;
mod materialize;
mod object;
mod offload;
mod provider;
mod query;
mod replica;
mod replication;
mod session;
mod storage;
mod transaction;
mod vector;
mod worker_state;

pub use cas::{CasCommitAuthority, LeaseGrant, LeaseIdentity};
pub use checkpoint::{CheckpointReceipt, ObjectStoreCheckpointPublisher};
pub use endpoint::{ReplicaEndpoint, ReplicaEndpointError, ReplicaEndpointRole};
pub use error::{Error, Result};
pub use graph::GraphIndexConfig;
pub use lakehouse::{LakehouseObject, PublicationAuthorization, StorageBinding};
pub use materialize::{
    ArtifactCoverage, ArtifactKind, ArtifactReceipt, DerivedArtifact,
    ObjectStoreDerivedArtifactPublisher,
};
pub use object::{ObjectKind, ObjectPolicy};
pub use offload::{
    ArchiveReceipt, ManagedTransactionArchive, ObjectStoreOffloadBatchArchive,
    ObjectStoreTransactionArchive, OffloadBatch, OffloadBatchArchive, OffloadBatchPolicy,
    OffloadBatchReceipt, OffloadBatcher, OffloadReport, TransactionArchive,
};
pub use provider::{DoTableProvider, TransactionHandle};
pub use query::{DatasetCache, DatasetCachePin, QueryObject};
pub use replica::{ApplyOutcome, ReplicaEntry, ReplicaState, SqliteReplicaStore};
pub use replication::{ReplicaCommitAuthority, ReplicaReplayEntry, ReplicaSink, UnixReplicaSink};
pub use session::DoSession;
pub use storage::{CheckpointManifest, DoEngine, DoStorage, Predicate, Projection, SnapshotFence};
pub use transaction::{
    CommitAuthority, CommitReceipt, DoTransaction, EngineTransaction, IsolationLevel,
    MutationBatch, MutationDomain, TableId, TransactionEnvelope,
};
pub use vector::VectorIndexConfig;
pub use worker_state::{
    WORKER_ALARM_TABLE, WORKER_ATTACHMENTS_TABLE, WORKER_KV_TABLE, WorkerStateView,
    ensure_worker_tables, stage_alarm_clear, stage_alarm_set, stage_attachment, stage_kv_delete,
    stage_kv_put,
};
