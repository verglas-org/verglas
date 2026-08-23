//! Snapshot storage abstraction with optional SQLite-backed replica replay.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Instant;

use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::stream;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::error::{Error, Result};
use crate::graph::{GraphIndexConfig, LiveGraphProjection};
use crate::materialize::{ArtifactCoverage, ArtifactKind, DerivedArtifact};
use crate::offload::{
    ManagedTransactionArchive, OffloadBatchArchive, OffloadBatchPolicy, OffloadBatcher,
    OffloadReport, TransactionArchive,
};
use crate::replica::SqliteReplicaStore;
use crate::transaction::{
    CommitAuthority, CommitReceipt, DoTransaction, EngineTransaction, IsolationLevel,
    MutationDomain, TableId, TransactionEnvelope,
};
use crate::vector::{LiveVectorProjection, VectorIndexConfig};
use verglas_graph::{Direction, Neighbor as GraphNeighbor};
use verglas_vector::Neighbor;

/// Explicit commit-sequence boundary used by every table scan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotFence(u64);

impl SnapshotFence {
    /// Creates a fence at one committed transaction sequence.
    pub fn at(commit_sequence: u64) -> Self {
        Self(commit_sequence)
    }

    /// Returns the maximum commit sequence visible to the scan.
    pub fn commit_sequence(self) -> u64 {
        self.0
    }
}

/// Column projection requested by a storage scan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Projection {
    /// Returns every table column in schema order.
    All,
    /// Returns only the zero-based columns in the supplied order.
    Columns(Vec<usize>),
}

impl Projection {
    /// Creates a projection containing every column.
    pub fn all() -> Self {
        Self::All
    }
}

/// Storage-owned predicate representation prepared by the DataFusion provider.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Predicate(String);

impl Predicate {
    /// Creates a predicate from a stable display representation.
    pub fn new(expression: impl Into<String>) -> Self {
        Self(expression.into())
    }

    /// Returns the stable predicate representation.
    pub fn expression(&self) -> &str {
        &self.0
    }
}

/// Manifest produced after state through one commit sequence is checkpointed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckpointManifest {
    through: u64,
}

impl CheckpointManifest {
    /// Creates a verified checkpoint manifest watermark.
    pub fn new(through: u64) -> Self {
        Self { through }
    }

    /// Returns the newest commit included by this checkpoint.
    pub fn through(self) -> u64 {
        self.through
    }
}

/// Verglas-owned storage contract behind DataFusion table providers.
#[async_trait]
pub trait DoStorage: Send + Sync {
    /// Begins private transaction state at the current applied snapshot.
    async fn begin(&self, isolation: IsolationLevel) -> Result<Box<dyn DoTransaction>>;

    /// Scans one table at an explicit snapshot fence.
    async fn scan(
        &self,
        table: TableId,
        snapshot: SnapshotFence,
        projection: Projection,
        filters: Vec<Predicate>,
    ) -> Result<SendableRecordBatchStream>;

    /// Submits one canonical envelope and applies it only after authority ACK.
    async fn commit(&self, transaction: Box<dyn DoTransaction>) -> Result<CommitReceipt>;

    /// Checkpoints applied state through a committed sequence.
    async fn checkpoint(&self, through: u64) -> Result<CheckpointManifest>;
}

/// One relational batch and the transaction sequence that made it visible.
#[derive(Clone)]
struct VersionedBatch {
    sequence: u64,
    batch: RecordBatch,
}

/// Mutable state of one table in the prototype MVCC memtable.
struct TableState {
    schema: SchemaRef,
    relational: Vec<VersionedBatch>,
}

/// Atomically applied Durable Object state across all three domains.
#[derive(Default)]
struct AppliedState {
    sequence: u64,
    tables: HashMap<TableId, TableState>,
    vector_log: HashMap<TableId, Vec<VersionedBatch>>,
    vector_indexes: HashMap<TableId, LiveVectorProjection>,
    graph_log: HashMap<TableId, Vec<VersionedBatch>>,
    graph_indexes: HashMap<TableId, LiveGraphProjection>,
    domain_watermarks: HashMap<MutationDomain, u64>,
    committed: HashMap<Uuid, (Vec<u8>, CommitReceipt)>,
    archive_log: Vec<ManagedTransactionArchive>,
    archive_watermark: u64,
    checkpoint: u64,
}

/// Embedded Durable Object engine with a consensus-owned commit authority.
pub struct DoEngine {
    do_id: String,
    authority: Arc<dyn CommitAuthority>,
    state: RwLock<AppliedState>,
    commit_gate: Mutex<()>,
    offload_gate: Mutex<()>,
    replica: Option<Arc<SqliteReplicaStore>>,
}

impl DoEngine {
    /// Creates an empty Durable Object state machine behind one commit authority.
    pub fn new(do_id: impl Into<String>, authority: Arc<dyn CommitAuthority>) -> Self {
        Self {
            do_id: do_id.into(),
            authority,
            state: RwLock::new(AppliedState::default()),
            commit_gate: Mutex::new(()),
            offload_gate: Mutex::new(()),
            replica: None,
        }
    }

    /// Opens a SQLite-backed replica and replays its committed transaction log.
    pub fn open_persistent(
        do_id: impl Into<String>,
        authority: Arc<dyn CommitAuthority>,
        replica: Arc<SqliteReplicaStore>,
    ) -> Result<Self> {
        let do_id = do_id.into();
        if replica.do_id() != do_id {
            return Err(Error::WrongDo {
                expected: do_id,
                actual: replica.do_id().to_owned(),
            });
        }
        let replica_state = replica.state()?;
        let engine = Self {
            do_id,
            authority,
            state: RwLock::new(AppliedState::default()),
            commit_gate: Mutex::new(()),
            offload_gate: Mutex::new(()),
            replica: Some(replica.clone()),
        };
        {
            let mut state = engine.state.write().map_err(|_| Error::Poisoned)?;
            for entry in replica.replay()? {
                let envelope =
                    TransactionEnvelope::from_canonical_bytes(entry.canonical_envelope())?;
                if envelope.do_id() != engine.do_id
                    || envelope.transaction_id() != entry.transaction_id()
                {
                    return Err(Error::ReplicaConflict(format!(
                        "replay identity mismatch at sequence {}",
                        entry.commit_sequence()
                    )));
                }
                for mutation in envelope.mutations() {
                    match state.tables.get(mutation.table()) {
                        Some(table)
                            if table.schema.as_ref() != mutation.batch().schema().as_ref() =>
                        {
                            return Err(Error::ReplicaConflict(format!(
                                "replay schema mismatch for table {}",
                                mutation.table().as_str()
                            )));
                        }
                        Some(_) => {}
                        None => {
                            state.tables.insert(
                                mutation.table().clone(),
                                TableState {
                                    schema: mutation.batch().schema(),
                                    relational: Vec::new(),
                                },
                            );
                        }
                    }
                }
                let receipt = CommitReceipt::new(entry.commit_sequence(), entry.transaction_id());
                engine.validate_receipt(&state, &envelope, receipt)?;
                engine.apply(
                    &mut state,
                    &envelope,
                    receipt,
                    entry.canonical_envelope().to_vec(),
                )?;
            }
            if state.sequence != replica_state.applied_sequence() {
                return Err(Error::ReplicaSequence(format!(
                    "replayed sequence {} differs from pager sequence {}",
                    state.sequence,
                    replica_state.applied_sequence()
                )));
            }
            state.archive_watermark = replica_state.archive_sequence();
            state.checkpoint = replica_state.checkpoint_sequence();
        }
        Ok(engine)
    }

    /// Registers a SQL table schema before row transactions begin.
    pub async fn create_table(&self, table: TableId, schema: SchemaRef) -> Result<()> {
        let mut state = self.state.write().map_err(|_| Error::Poisoned)?;
        state.tables.insert(
            table,
            TableState {
                schema,
                relational: Vec::new(),
            },
        );
        Ok(())
    }

    /// Registers and rebuilds one Vamana projection from every committed vector mutation.
    pub async fn register_vector_index(&self, config: VectorIndexConfig) -> Result<()> {
        let _guard = self.commit_gate.lock().await;
        let mut state = self.state.write().map_err(|_| Error::Poisoned)?;
        if !state.tables.contains_key(config.table()) {
            return Err(Error::UnknownTable(config.table().as_str().to_owned()));
        }
        let mut projection = LiveVectorProjection::new(config.clone())?;
        if let Some(history) = state.vector_log.get(config.table()) {
            for versioned in history {
                projection.apply(versioned.sequence, &versioned.batch)?;
            }
        }
        state
            .vector_indexes
            .insert(config.table().clone(), projection);
        Ok(())
    }

    /// Searches the complete live vector row set with exact distance reranking.
    pub fn vector_search(&self, table: &TableId, query: &[f32], k: usize) -> Result<Vec<Neighbor>> {
        self.state
            .read()
            .map_err(|_| Error::Poisoned)?
            .vector_indexes
            .get(table)
            .ok_or_else(|| Error::UnknownTable(table.as_str().to_owned()))?
            .search(query, k)
    }

    /// Returns the transaction sequence represented by one live Vamana projection.
    pub fn vector_index_through(&self, table: &TableId) -> Option<u64> {
        self.state.read().ok().and_then(|state| {
            state
                .vector_indexes
                .get(table)
                .map(LiveVectorProjection::through)
        })
    }

    /// Frames the live Vamana delta as a Puffin artifact with explicit DO coverage.
    pub async fn vector_puffin_artifact(&self, table: &TableId) -> Result<DerivedArtifact> {
        let (index, through) = {
            let state = self.state.read().map_err(|_| Error::Poisoned)?;
            let projection = state
                .vector_indexes
                .get(table)
                .ok_or_else(|| Error::UnknownTable(table.as_str().to_owned()))?;
            (projection.index_for_puffin(), projection.through())
        };
        let bytes = verglas_vector::puffin::to_puffin_bytes(&index, HashMap::new()).await?;
        Ok(DerivedArtifact::new(
            self.do_id.clone(),
            table.clone(),
            ArtifactKind::VamanaPuffin,
            ArtifactCoverage::new(0, through)?,
            bytes,
        ))
    }

    /// Registers and rebuilds one adjacency projection from committed graph mutations.
    pub async fn register_graph_index(&self, config: GraphIndexConfig) -> Result<()> {
        let _guard = self.commit_gate.lock().await;
        let mut state = self.state.write().map_err(|_| Error::Poisoned)?;
        if !state.tables.contains_key(config.table()) {
            return Err(Error::UnknownTable(config.table().as_str().to_owned()));
        }
        let mut projection = LiveGraphProjection::new(config.clone());
        if let Some(history) = state.graph_log.get(config.table()) {
            for versioned in history {
                projection.apply(versioned.sequence, &versioned.batch)?;
            }
        }
        state
            .graph_indexes
            .insert(config.table().clone(), projection);
        Ok(())
    }

    /// Reads one node's forward, reverse, or bidirectional live adjacency.
    pub fn graph_neighbors(
        &self,
        table: &TableId,
        node_id: &str,
        direction: Direction,
        predicate: Option<&str>,
    ) -> Result<Vec<GraphNeighbor>> {
        Ok(self
            .state
            .read()
            .map_err(|_| Error::Poisoned)?
            .graph_indexes
            .get(table)
            .ok_or_else(|| Error::UnknownTable(table.as_str().to_owned()))?
            .neighbors(node_id, direction, predicate))
    }

    /// Returns the transaction sequence represented by one adjacency projection.
    pub fn graph_index_through(&self, table: &TableId) -> Option<u64> {
        self.state.read().ok().and_then(|state| {
            state
                .graph_indexes
                .get(table)
                .map(LiveGraphProjection::through)
        })
    }

    /// Frames live graph adjacency as a Puffin artifact with explicit DO coverage.
    pub async fn graph_puffin_artifact(&self, table: &TableId) -> Result<DerivedArtifact> {
        let (adjacency, through) = {
            let state = self.state.read().map_err(|_| Error::Poisoned)?;
            let projection = state
                .graph_indexes
                .get(table)
                .ok_or_else(|| Error::UnknownTable(table.as_str().to_owned()))?;
            (projection.adjacency_for_puffin(), projection.through())
        };
        let bytes = verglas_graph::puffin::to_puffin_bytes(&adjacency)
            .await
            .map_err(|error| Error::GraphProjection(error.to_string()))?;
        Ok(DerivedArtifact::new(
            self.do_id.clone(),
            table.clone(),
            ArtifactKind::GraphPuffin,
            ArtifactCoverage::new(0, through)?,
            bytes,
        ))
    }

    /// Begins a retryable transaction with a caller-supplied identity.
    pub async fn begin_with_id(
        &self,
        isolation: IsolationLevel,
        transaction_id: Uuid,
    ) -> Result<Box<dyn DoTransaction>> {
        let base = self.state.read().map_err(|_| Error::Poisoned)?.sequence;
        Ok(Box::new(EngineTransaction::new(TransactionEnvelope::new(
            self.do_id.clone(),
            transaction_id,
            base,
            isolation,
        ))))
    }

    /// Commits exact canonical bytes received by the isolated worker protocol.
    pub async fn commit_canonical(&self, canonical: &[u8]) -> Result<CommitReceipt> {
        let envelope = TransactionEnvelope::from_canonical_bytes(canonical)?;
        self.commit_envelope(&envelope, canonical.to_vec()).await
    }

    /// Serializes one validated envelope through authority and local projections.
    async fn commit_envelope(
        &self,
        envelope: &TransactionEnvelope,
        canonical: Vec<u8>,
    ) -> Result<CommitReceipt> {
        let _guard = self.commit_gate.lock().await;
        if envelope.do_id() != self.do_id {
            return Err(Error::WrongDo {
                expected: self.do_id.clone(),
                actual: envelope.do_id().to_owned(),
            });
        }
        {
            let state = self.state.read().map_err(|_| Error::Poisoned)?;
            if let Some((old_bytes, receipt)) = state.committed.get(&envelope.transaction_id()) {
                if old_bytes == &canonical {
                    return Ok(*receipt);
                }
                return Err(Error::InvalidReceipt(
                    "transaction identity was reused for different content".to_owned(),
                ));
            }
            self.validate_envelope(&state, envelope)?;
        }
        let receipt = self.authority.commit(envelope).await?;
        let mut state = self.state.write().map_err(|_| Error::Poisoned)?;
        self.validate_receipt(&state, envelope, receipt)?;
        if let Some(replica) = &self.replica {
            replica.apply_committed(
                receipt.commit_sequence(),
                envelope.transaction_id(),
                &canonical,
            )?;
        }
        self.apply(&mut state, envelope, receipt, canonical)?;
        Ok(receipt)
    }

    /// Returns the highest transaction sequence applied to local state.
    pub fn applied_sequence(&self) -> u64 {
        self.state.read().map(|state| state.sequence).unwrap_or(0)
    }

    /// Returns the registered Arrow schema for one SQL table.
    pub fn table_schema(&self, table: &TableId) -> Result<SchemaRef> {
        self.state
            .read()
            .map_err(|_| Error::Poisoned)?
            .tables
            .get(table)
            .map(|state| state.schema.clone())
            .ok_or_else(|| Error::UnknownTable(table.as_str().to_owned()))
    }

    /// Returns the newest commit applied to one domain projection.
    pub fn domain_watermark(&self, domain: MutationDomain) -> u64 {
        self.state
            .read()
            .map(|state| state.domain_watermarks.get(&domain).copied().unwrap_or(0))
            .unwrap_or(0)
    }

    /// Returns the contiguous transaction sequence verified in managed storage.
    pub fn archive_watermark(&self) -> u64 {
        self.state
            .read()
            .map(|state| state.archive_watermark)
            .unwrap_or(0)
    }

    /// Returns canonical bytes awaiting managed archive publication.
    pub fn pending_archive_bytes(&self) -> Result<usize> {
        let pending = if let Some(replica) = &self.replica {
            replica.pending_archive()?
        } else {
            let state = self.state.read().map_err(|_| Error::Poisoned)?;
            state
                .archive_log
                .iter()
                .filter(|transaction| transaction.commit_sequence() > state.archive_watermark)
                .cloned()
                .collect::<Vec<_>>()
        };
        Ok(pending.iter().fold(0_usize, |bytes, transaction| {
            bytes.saturating_add(transaction.canonical_envelope().len())
        }))
    }

    /// Archives every committed transaction after the verified watermark in order.
    pub async fn offload_pending(&self, archive: &dyn TransactionArchive) -> Result<OffloadReport> {
        let _guard = self.offload_gate.lock().await;
        let pending = if let Some(replica) = &self.replica {
            replica.pending_archive()?
        } else {
            let state = self.state.read().map_err(|_| Error::Poisoned)?;
            state
                .archive_log
                .iter()
                .filter(|transaction| transaction.commit_sequence() > state.archive_watermark)
                .cloned()
                .collect::<Vec<_>>()
        };
        let mut archived = 0_usize;
        for transaction in pending {
            let receipt = archive.archive(&transaction).await?;
            if receipt.commit_sequence() != transaction.commit_sequence()
                || receipt.etag().is_empty()
            {
                return Err(Error::Archive(format!(
                    "invalid receipt for sequence {}",
                    transaction.commit_sequence()
                )));
            }
            if let Some(replica) = &self.replica {
                replica.mark_archived(transaction.commit_sequence(), receipt.etag())?;
            }
            let mut state = self.state.write().map_err(|_| Error::Poisoned)?;
            let expected = state.archive_watermark.saturating_add(1);
            if transaction.commit_sequence() != expected {
                return Err(Error::Archive(format!(
                    "archive sequence {} is not contiguous after {}",
                    transaction.commit_sequence(),
                    state.archive_watermark
                )));
            }
            state.archive_watermark = transaction.commit_sequence();
            archived = archived.saturating_add(1);
        }
        Ok(OffloadReport::new(archived, self.archive_watermark()))
    }

    /// Compacts pending transactions under the threshold policy and explicitly drains all work.
    pub async fn drain_offload(
        &self,
        archive: &dyn OffloadBatchArchive,
        policy: OffloadBatchPolicy,
    ) -> Result<OffloadReport> {
        let _guard = self.offload_gate.lock().await;
        let pending = if let Some(replica) = &self.replica {
            replica.pending_archive()?
        } else {
            let state = self.state.read().map_err(|_| Error::Poisoned)?;
            state
                .archive_log
                .iter()
                .filter(|transaction| transaction.commit_sequence() > state.archive_watermark)
                .cloned()
                .collect::<Vec<_>>()
        };
        let mut batcher = OffloadBatcher::new(policy);
        let mut batches = Vec::new();
        let now = Instant::now();
        for transaction in pending {
            if let Some(batch) = batcher.push(transaction, now)? {
                batches.push(batch);
            }
        }
        if let Some(batch) = batcher.drain() {
            batches.push(batch);
        }
        let mut archived = 0_usize;
        for batch in batches {
            let receipt = archive.archive(&batch).await?;
            if receipt.from_sequence() != batch.from_sequence()
                || receipt.through_sequence() != batch.through_sequence()
                || receipt.transactions() != batch.transactions().len()
                || receipt.etag().is_empty()
            {
                return Err(Error::Archive(format!(
                    "invalid compacted receipt through {}",
                    batch.through_sequence()
                )));
            }
            for transaction in batch.transactions() {
                if let Some(replica) = &self.replica {
                    replica.mark_archived(transaction.commit_sequence(), receipt.etag())?;
                }
                let mut state = self.state.write().map_err(|_| Error::Poisoned)?;
                let expected = state.archive_watermark.saturating_add(1);
                if transaction.commit_sequence() != expected {
                    return Err(Error::Archive(format!(
                        "archive sequence {} is not contiguous after {}",
                        transaction.commit_sequence(),
                        state.archive_watermark
                    )));
                }
                state.archive_watermark = transaction.commit_sequence();
                archived = archived.saturating_add(1);
            }
        }
        Ok(OffloadReport::new(archived, self.archive_watermark()))
    }

    /// Rejects malformed mutations before they can reach the commit authority.
    fn validate_envelope(
        &self,
        state: &AppliedState,
        envelope: &TransactionEnvelope,
    ) -> Result<()> {
        for mutation in envelope.mutations() {
            let Some(table) = state.tables.get(mutation.table()) else {
                return Err(Error::UnknownTable(mutation.table().as_str().to_owned()));
            };
            if table.schema.as_ref() != mutation.batch().schema().as_ref() {
                return Err(Error::Arrow(arrow_schema::ArrowError::SchemaError(
                    format!(
                        "mutation schema for {} does not match registered table schema",
                        mutation.table().as_str()
                    ),
                )));
            }
            if mutation.domain() == MutationDomain::Vector
                && let Some(index) = state.vector_indexes.get(mutation.table())
            {
                index.validate(state.sequence.saturating_add(1), mutation.batch())?;
            }
            if mutation.domain() == MutationDomain::Graph
                && let Some(index) = state.graph_indexes.get(mutation.table())
            {
                index.validate(state.sequence.saturating_add(1), mutation.batch())?;
            }
        }
        Ok(())
    }

    /// Validates the authority receipt before changing applied state.
    fn validate_receipt(
        &self,
        state: &AppliedState,
        envelope: &TransactionEnvelope,
        receipt: CommitReceipt,
    ) -> Result<()> {
        if receipt.transaction_id() != envelope.transaction_id() {
            return Err(Error::InvalidReceipt(
                "authority returned a different transaction identity".to_owned(),
            ));
        }
        let expected = state.sequence.saturating_add(1);
        if receipt.commit_sequence() != expected {
            return Err(Error::InvalidReceipt(format!(
                "authority returned sequence {}, expected {expected}",
                receipt.commit_sequence()
            )));
        }
        Ok(())
    }

    /// Applies every domain projection under one write lock and sequence.
    fn apply(
        &self,
        state: &mut AppliedState,
        envelope: &TransactionEnvelope,
        receipt: CommitReceipt,
        canonical: Vec<u8>,
    ) -> Result<()> {
        let sequence = receipt.commit_sequence();
        for mutation in envelope.mutations() {
            let versioned = VersionedBatch {
                sequence,
                batch: mutation.batch().clone(),
            };
            if mutation.domain() == MutationDomain::Relational
                && let Some(table) = state.tables.get_mut(mutation.table())
            {
                table.relational.push(versioned.clone());
            }
            if mutation.domain() == MutationDomain::Vector {
                state
                    .vector_log
                    .entry(mutation.table().clone())
                    .or_default()
                    .push(versioned.clone());
                if let Some(index) = state.vector_indexes.get_mut(mutation.table()) {
                    index.apply(sequence, mutation.batch())?;
                }
            }
            if mutation.domain() == MutationDomain::Graph {
                state
                    .graph_log
                    .entry(mutation.table().clone())
                    .or_default()
                    .push(versioned);
                if let Some(index) = state.graph_indexes.get_mut(mutation.table()) {
                    index.apply(sequence, mutation.batch())?;
                }
            }
            state.domain_watermarks.insert(mutation.domain(), sequence);
        }
        state.sequence = sequence;
        state.archive_log.push(ManagedTransactionArchive::new(
            self.do_id.clone(),
            envelope.transaction_id(),
            sequence,
            canonical.clone(),
        ));
        state
            .committed
            .insert(envelope.transaction_id(), (canonical, receipt));
        Ok(())
    }
}

#[async_trait]
impl DoStorage for DoEngine {
    /// Begins a new transaction at the current sequence with a random retry identity.
    async fn begin(&self, isolation: IsolationLevel) -> Result<Box<dyn DoTransaction>> {
        self.begin_with_id(isolation, Uuid::new_v4()).await
    }

    /// Streams committed relational batches visible at the requested fence.
    async fn scan(
        &self,
        table: TableId,
        snapshot: SnapshotFence,
        projection: Projection,
        filters: Vec<Predicate>,
    ) -> Result<SendableRecordBatchStream> {
        if let Some(predicate) = filters.first() {
            return Err(Error::UnsupportedPredicate(
                predicate.expression().to_owned(),
            ));
        }
        let state = self.state.read().map_err(|_| Error::Poisoned)?;
        if snapshot.commit_sequence() > state.sequence {
            return Err(Error::SnapshotAhead {
                requested: snapshot.commit_sequence(),
                applied: state.sequence,
            });
        }
        let table_state = state
            .tables
            .get(&table)
            .ok_or_else(|| Error::UnknownTable(table.as_str().to_owned()))?;
        let (schema, columns) = match projection {
            Projection::All => (table_state.schema.clone(), None),
            Projection::Columns(columns) => {
                let fields = columns
                    .iter()
                    .map(|index| {
                        table_state.schema.fields().get(*index).cloned().ok_or(
                            Error::InvalidProjection {
                                index: *index,
                                width: table_state.schema.fields().len(),
                            },
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;
                let schema = Arc::new(Schema::new(fields));
                (schema, Some(columns))
            }
        };
        let batches = table_state
            .relational
            .iter()
            .filter(|version| version.sequence <= snapshot.commit_sequence())
            .map(|version| match &columns {
                Some(columns) => version.batch.project(columns),
                None => Ok(version.batch.clone()),
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let stream = stream::iter(batches.into_iter().map(Ok));
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    /// Serializes one transaction through the sole authority and then applies it atomically.
    async fn commit(&self, transaction: Box<dyn DoTransaction>) -> Result<CommitReceipt> {
        let envelope = transaction.envelope();
        let canonical = envelope.canonical_bytes()?;
        self.commit_envelope(envelope, canonical).await
    }

    /// Advances the local checkpoint watermark without exceeding applied state.
    async fn checkpoint(&self, through: u64) -> Result<CheckpointManifest> {
        let mut state = self.state.write().map_err(|_| Error::Poisoned)?;
        if through > state.sequence {
            return Err(Error::SnapshotAhead {
                requested: through,
                applied: state.sequence,
            });
        }
        state.checkpoint = state.checkpoint.max(through);
        Ok(CheckpointManifest::new(state.checkpoint))
    }
}
