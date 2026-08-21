//! Small commit metadata rides inside the Raft entry; large payloads still
//! take the erasure-coded path. See `tests/cluster-local/RAFT-DEFECT.md` for
//! the measured defect this closes: commit metadata staged and sealed as if
//! it were object payload.

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use openraft::BasicNode;
use openraft::error::{InstallSnapshotError, NetworkError, RPCError, RemoteError};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use tempfile::TempDir;
use tokio::sync::RwLock;
use verglas_consensus::{
    AppliedOutcome, CommandKind, ConsensusGroup, DistributedPayloadStore, FilePayloadReplica,
    GroupError, PayloadCertificate, PayloadError, PayloadRepresentation, PayloadSet, PayloadStore,
    PayloadStoreFactory, PersistentLogStore, PersistentStateMachine, ReconstructRequest,
    ReleaseRequest, RepairRequest, ReplicationMode, RepresentationTransport, RequestId,
    SealRequest, StagedPayload,
};

type Raft = openraft::Raft<verglas_consensus::VerglasRaftConfig>;
type RaftError<E = openraft::error::Infallible> = openraft::error::RaftError<u64, E>;
type NetworkResult<T, E = openraft::error::Infallible> =
    Result<T, RPCError<u64, BasicNode, RaftError<E>>>;

#[derive(Clone, Default)]
struct Router {
    nodes: Arc<RwLock<BTreeMap<u64, Raft>>>,
}

impl Router {
    async fn target<E: std::error::Error>(&self, target: u64) -> NetworkResult<Raft, E> {
        self.nodes
            .read()
            .await
            .get(&target)
            .cloned()
            .ok_or_else(|| {
                RPCError::Network(NetworkError::new(&io::Error::new(
                    io::ErrorKind::NotFound,
                    "unknown test node",
                )))
            })
    }
}

impl RaftNetworkFactory<verglas_consensus::VerglasRaftConfig> for Router {
    type Network = Connection;

    async fn new_client(&mut self, target: u64, _node: &BasicNode) -> Self::Network {
        Connection {
            router: self.clone(),
            target,
        }
    }
}

struct Connection {
    router: Router,
    target: u64,
}

impl RaftNetwork<verglas_consensus::VerglasRaftConfig> for Connection {
    async fn append_entries(
        &mut self,
        request: AppendEntriesRequest<verglas_consensus::VerglasRaftConfig>,
        _option: RPCOption,
    ) -> NetworkResult<AppendEntriesResponse<u64>> {
        let target = self.router.target(self.target).await?;
        target
            .append_entries(request)
            .await
            .map_err(|error| RPCError::RemoteError(RemoteError::new(self.target, error)))
    }

    async fn install_snapshot(
        &mut self,
        request: InstallSnapshotRequest<verglas_consensus::VerglasRaftConfig>,
        _option: RPCOption,
    ) -> NetworkResult<InstallSnapshotResponse<u64>, InstallSnapshotError> {
        let target = self.router.target(self.target).await?;
        target
            .install_snapshot(request)
            .await
            .map_err(|error| RPCError::RemoteError(RemoteError::new(self.target, error)))
    }

    async fn vote(
        &mut self,
        request: VoteRequest<u64>,
        _option: RPCOption,
    ) -> NetworkResult<VoteResponse<u64>> {
        let target = self.router.target(self.target).await?;
        target
            .vote(request)
            .await
            .map_err(|error| RPCError::RemoteError(RemoteError::new(self.target, error)))
    }
}

/// Starts an in-process Raft cluster of `node_count` voters rooted under `root`,
/// waits for the uniform membership to apply, and confirms node one leads.
async fn start_cluster(
    root: &TempDir,
    node_count: u64,
) -> (BTreeMap<u64, Raft>, BTreeMap<u64, PersistentStateMachine>) {
    let shared_nodes = Arc::new(RwLock::new(BTreeMap::new()));
    let mut state_machines = BTreeMap::new();
    let config = Arc::new(
        openraft::Config {
            heartbeat_interval: 50,
            election_timeout_min: 150,
            election_timeout_max: 300,
            ..Default::default()
        }
        .validate()
        .expect("Raft config"),
    );
    for node in 1..=node_count {
        let directory = root.path().join(node.to_string());
        let log = PersistentLogStore::open(directory.join("log.json"))
            .await
            .expect("log store");
        let state = PersistentStateMachine::open(directory.join("state.json"))
            .await
            .expect("state machine");
        let raft = Raft::new(
            node,
            config.clone(),
            Router {
                nodes: shared_nodes.clone(),
            },
            log,
            state.clone(),
        )
        .await
        .expect("start Raft node");
        shared_nodes.write().await.insert(node, raft);
        state_machines.insert(node, state);
    }
    let members: BTreeMap<_, _> = (1..=node_count)
        .map(|node| (node, BasicNode::new(format!("node-{node}"))))
        .collect();
    let node1 = shared_nodes.read().await[&1].clone();
    node1.initialize(members).await.expect("initialize cluster");
    let expected: BTreeSet<_> = (1..=node_count).collect();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if state_machines[&1].committed_voters().await == expected {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("initial membership is applied");
    node1
        .wait(Some(Duration::from_secs(5)))
        .current_leader(1, "initial leader")
        .await
        .expect("node one elected");
    let nodes = shared_nodes.read().await.clone();
    (nodes, state_machines)
}

/// A payload store that fails loudly if the write hot path ever touches it.
///
/// Used to prove an inline commit performs no staging and no seal: if
/// `commit_header` regressed and called either, the whole commit would fail
/// with this injected error instead of silently succeeding.
struct NeverStageOrSeal;

#[async_trait::async_trait]
impl PayloadStore for NeverStageOrSeal {
    async fn set_voters(&self, _voters: Vec<u64>) -> Result<(), PayloadError> {
        Ok(())
    }

    async fn stage(
        &self,
        _request: RequestId,
        _group: &str,
        _configuration_generation: u64,
        _mode: ReplicationMode,
        _body: &[u8],
        _candidates: &[u64],
    ) -> Result<StagedPayload, PayloadError> {
        Err(PayloadError::Transport(
            "stage must not be called for a body at or under the inline threshold".to_owned(),
        ))
    }

    async fn reconstruct(&self, _read: ReconstructRequest<'_>) -> Result<Bytes, PayloadError> {
        Err(PayloadError::Transport(
            "reconstruct must not be called for an inline-committed body".to_owned(),
        ))
    }

    async fn repair(&self, _repair: RepairRequest<'_>) -> Result<PayloadCertificate, PayloadError> {
        Err(PayloadError::Transport(
            "repair must not be called for an inline-committed body".to_owned(),
        ))
    }

    async fn seal(&self, _seal: SealRequest<'_>) -> Result<(), PayloadError> {
        Err(PayloadError::Transport(
            "seal must not be called for a body at or under the inline threshold".to_owned(),
        ))
    }

    async fn release(&self, _release: ReleaseRequest<'_>) -> Result<(), PayloadError> {
        Err(PayloadError::Transport(
            "release must not be called for an inline-committed body".to_owned(),
        ))
    }
}

/// Call counters shared between a `RecordingPayloadStore` and its test.
#[derive(Clone, Default)]
struct CallCounts {
    stage: Arc<AtomicUsize>,
    seal: Arc<AtomicUsize>,
}

/// Delegates every call to a real `PayloadSet` while counting stage and seal
/// calls, so a test can prove the coded path still runs for a large body.
struct RecordingPayloadStore {
    inner: Arc<PayloadSet>,
    counts: CallCounts,
}

#[async_trait::async_trait]
impl PayloadStore for RecordingPayloadStore {
    async fn set_voters(&self, voters: Vec<u64>) -> Result<(), PayloadError> {
        self.inner.set_voters(voters).await
    }

    async fn stage(
        &self,
        request: RequestId,
        group: &str,
        configuration_generation: u64,
        mode: ReplicationMode,
        body: &[u8],
        candidates: &[u64],
    ) -> Result<StagedPayload, PayloadError> {
        self.counts.stage.fetch_add(1, Ordering::SeqCst);
        self.inner
            .stage(
                request,
                group,
                configuration_generation,
                mode,
                body,
                candidates,
            )
            .await
    }

    async fn reconstruct(&self, read: ReconstructRequest<'_>) -> Result<Bytes, PayloadError> {
        self.inner.reconstruct(read).await
    }

    async fn repair(&self, repair: RepairRequest<'_>) -> Result<PayloadCertificate, PayloadError> {
        self.inner.repair(repair).await
    }

    async fn seal(&self, seal: SealRequest<'_>) -> Result<(), PayloadError> {
        self.counts.seal.fetch_add(1, Ordering::SeqCst);
        self.inner.seal(seal).await
    }

    async fn release(&self, release: ReleaseRequest<'_>) -> Result<(), PayloadError> {
        self.inner.release(release).await
    }
}

/// In-memory peer transport for lazy-store construction and coding-path tests.
#[derive(Default)]
struct MemoryTransport {
    records: std::sync::Mutex<BTreeMap<u64, PayloadRepresentation>>,
}

impl MemoryTransport {
    /// Returns the number of representations currently held by the transport.
    fn stored_count(&self) -> usize {
        self.records.lock().expect("payload records lock").len()
    }
}

#[async_trait::async_trait]
impl RepresentationTransport for MemoryTransport {
    /// Retains one representation under its configured voter identity.
    async fn store(
        &self,
        voter: u64,
        representation: PayloadRepresentation,
    ) -> Result<(), PayloadError> {
        self.records
            .lock()
            .expect("payload records lock")
            .insert(voter, representation);
        Ok(())
    }

    /// Returns one retained representation when its request identity matches.
    async fn load(
        &self,
        voter: u64,
        hash: [u8; 32],
        group: &str,
        configuration_generation: u64,
        request: RequestId,
    ) -> Result<Option<PayloadRepresentation>, PayloadError> {
        Ok(self
            .records
            .lock()
            .expect("payload records lock")
            .get(&voter)
            .filter(|record| {
                record.hash == hash
                    && record.group == group
                    && record.configuration_generation == configuration_generation
                    && record.request == request
            })
            .cloned())
    }

    /// Deletes one retained representation for an exact holder identity.
    async fn delete(
        &self,
        voter: u64,
        _hash: [u8; 32],
        _group: &str,
        _configuration_generation: u64,
        _request: RequestId,
        _slot: usize,
    ) -> Result<(), PayloadError> {
        self.records
            .lock()
            .expect("payload records lock")
            .remove(&voter);
        Ok(())
    }
}

/// A commit metadata body far below the 4 KiB inline threshold: the exact
/// shape of the object-commit JSON described in RAFT-DEFECT.md (storage
/// binding, bucket, key, object id, length, hash, geometry, placements).
fn small_object_metadata() -> Bytes {
    Bytes::from_static(
        br#"{"storage_binding":"b0","bucket":"objects","key":"k","object_id":"o1","object_len":4096,"payload_hash":"aa","k":3,"m":2,"placements":[]}"#,
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_voter_inline_commit_does_not_construct_payload_store() {
    let root = TempDir::new().expect("cluster directory");
    let (nodes, state_machines) = start_cluster(&root, 1).await;
    let transport = Arc::new(MemoryTransport::default());
    let factory_calls = Arc::new(AtomicUsize::new(0));
    let factory: PayloadStoreFactory = {
        let transport = transport.clone();
        let factory_calls = factory_calls.clone();
        Box::new(move || {
            factory_calls.fetch_add(1, Ordering::SeqCst);
            let store = DistributedPayloadStore::new(1, 1, vec![1], transport.clone())?;
            Ok(Arc::new(store) as Arc<dyn PayloadStore>)
        })
    };
    let group = ConsensusGroup::new(
        "warehouse/lazy-inline",
        1,
        nodes[&1].clone(),
        state_machines[&1].clone(),
        factory,
    )
    .expect("group");

    let body = small_object_metadata();
    let committed = group
        .commit(
            RequestId::from_u128(101),
            CommandKind::Object,
            ReplicationMode::Coded,
            body.clone(),
            None,
            &[1],
        )
        .await
        .expect("an inline commit must not construct the payload store");
    assert_eq!(committed.outcome, AppliedOutcome::Committed);
    assert_eq!(
        group
            .read(committed.index)
            .await
            .expect("an inline read must not construct the payload store"),
        body
    );
    assert_eq!(factory_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_voter_large_commit_surfaces_invalid_payload_geometry() {
    let root = TempDir::new().expect("cluster directory");
    let (nodes, state_machines) = start_cluster(&root, 1).await;
    let transport = Arc::new(MemoryTransport::default());
    let factory_calls = Arc::new(AtomicUsize::new(0));
    let factory: PayloadStoreFactory = {
        let transport = transport.clone();
        let factory_calls = factory_calls.clone();
        Box::new(move || {
            factory_calls.fetch_add(1, Ordering::SeqCst);
            let store = DistributedPayloadStore::new(1, 1, vec![1], transport.clone())?;
            Ok(Arc::new(store) as Arc<dyn PayloadStore>)
        })
    };
    let group = ConsensusGroup::new(
        "warehouse/lazy-large",
        1,
        nodes[&1].clone(),
        state_machines[&1].clone(),
        factory,
    )
    .expect("group");

    let body = Bytes::from(vec![7u8; 4_097]);
    let error = group
        .commit(
            RequestId::from_u128(102),
            CommandKind::Object,
            ReplicationMode::Coded,
            body.clone(),
            None,
            &[1],
        )
        .await
        .expect_err("a large single-voter body must require valid payload geometry");
    assert!(matches!(
        error,
        GroupError::Payload(PayloadError::InvalidGeometry)
    ));

    let retry_error = group
        .commit(
            RequestId::from_u128(103),
            CommandKind::Object,
            ReplicationMode::Coded,
            body,
            None,
            &[1],
        )
        .await
        .expect_err("the cached factory error remains visible on later payload use");
    assert!(matches!(
        retry_error,
        GroupError::Payload(PayloadError::InvalidGeometry)
    ));
    assert_eq!(factory_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn four_voter_group_lazily_constructs_and_stages_coded_payloads() {
    let root = TempDir::new().expect("cluster directory");
    let (nodes, state_machines) = start_cluster(&root, 4).await;
    let transport = Arc::new(MemoryTransport::default());
    let factory_calls = Arc::new(AtomicUsize::new(0));
    let factory: PayloadStoreFactory = {
        let transport = transport.clone();
        let factory_calls = factory_calls.clone();
        Box::new(move || {
            factory_calls.fetch_add(1, Ordering::SeqCst);
            let store = DistributedPayloadStore::new(2, 2, vec![1, 2, 3, 4], transport.clone())?;
            Ok(Arc::new(store) as Arc<dyn PayloadStore>)
        })
    };
    let group = ConsensusGroup::new(
        "warehouse/lazy-coded",
        1,
        nodes[&1].clone(),
        state_machines[&1].clone(),
        factory,
    )
    .expect("group");
    let body = Bytes::from(vec![9u8; 5_000]);
    let committed = group
        .commit(
            RequestId::from_u128(104),
            CommandKind::Object,
            ReplicationMode::Coded,
            body.clone(),
            None,
            &[1, 2, 3, 4],
        )
        .await
        .expect("a valid coded geometry must stage and commit");

    assert_eq!(committed.outcome, AppliedOutcome::Committed);
    assert_eq!(factory_calls.load(Ordering::SeqCst), 1);
    assert!(transport.stored_count() >= 3);
    assert_eq!(group.read(committed.index).await.expect("coded read"), body);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn small_commit_performs_no_payload_staging_and_no_seal() {
    let root = TempDir::new().expect("cluster directory");
    let (nodes, state_machines) = start_cluster(&root, 1).await;
    let store: Arc<dyn PayloadStore> = Arc::new(NeverStageOrSeal);
    state_machines[&1]
        .attach_payload_store(store.clone())
        .await
        .expect("attach store");
    let group = ConsensusGroup::new(
        "warehouse/inline",
        1,
        nodes[&1].clone(),
        state_machines[&1].clone(),
        Box::new(move || Ok(Arc::clone(&store))),
    )
    .expect("group");

    let body = small_object_metadata();
    // The real object-commit call site always requests `Coded`
    // (`ObjectGroupSubmitter::submit_batch`); a small body must ignore that
    // request and go inline rather than erasure-code a few hundred bytes.
    let committed = group
        .commit(
            RequestId::from_u128(1),
            CommandKind::Object,
            ReplicationMode::Coded,
            body.clone(),
            None,
            &[1],
        )
        .await
        .expect("an inline commit must not touch the payload store");
    assert_eq!(committed.outcome, AppliedOutcome::Committed);

    let read_back = group
        .read(committed.index)
        .await
        .expect("an inline read must not touch the payload store");
    assert_eq!(read_back, body);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn large_commit_still_stages_and_seals_through_the_coded_path() {
    let root = TempDir::new().expect("cluster directory");
    let (nodes, state_machines) = start_cluster(&root, 2).await;
    let replicas = (1..=2)
        .map(|node| FilePayloadReplica::open(node, root.path().join(format!("{node}-payload"))))
        .collect::<Result<Vec<_>, _>>()
        .expect("payload replicas");
    let inner = Arc::new(PayloadSet::new(1, 1, replicas).expect("payload set"));
    let counts = CallCounts::default();
    let store: Arc<dyn PayloadStore> = Arc::new(RecordingPayloadStore {
        inner,
        counts: counts.clone(),
    });
    for state in state_machines.values() {
        state
            .attach_payload_store(store.clone())
            .await
            .expect("attach store");
    }
    let group = ConsensusGroup::new(
        "warehouse/inline",
        1,
        nodes[&1].clone(),
        state_machines[&1].clone(),
        Box::new(move || Ok(Arc::clone(&store))),
    )
    .expect("group");
    group
        .refresh_voters(vec![1, 2])
        .await
        .expect("publish payload voters");

    let body = Bytes::from(vec![7u8; 5_000]);
    let committed = group
        .commit(
            RequestId::from_u128(2),
            CommandKind::Object,
            ReplicationMode::Coded,
            body.clone(),
            None,
            &[1, 2],
        )
        .await
        .expect("large coded commit");
    assert_eq!(committed.outcome, AppliedOutcome::Committed);
    assert!(
        counts.stage.load(Ordering::SeqCst) >= 1,
        "a body over the inline threshold must still be staged"
    );
    assert!(
        counts.seal.load(Ordering::SeqCst) >= 1,
        "a body over the inline threshold must still be sealed"
    );

    assert_eq!(
        group
            .read(committed.index)
            .await
            .expect("coded reconstruction"),
        body
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inline_committed_entry_recovers_after_restart() {
    let root = TempDir::new().expect("cluster directory");
    let body = small_object_metadata();
    let (index, directory) = {
        let (nodes, state_machines) = start_cluster(&root, 1).await;
        let store: Arc<dyn PayloadStore> = Arc::new(NeverStageOrSeal);
        state_machines[&1]
            .attach_payload_store(store.clone())
            .await
            .expect("attach store");
        let group = ConsensusGroup::new(
            "warehouse/inline",
            1,
            nodes[&1].clone(),
            state_machines[&1].clone(),
            Box::new(move || Ok(Arc::clone(&store))),
        )
        .expect("group");
        let committed = group
            .commit(
                RequestId::from_u128(3),
                CommandKind::Object,
                ReplicationMode::Coded,
                body.clone(),
                None,
                &[1],
            )
            .await
            .expect("inline commit");
        let directory = root.path().join("1");
        nodes[&1].shutdown().await.expect("shutdown node");
        (committed.index, directory)
    };

    // Restart the node exactly as a restarted process would: reopen both
    // stores and start Raft without re-initializing. The state image is
    // written only by a snapshot, so recovering this entry proves Raft
    // replayed it from the durable log.
    let restarted_nodes = Arc::new(RwLock::new(BTreeMap::new()));
    let log = PersistentLogStore::open(directory.join("log.json"))
        .await
        .expect("reopen log store");
    let recovered = PersistentStateMachine::open(directory.join("state.json"))
        .await
        .expect("reopen state machine");
    let raft = Raft::new(
        1,
        Arc::new(
            openraft::Config {
                heartbeat_interval: 50,
                election_timeout_min: 150,
                election_timeout_max: 300,
                ..Default::default()
            }
            .validate()
            .expect("Raft config"),
        ),
        Router {
            nodes: restarted_nodes.clone(),
        },
        log,
        recovered.clone(),
    )
    .await
    .expect("restart Raft node");
    restarted_nodes.write().await.insert(1, raft);

    let header = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(header) = recovered.committed_header(index).await {
                return header;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("committed header is replayed from the log after restart");
    assert_eq!(
        header.inline_body(),
        Some(body.as_ref()),
        "the inline body itself must survive restart, not just its certificate"
    );
}

/// A replica that is not the leader must report that as a distinct, typed
/// condition, not as an opaque Raft string.
///
/// The caller forwards a command to whichever node its own replica last named
/// as leader. That view can be stale: by the time the command lands, the
/// target may no longer be the leader and may not yet know who is. The sender
/// can only re-resolve and retry if it can tell that case apart from a real
/// failure, so the error carries the condition and the leader hint.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_reports_not_leader_with_its_leader_hint() {
    let root = TempDir::new().expect("cluster directory");
    let (nodes, state_machines) = start_cluster(&root, 3).await;

    let leader = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(leader) = nodes[&1].current_leader().await {
                return leader;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("cluster elects a leader");
    let follower = (1..=3u64)
        .find(|node| *node != leader)
        .expect("a non-leader replica exists");

    let store: Arc<dyn PayloadStore> = Arc::new(NeverStageOrSeal);
    state_machines[&follower]
        .attach_payload_store(store.clone())
        .await
        .expect("attach store");
    let group = ConsensusGroup::new(
        "warehouse/inline",
        1,
        nodes[&follower].clone(),
        state_machines[&follower].clone(),
        Box::new(move || Ok(Arc::clone(&store))),
    )
    .expect("group");

    let error = group
        .commit(
            RequestId::from_u128(11),
            CommandKind::Object,
            ReplicationMode::Coded,
            small_object_metadata(),
            None,
            &[1],
        )
        .await
        .expect_err("a follower cannot commit");
    match error {
        GroupError::NotLeader { leader: hint } => assert_eq!(
            hint,
            Some(leader),
            "the rejection carries the leader the follower knows about"
        ),
        other => panic!("expected a typed not-leader rejection, got {other}"),
    }

    for raft in nodes.values() {
        raft.shutdown().await.expect("shutdown node");
    }
}
