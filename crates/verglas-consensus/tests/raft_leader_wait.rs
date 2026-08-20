//! Proves the write hot path's leader resolution is event-driven, not a
//! fixed-interval poll (#164).
//!
//! `ConsensusPlane::submit_with_timeout` (bins/cache-node/src/consensus.rs)
//! used to loop on a flat 25ms `tokio::time::sleep` until a leader appeared or
//! a forwarded command succeeded. `ConsensusGroup::await_leader` is the
//! primitive that replaced it: it blocks on OpenRaft's own metrics-change
//! signal, bounded by a caller-supplied timeout, and never sleeps on a fixed
//! tick. These tests exercise that primitive directly, since it is the exact
//! mechanism `submit_with_timeout` now delegates to for leader resolution
//! (one `await_leader` call, then at most one execute-or-forward attempt).

use std::collections::BTreeMap;
use std::io;
use std::sync::Arc;
use std::time::{Duration, Instant};

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
    ConsensusGroup, FilePayloadReplica, PayloadSet, PersistentLogStore, PersistentStateMachine,
    VerglasRaftConfig,
};

type Raft = openraft::Raft<VerglasRaftConfig>;
type RaftError<E = openraft::error::Infallible> = openraft::error::RaftError<u64, E>;
type NetworkResult<T, E = openraft::error::Infallible> =
    Result<T, RPCError<u64, BasicNode, RaftError<E>>>;

/// A single-voter loopback network. No test here ever needs a second voter:
/// a lone node's own election never crosses the wire, so this only exists to
/// satisfy `Raft::new`'s network-factory bound.
#[derive(Clone, Default)]
struct LoopbackRouter {
    nodes: Arc<RwLock<BTreeMap<u64, Raft>>>,
}

impl RaftNetworkFactory<VerglasRaftConfig> for LoopbackRouter {
    type Network = LoopbackConnection;

    async fn new_client(&mut self, target: u64, _node: &BasicNode) -> Self::Network {
        LoopbackConnection {
            router: self.clone(),
            target,
        }
    }
}

struct LoopbackConnection {
    router: LoopbackRouter,
    target: u64,
}

impl LoopbackConnection {
    async fn target<E: std::error::Error>(&self) -> NetworkResult<Raft, E> {
        self.router
            .nodes
            .read()
            .await
            .get(&self.target)
            .cloned()
            .ok_or_else(|| {
                RPCError::Network(NetworkError::new(&io::Error::new(
                    io::ErrorKind::NotFound,
                    "unknown test node",
                )))
            })
    }
}

impl RaftNetwork<VerglasRaftConfig> for LoopbackConnection {
    async fn append_entries(
        &mut self,
        request: AppendEntriesRequest<VerglasRaftConfig>,
        _option: RPCOption,
    ) -> NetworkResult<AppendEntriesResponse<u64>> {
        let target = self.target().await?;
        target
            .append_entries(request)
            .await
            .map_err(|error| RPCError::RemoteError(RemoteError::new(self.target, error)))
    }

    async fn install_snapshot(
        &mut self,
        request: InstallSnapshotRequest<VerglasRaftConfig>,
        _option: RPCOption,
    ) -> NetworkResult<InstallSnapshotResponse<u64>, InstallSnapshotError> {
        let target = self.target().await?;
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
        let target = self.target().await?;
        target
            .vote(request)
            .await
            .map_err(|error| RPCError::RemoteError(RemoteError::new(self.target, error)))
    }
}

/// A short, deterministic election profile so tests complete quickly.
fn fast_election_config() -> Arc<openraft::Config> {
    Arc::new(
        openraft::Config {
            heartbeat_interval: 20,
            election_timeout_min: 40,
            election_timeout_max: 80,
            ..Default::default()
        }
        .validate()
        .expect("valid Raft timing"),
    )
}

/// Opens one durable single-voter Raft plus the `ConsensusGroup` in front of
/// it. Membership is never initialized here: the caller decides whether and
/// when this node becomes a leader.
async fn open_single_voter(
    root: &std::path::Path,
    node: u64,
) -> (LoopbackRouter, Raft, ConsensusGroup) {
    let router = LoopbackRouter::default();
    let directory = root.join(node.to_string());
    let log = PersistentLogStore::open(directory.join("log.json"))
        .await
        .expect("open durable log");
    let state = PersistentStateMachine::open(directory.join("state.json"))
        .await
        .expect("open durable state machine");
    let raft = Raft::new(
        node,
        fast_election_config(),
        router.clone(),
        log,
        state.clone(),
    )
    .await
    .expect("start Raft node");
    router.nodes.write().await.insert(node, raft.clone());

    // `ConsensusGroup::new` requires a payload store even though neither test
    // here stages a payload; k=1/m=1 is the minimal valid geometry.
    let replicas = vec![
        FilePayloadReplica::open(node, directory.join("payload-a")).expect("payload replica a"),
        FilePayloadReplica::open(node + 1000, directory.join("payload-b"))
            .expect("payload replica b"),
    ];
    let payloads = Arc::new(PayloadSet::new(1, 1, replicas).expect("payload set"));
    state
        .attach_payload_store(payloads.clone())
        .await
        .expect("attach payload store");

    let group = ConsensusGroup::new("leader-wait-test", 1, raft.clone(), state, payloads)
        .expect("build consensus group");
    (router, raft, group)
}

/// A group that never elects a leader must return `None` once its bounded
/// wait elapses, not hang or spin. `ConsensusPlane::submit_with_timeout`
/// turns this `None` into the caller-visible error; this test proves the
/// underlying wait itself is bounded and terminates cleanly.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn await_leader_on_a_leaderless_group_returns_none_within_its_bound() {
    let root = TempDir::new().expect("test directory");
    let (_router, _raft, group) = open_single_voter(root.path(), 1).await;

    let timeout = Duration::from_millis(150);
    let start = Instant::now();
    let leader = group
        .await_leader(timeout)
        .await
        .expect("await_leader does not error on a plain timeout");
    let elapsed = start.elapsed();

    assert_eq!(leader, None, "no membership was ever initialized");
    // Bounded: it must not return long before the deadline (that would mean
    // it gave up early) or long after it (that would mean it kept spinning
    // past the caller's budget instead of returning a real answer).
    assert!(
        elapsed >= timeout,
        "returned after {elapsed:?}, before its {timeout:?} bound was reached"
    );
    assert!(
        elapsed < timeout + Duration::from_millis(150),
        "returned after {elapsed:?}, well past its {timeout:?} bound; \
         a fixed-interval retry loop can overshoot like this, an event wait must not"
    );
}

/// When a leader is already known at call time, resolving it must cost
/// nothing but a channel read. The polling design this replaces slept a flat
/// 25ms per iteration before ever rechecking Raft state; this asserts the
/// replacement returns effectively instantly instead.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn await_leader_returns_immediately_once_a_leader_is_already_known() {
    let root = TempDir::new().expect("test directory");
    let (_router, raft, group) = open_single_voter(root.path(), 1).await;

    let members = BTreeMap::from([(1, BasicNode::new("node-1"))]);
    raft.initialize(members)
        .await
        .expect("initialize single-voter cluster");
    raft.wait(Some(Duration::from_secs(5)))
        .current_leader(1, "initial election")
        .await
        .expect("single voter elects itself");

    let start = Instant::now();
    let leader = group
        .await_leader(Duration::from_secs(25))
        .await
        .expect("await_leader succeeds");
    let elapsed = start.elapsed();

    assert_eq!(leader, Some(1));
    assert!(
        elapsed < Duration::from_millis(10),
        "took {elapsed:?} to report an already-known leader; \
         a timer-based wait would add up to its full poll tick here"
    );
}
