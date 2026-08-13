//! Real five-node Raft agreement under leader isolation.

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use openraft::BasicNode;
use openraft::error::{InstallSnapshotError, NetworkError, RPCError, RemoteError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use sha2::Digest;
use tempfile::TempDir;
use tokio::sync::RwLock;
use verglas_consensus::{
    AppliedOutcome, CatalogAction, CatalogBatch, CatalogEntity, CatalogRequirement, CommandKind,
    ConsensusGroup, EntryHeader, FilePayloadReplica, PayloadCertificate, PayloadSet, PayloadStore,
    PersistentLogStore, PersistentStateMachine, RaftCommand, ReplicationMode, RequestId,
    SealRequest, VerglasRaftConfig,
};

type Raft = openraft::Raft<VerglasRaftConfig>;
type RaftError<E = openraft::error::Infallible> = openraft::error::RaftError<u64, E>;
type NetworkResult<T, E = openraft::error::Infallible> =
    Result<T, RPCError<u64, BasicNode, RaftError<E>>>;

#[derive(Clone, Default)]
struct Router {
    nodes: Arc<RwLock<BTreeMap<u64, Raft>>>,
    blocked: Arc<RwLock<BTreeSet<(u64, u64)>>>,
    source: u64,
}

impl Router {
    async fn block(&self, left: u64, right: u64) {
        let mut blocked = self.blocked.write().await;
        blocked.insert((left, right));
        blocked.insert((right, left));
    }

    async fn target<E: std::error::Error>(&self, target: u64) -> NetworkResult<Raft, E> {
        if self.blocked.read().await.contains(&(self.source, target)) {
            return Err(RPCError::Unreachable(Unreachable::new(&io::Error::new(
                io::ErrorKind::ConnectionRefused,
                "test partition",
            ))));
        }
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

impl RaftNetworkFactory<VerglasRaftConfig> for Router {
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

impl RaftNetwork<VerglasRaftConfig> for Connection {
    async fn append_entries(
        &mut self,
        request: AppendEntriesRequest<VerglasRaftConfig>,
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
        request: InstallSnapshotRequest<VerglasRaftConfig>,
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

fn command(request: u128) -> RaftCommand {
    command_with_hash(request, request as u8)
}

fn command_with_hash(request: u128, hash: u8) -> RaftCommand {
    RaftCommand::Commit(
        EntryHeader::new(
            "warehouse/a",
            1,
            CommandKind::Catalog,
            RequestId::from_u128(request),
            1,
            sha2::Sha256::digest([hash]).into(),
            None,
            PayloadCertificate::new(ReplicationMode::Coded, 3, 2, vec![1, 2, 3, 4, 5])
                .expect("certificate"),
        )
        .expect("header"),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn isolated_old_leader_cannot_commit_a_conflicting_index() {
    let root = TempDir::new().expect("cluster directory");
    let shared_nodes = Arc::new(RwLock::new(BTreeMap::new()));
    let blocked = Arc::new(RwLock::new(BTreeSet::new()));
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

    for node in 1..=5 {
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
                blocked: blocked.clone(),
                source: node,
            },
            log,
            state.clone(),
        )
        .await
        .expect("start Raft node");
        shared_nodes.write().await.insert(node, raft);
        state_machines.insert(node, state);
    }

    let members: BTreeMap<_, _> = (1..=5)
        .map(|node| (node, BasicNode::new(format!("node-{node}"))))
        .collect();
    let node1 = shared_nodes.read().await[&1].clone();
    node1.initialize(members).await.expect("initialize cluster");
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if state_machines[&1].committed_voters().await == BTreeSet::from([1, 2, 3, 4, 5]) {
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
    let first = node1.client_write(command(1)).await.expect("first commit");
    let first_index = first.log_id.index;
    let duplicate = node1
        .client_write(command(1))
        .await
        .expect("duplicate reaches state machine");
    assert_eq!(duplicate.data.index, first_index);
    assert_eq!(duplicate.data.outcome, AppliedOutcome::Duplicate);
    let conflict = node1
        .client_write(command_with_hash(1, 99))
        .await
        .expect("conflict reaches state machine");
    assert_eq!(conflict.data.outcome, AppliedOutcome::ConflictingRetry);

    let router = Router {
        nodes: shared_nodes.clone(),
        blocked: blocked.clone(),
        source: 0,
    };
    for peer in 2..=5 {
        router.block(1, peer).await;
    }
    let node2 = shared_nodes.read().await[&2].clone();
    node2
        .trigger()
        .elect()
        .await
        .expect("trigger replacement election");
    let replacement = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(leader) = node2.current_leader().await
                && leader != 1
            {
                break leader;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("replacement leader elected");
    let replacement_node = shared_nodes.read().await[&replacement].clone();
    let second = replacement_node
        .client_write(command(2))
        .await
        .expect("majority commit");
    assert!(second.log_id.index > first_index);

    let stale =
        tokio::time::timeout(Duration::from_millis(500), node1.client_write(command(3))).await;
    assert!(stale.is_err() || stale.expect("completed stale request").is_err());
    assert!(
        state_machines[&replacement]
            .committed_header(second.log_id.index)
            .await
            .is_some()
    );

    let payload_replicas = (1..=5)
        .map(|node| {
            FilePayloadReplica::open(node, root.path().join(node.to_string()).join("large"))
        })
        .collect::<Result<Vec<_>, _>>()
        .expect("payload replicas");
    let payloads = Arc::new(PayloadSet::new(3, 2, payload_replicas).expect("payload set"));
    let leader_group = ConsensusGroup::new(
        "warehouse/a",
        1,
        replacement_node.clone(),
        state_machines[&replacement].clone(),
        payloads.clone(),
    )
    .expect("leader group");
    let unrecovered = leader_group.catalog_table("missing").await;
    assert!(
        matches!(
            unrecovered,
            Err(verglas_consensus::GroupError::Payload(
                verglas_consensus::PayloadError::ReconstructionUnavailable
            ))
        ),
        "unexpected recovery result: {unrecovered:?}"
    );
    let first_staged = payloads
        .stage_local(
            RequestId::from_u128(1),
            "warehouse/a",
            1,
            ReplicationMode::Coded,
            &[1],
            &[1, 2, 3, 4, 5],
        )
        .expect("repair the pre-existing committed entry");
    let second_staged = payloads
        .stage_local(
            RequestId::from_u128(2),
            "warehouse/a",
            1,
            ReplicationMode::Coded,
            &[2],
            &[1, 2, 3, 4, 5],
        )
        .expect("repair the replacement committed entry");
    let first_log_id = state_machines[&replacement]
        .committed_log_id(first_index)
        .await
        .expect("first committed log identity");
    payloads
        .seal(SealRequest {
            hash: first_staged.hash(),
            group: "warehouse/a",
            configuration_generation: 1,
            request: RequestId::from_u128(1),
            term: first_log_id.leader_id.term,
            index: first_log_id.index,
            certificate: first_staged.certificate(),
        })
        .await
        .expect("seal first allocation");
    payloads
        .seal(SealRequest {
            hash: second_staged.hash(),
            group: "warehouse/a",
            configuration_generation: 1,
            request: RequestId::from_u128(2),
            term: second.log_id.leader_id.term,
            index: second.log_id.index,
            certificate: second_staged.certificate(),
        })
        .await
        .expect("seal replacement allocation");
    assert_eq!(
        leader_group
            .catalog_table("missing")
            .await
            .expect("recovered leader serves"),
        None
    );
    leader_group
        .repair_committed(vec![1, 2, 3, 4, 5])
        .await
        .expect("repair allocations through a distinct committed generation");
    assert_eq!(
        leader_group
            .catalog_table("missing")
            .await
            .expect("repaired leader serves from sealed replacement allocations"),
        None
    );
    let catalog_body = Bytes::from_static(b"authoritative catalog batch");
    let committed = leader_group
        .commit(
            RequestId::from_u128(50),
            CommandKind::Catalog,
            ReplicationMode::Coded,
            catalog_body.clone(),
            None,
            &[1, 2, 3, 4, 5],
        )
        .await
        .expect("stage and commit");
    assert_eq!(committed.outcome, AppliedOutcome::Committed);
    assert_eq!(
        leader_group
            .read(committed.index)
            .await
            .expect("linearizable reconstruction"),
        catalog_body
    );

    let opened = leader_group
        .open_timeline(RequestId::from_u128(61), 0x1000, &[1, 2, 3])
        .await
        .expect("initialize nonzero PostgreSQL WAL start");
    assert_eq!(opened.wal_end, Some(0x1000));
    let lease = leader_group
        .acquire_writer(RequestId::from_u128(51), "compute-a", &[1, 2, 3])
        .await
        .expect("writer lease");
    assert_eq!(lease.writer_epoch, Some(1));
    assert_eq!(lease.wal_end, Some(0x1000));
    let wal = leader_group
        .append_wal(
            RequestId::from_u128(52),
            1,
            0x1000,
            Bytes::from_static(b"wal"),
            &[1, 2, 3, 4, 5],
        )
        .await
        .expect("contiguous WAL append");
    assert_eq!(wal.wal_end, Some(0x1003));
    assert_eq!(
        leader_group
            .open_timeline(RequestId::from_u128(62), 0x1000, &[1, 2, 3])
            .await
            .expect("restart may reopen from its older redo boundary")
            .wal_end,
        Some(0x1003)
    );
    assert!(
        leader_group
            .open_timeline(RequestId::from_u128(63), 0x2000, &[1, 2, 3])
            .await
            .is_err()
    );
    assert_eq!(
        leader_group
            .read_wal(0x1000, 0x1003, wal.index)
            .await
            .expect("committed WAL read"),
        Bytes::from_static(b"wal")
    );
    assert!(
        leader_group
            .append_wal(
                RequestId::from_u128(53),
                1,
                0x1004,
                Bytes::from_static(b"gap"),
                &[1, 2, 3, 4, 5],
            )
            .await
            .is_err()
    );
    let checkpoint = leader_group
        .archive_checkpoint(
            RequestId::from_u128(54),
            0x1003,
            "s3://wal/segment-0",
            &[1, 2, 3],
        )
        .await
        .expect("archive checkpoint");
    assert_eq!(checkpoint.archive_lsn, Some(0x1003));
    leader_group
        .release_writer(RequestId::from_u128(57), 1, &[1, 2, 3])
        .await
        .expect("release writer");
    assert!(
        leader_group
            .append_wal(
                RequestId::from_u128(58),
                1,
                0x1003,
                Bytes::from_static(b"after release"),
                &[1, 2, 3, 4, 5],
            )
            .await
            .is_err()
    );

    let create_table = CatalogBatch::new(
        vec![CatalogRequirement::TableAbsent {
            table: "analytics.events".to_owned(),
        }],
        vec![
            CatalogAction::CreateNamespace {
                namespace: "analytics".to_owned(),
            },
            CatalogAction::PutTable {
                table: "analytics.events".to_owned(),
                metadata_location: "s3://warehouse/events/v1.metadata.json".to_owned(),
            },
        ],
    )
    .expect("catalog batch");
    leader_group
        .commit_catalog(RequestId::from_u128(55), create_table.clone(), &[1, 2, 3])
        .await
        .expect("catalog transaction");
    assert_eq!(
        leader_group
            .catalog_table("analytics.events")
            .await
            .expect("linearizable catalog read"),
        Some("s3://warehouse/events/v1.metadata.json".to_owned())
    );
    assert!(
        leader_group
            .commit_catalog(RequestId::from_u128(56), create_table, &[1, 2, 3])
            .await
            .is_err()
    );

    let create_hosted_namespace = CatalogBatch::new(
        vec![CatalogRequirement::RecordAbsent {
            entity: CatalogEntity::Namespace,
            id: "namespace/default/analytics".to_owned(),
        }],
        vec![CatalogAction::PutRecord {
            entity: CatalogEntity::Namespace,
            id: "namespace/default/analytics".to_owned(),
            document: r#"{"namespace":["analytics"],"properties":{}}"#.to_owned(),
        }],
    )
    .expect("hosted namespace batch");
    leader_group
        .commit_catalog(
            RequestId::from_u128(70),
            create_hosted_namespace,
            &[1, 2, 3],
        )
        .await
        .expect("hosted namespace transaction persists");
    assert_eq!(
        leader_group
            .catalog_record(CatalogEntity::Namespace, "namespace/default/analytics")
            .await
            .expect("linearizable hosted namespace read"),
        Some(r#"{"namespace":["analytics"],"properties":{}}"#.to_owned())
    );

    assert_eq!(
        leader_group
            .register_warehouse(
                RequestId::from_u128(59),
                "analytics".to_owned(),
                "warehouse/tenant-a/analytics".to_owned(),
                &[1, 2, 3],
            )
            .await
            .expect("register tenant warehouse"),
        Some("warehouse/tenant-a/analytics".to_owned())
    );
    assert!(
        leader_group
            .register_warehouse(
                RequestId::from_u128(60),
                "analytics".to_owned(),
                "warehouse/tenant-a/conflict".to_owned(),
                &[1, 2, 3],
            )
            .await
            .is_err()
    );

    let isolated_group = ConsensusGroup::new(
        "warehouse/a",
        1,
        node1.clone(),
        state_machines[&1].clone(),
        payloads,
    )
    .expect("isolated group");
    assert!(isolated_group.read(committed.index).await.is_err());
    assert!(
        isolated_group
            .read_wal(0x1000, 0x1003, wal.index)
            .await
            .is_err()
    );

    // A replacement voter must first be a caught-up learner. OpenRaft commits
    // the joint and uniform configurations; the state machine exposes only the
    // final committed configuration, never a process-local desired set.
    let directory = root.path().join("6");
    let log = PersistentLogStore::open(directory.join("log.json"))
        .await
        .expect("learner log store");
    let learner_state = PersistentStateMachine::open(directory.join("state.json"))
        .await
        .expect("learner state machine");
    let learner = Raft::new(
        6,
        config.clone(),
        Router {
            nodes: shared_nodes.clone(),
            blocked: blocked.clone(),
            source: 6,
        },
        log,
        learner_state.clone(),
    )
    .await
    .expect("start learner");
    shared_nodes.write().await.insert(6, learner);
    replacement_node
        .add_learner(6, BasicNode::new("node-6"), true)
        .await
        .expect("catch up learner before membership transition");
    replacement_node
        .change_membership(BTreeSet::from([1, 2, 3, 4, 6]), false)
        .await
        .expect("commit joint then uniform membership");
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if learner_state.committed_voters().await == BTreeSet::from([1, 2, 3, 4, 6]) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("learner applies the uniform membership");

    for raft in shared_nodes.read().await.values() {
        raft.shutdown().await.expect("shutdown node");
    }
    let recovered =
        PersistentStateMachine::open(root.path().join(replacement.to_string()).join("state.json"))
            .await
            .expect("reopen tenant-root state");
    assert_eq!(
        recovered.warehouse_group("analytics").await,
        Some("warehouse/tenant-a/analytics".to_owned())
    );
}
