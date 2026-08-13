//! Multi-Raft group lifecycle hosted by one cache-node process.
//!
//! Group creation opens durable local state concurrently on explicit voters,
//! requires a Raft quorum, then asks the smallest successful voter to initialize
//! Raft exactly once. The ring's gossip membership never changes these voter sets.

use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use futures::future::join_all;
use openraft::BasicNode;
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;
use verglas_cluster::{
    BootstrapGroupFn, GroupCommandFn, MembershipReplace, MembershipReplaceFn, OpenGroupFn,
    RaftHttpTransport,
};
use verglas_consensus::{
    ConsensusGroup, DistributedPayloadStore, GroupRequest, GroupResponse, PersistentLogStore,
    PersistentStateMachine, ReplicationMode, VerglasRaftConfig,
};
use verglas_write::{ConsensusCommitter, ObjectCommit, StagedObject};

use crate::ring::RingPlane;

type Raft = openraft::Raft<VerglasRaftConfig>;
type PlaneError = Box<dyn std::error::Error + Send + Sync>;
/// Bounds one peer's group-open request so an unreachable voter cannot block a quorum.
const GROUP_OPEN_TIMEOUT: Duration = Duration::from_secs(5);

/// Maps a stable fragment holder identity to the corresponding Raft voter id.
fn numeric_node_id(node: &str) -> u64 {
    node.bytes().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3)
    })
}

/// Process-local registry for the many groups hosted on the cache fleet.
pub struct ConsensusPlane {
    root: PathBuf,
    ring: Arc<RingPlane>,
    k: usize,
    m: usize,
    groups: Mutex<BTreeMap<String, LocalGroup>>,
}

/// Commits staged immutable-object certificates through per-object Raft groups.
pub struct ObjectConsensusCommitter {
    plane: Arc<ConsensusPlane>,
}

impl ObjectConsensusCommitter {
    /// Binds S3 write-back publication to the process's universal Multi-Raft plane.
    pub fn new(plane: Arc<ConsensusPlane>) -> Self {
        Self { plane }
    }
}

#[async_trait::async_trait]
impl ConsensusCommitter for ObjectConsensusCommitter {
    async fn commit(&self, staged: StagedObject) -> Result<ObjectCommit, String> {
        let identity = serde_json::json!({
            "storage_binding": staged.key.storage_binding_id,
            "bucket": staged.key.bucket,
            "key": staged.key.key,
            "object_id": staged.object_id,
            "object_len": staged.object_len,
            "payload_hash": hex::encode(staged.payload_hash),
            "k": staged.geometry.k,
            "m": staged.geometry.m,
            "chunk": staged.geometry.chunk,
            "placements": staged.placements.iter().map(|placement| {
                serde_json::json!({"index": placement.index, "node": placement.node})
            }).collect::<Vec<_>>(),
        });
        let payload = serde_json::to_vec(&identity).map_err(|error| error.to_string())?;
        let digest = Sha256::digest(&payload);
        let request_id = u128::from_be_bytes(
            digest[..16]
                .try_into()
                .map_err(|_| "object request identity is malformed".to_owned())?,
        );
        let group = format!("object/{}", hex::encode(Sha256::digest(&payload[..])));
        let holders = staged
            .placements
            .iter()
            .map(|placement| numeric_node_id(&placement.node))
            .collect();
        match self
            .plane
            .submit(
                &group,
                GroupRequest::CommitObject {
                    request_id,
                    payload,
                    holders,
                    mode: ReplicationMode::Coded,
                },
            )
            .await
            .map_err(|error| error.to_string())?
        {
            GroupResponse::Applied(response) => Ok(ObjectCommit {
                index: response.index,
            }),
            _ => Err("object group returned a non-applied response".to_owned()),
        }
    }
}

struct LocalGroup {
    raft: Raft,
    state_machine: PersistentStateMachine,
    group: Arc<ConsensusGroup>,
}

impl ConsensusPlane {
    /// Promotes a fully caught-up learner after repairing every committed payload allocation.
    ///
    /// This must execute on the observed leader; callers retry through that
    /// leader rather than performing an unsafe local membership mutation.
    pub async fn replace_voter(
        self: &Arc<Self>,
        group: &str,
        remove: u64,
        candidate: u64,
        candidate_node: String,
        address: SocketAddr,
        target_voters: Vec<u64>,
    ) -> Result<(), PlaneError> {
        if target_voters.len() != self.k + self.m
            || target_voters.iter().copied().collect::<BTreeSet<_>>().len() != target_voters.len()
            || !target_voters.contains(&candidate)
        {
            return Err("target voter geometry is invalid".into());
        }
        self.ring.register_raft_address(candidate, address)?;
        self.ring
            .register_payload_peer(candidate, candidate_node.clone())?;
        let local = self.ensure_group(group).await?;
        match local.leader_id().await {
            Some(leader) if leader != self.ring.safekeeper_id() => {
                return self
                    .network(group)?
                    .membership_replace(
                        leader,
                        MembershipReplace {
                            group: group.to_owned(),
                            remove,
                            add: candidate,
                            node_id: candidate_node,
                            address,
                        },
                    )
                    .await
                    .map_err(Into::into);
            }
            Some(_) => {}
            None => return Err("membership replacement has no observed leader".into()),
        }
        let network = self.network(group)?;
        network.open_group(candidate).await?;
        let groups = self.groups.lock().await;
        let replica = groups
            .get(group)
            .ok_or("group disappeared during membership change")?;
        replica
            .raft
            .add_learner(candidate, BasicNode::new(candidate.to_string()), true)
            .await?;
        local.repair_committed(target_voters.clone()).await?;
        replica
            .raft
            .change_membership(BTreeSet::from_iter(target_voters.clone()), false)
            .await?;
        self.ring.set_payload_voters(&target_voters)?;
        local.refresh_voters(target_voters.clone()).await?;
        local.refresh_configuration_generation(replica.state_machine.membership_generation().await);
        Ok(())
    }

    /// Returns the durable committed voter set for lifecycle request validation.
    pub async fn committed_voters(&self, group: &str) -> Result<BTreeSet<u64>, PlaneError> {
        let local = self.open_local(group).await?;
        let groups = self.groups.lock().await;
        let replica = groups
            .get(group)
            .ok_or("group disappeared while reading membership")?;
        let _ = local;
        Ok(replica.state_machine.committed_voters().await)
    }

    /// Refuses process stop while this node remains a committed voter of any hosted group.
    pub async fn relinquished_local_voter(&self) -> Result<(), PlaneError> {
        let groups = self.groups.lock().await;
        let self_id = self.ring.safekeeper_id();
        for (group, replica) in groups.iter() {
            if replica
                .state_machine
                .committed_voters()
                .await
                .contains(&self_id)
            {
                return Err(format!("local node remains a committed voter for `{group}`").into());
            }
        }
        Ok(())
    }

    /// Creates the process registry and installs peer provisioning callbacks.
    pub async fn new(
        root: PathBuf,
        ring: Arc<RingPlane>,
        k: usize,
        m: usize,
    ) -> Result<Arc<Self>, PlaneError> {
        let plane = Arc::new(Self {
            root: root.join("consensus"),
            ring,
            k,
            m,
            groups: Mutex::new(BTreeMap::new()),
        });
        let open_plane = Arc::clone(&plane);
        let opener: OpenGroupFn = Arc::new(move |group| {
            let plane = Arc::clone(&open_plane);
            Box::pin(async move {
                plane
                    .open_local(&group)
                    .await
                    .map(|_| ())
                    .map_err(|e| e.to_string())
            })
        });
        plane.ring.raft_registry().set_opener(opener).await;
        let bootstrap_plane = Arc::clone(&plane);
        let bootstrapper: BootstrapGroupFn = Arc::new(move |group| {
            let plane = Arc::clone(&bootstrap_plane);
            Box::pin(async move {
                plane
                    .bootstrap_local(&group)
                    .await
                    .map_err(|e| e.to_string())
            })
        });
        plane
            .ring
            .raft_registry()
            .set_bootstrapper(bootstrapper)
            .await;
        let command_plane = Arc::clone(&plane);
        let commander: GroupCommandFn = Arc::new(move |group, body| {
            let plane = Arc::clone(&command_plane);
            Box::pin(async move { plane.execute_local(&group, &body).await })
        });
        plane.ring.raft_registry().set_commander(commander).await;
        let membership_plane = Arc::clone(&plane);
        let replacer: MembershipReplaceFn = Arc::new(move |request| {
            let plane = Arc::clone(&membership_plane);
            Box::pin(async move {
                let mut voters = plane
                    .committed_voters(&request.group)
                    .await
                    .map_err(|error| error.to_string())?;
                if !voters.remove(&request.remove) || voters.contains(&request.add) {
                    return Err("request is not one committed voter replacement".to_owned());
                }
                voters.insert(request.add);
                plane
                    .replace_voter(
                        &request.group,
                        request.remove,
                        request.add,
                        request.node_id,
                        request.address,
                        voters.into_iter().collect(),
                    )
                    .await
                    .map_err(|error| error.to_string())
            })
        });
        plane
            .ring
            .raft_registry()
            .set_membership_replacer(replacer)
            .await;
        Ok(plane)
    }

    /// Returns an initialized local group or provisions a Raft quorum on first use.
    ///
    /// An initialized local replica skips fleet provisioning entirely. First
    /// creation opens voters concurrently and bounded per peer; a down voter is
    /// left for a later request, while a minority fails before bootstrap can
    /// form a group without a Raft majority.
    pub async fn ensure_group(
        self: &Arc<Self>,
        group: &str,
    ) -> Result<Arc<ConsensusGroup>, PlaneError> {
        validate_group(group)?;
        if let Some(local) = self.initialized_local_group(group).await {
            return Ok(local);
        }
        let network = self.network(group)?;
        let voters = self.ring.consensus_voters();
        if voters.is_empty() {
            return Err("consensus group has no voters".into());
        }
        let opened = open_voters(&network, voters).await?;
        let bootstrap = opened
            .iter()
            .next()
            .copied()
            .ok_or("no voter opened the consensus group")?;
        network.bootstrap_group(bootstrap).await?;
        self.open_local(group).await
    }

    /// Returns this ingress's initialized replica without creating local state.
    async fn initialized_local_group(&self, group: &str) -> Option<Arc<ConsensusGroup>> {
        let (group, state_machine) = {
            let groups = self.groups.lock().await;
            let local = groups.get(group)?;
            (Arc::clone(&local.group), local.state_machine.clone())
        };
        state_machine.is_initialized().await.then_some(group)
    }

    /// Routes a typed request from any ingress to the currently observed leader.
    pub async fn submit(
        self: &Arc<Self>,
        group: &str,
        request: GroupRequest,
    ) -> Result<GroupResponse, PlaneError> {
        let local = self.ensure_group(group).await?;
        let leader = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if let Some(leader) = local.leader_id().await {
                    break leader;
                }
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
        })
        .await?;
        if leader == self.ring.safekeeper_id() {
            return Ok(local.execute(request).await?);
        }
        let encoded = serde_json::to_vec(&request)?;
        let response = self.network(group)?.command(leader, encoded).await?;
        Ok(serde_json::from_slice(&response)?)
    }

    /// Returns the locally opened authoritative group names in deterministic order.
    ///
    /// A lifecycle coordinator uses this inventory after closing admission; it
    /// never guesses groups from cache-ring membership or client traffic.
    pub async fn hosted_groups(&self) -> Vec<String> {
        self.groups.lock().await.keys().cloned().collect()
    }

    /// Opens one persistent local replica idempotently without initializing it.
    async fn open_local(&self, group: &str) -> Result<Arc<ConsensusGroup>, PlaneError> {
        validate_group(group)?;
        let mut groups = self.groups.lock().await;
        if let Some(local) = groups.get(group) {
            return Ok(Arc::clone(&local.group));
        }
        let directory = self
            .root
            .join(hex::encode(Sha256::digest(group.as_bytes())));
        std::fs::create_dir_all(&directory)?;
        let log = PersistentLogStore::open(directory.join("raft-log.json")).await?;
        let state_machine = PersistentStateMachine::open(directory.join("state.json")).await?;
        let committed_voters = state_machine.committed_voters().await;
        let voters = if committed_voters.len() == self.k + self.m {
            committed_voters.into_iter().collect()
        } else {
            self.ring.consensus_voters()
        };
        let payloads = Arc::new(DistributedPayloadStore::new(
            self.k,
            self.m,
            voters,
            self.ring.consensus_payload_transport(),
        )?);
        state_machine.attach_payload_store(payloads.clone()).await?;
        let raft = Raft::new(
            self.ring.safekeeper_id(),
            Arc::new(openraft::Config::default().validate()?),
            self.network(group)?,
            log,
            state_machine.clone(),
        )
        .await?;
        self.ring
            .raft_registry()
            .register(group, self.ring.safekeeper_id(), raft.clone())
            .await;
        let generation = state_machine.membership_generation().await.max(1);
        let consensus = Arc::new(ConsensusGroup::new(
            group,
            generation,
            raft.clone(),
            state_machine.clone(),
            payloads,
        )?);
        groups.insert(
            group.to_owned(),
            LocalGroup {
                raft,
                state_machine,
                group: Arc::clone(&consensus),
            },
        );
        Ok(consensus)
    }

    /// Initializes one fully opened group if persistent membership is still empty.
    async fn bootstrap_local(&self, group: &str) -> Result<(), PlaneError> {
        let _ = self.open_local(group).await?;
        let groups = self.groups.lock().await;
        let local = groups
            .get(group)
            .ok_or("group disappeared during bootstrap")?;
        if local.state_machine.is_initialized().await {
            return Ok(());
        }
        let members: BTreeMap<u64, BasicNode> = self
            .ring
            .consensus_voters()
            .into_iter()
            .map(|voter| (voter, BasicNode::new(voter.to_string())))
            .collect();
        local.raft.initialize(members).await?;
        Ok(())
    }

    /// Executes a forwarded command only when this replica currently leads.
    async fn execute_local(&self, group: &str, body: &[u8]) -> Result<Vec<u8>, String> {
        let local = self
            .open_local(group)
            .await
            .map_err(|error| error.to_string())?;
        if local.leader_id().await != Some(self.ring.safekeeper_id()) {
            return Err("forwarded command reached a non-leader".to_owned());
        }
        let request: GroupRequest =
            serde_json::from_slice(body).map_err(|error| error.to_string())?;
        let response = local
            .execute(request)
            .await
            .map_err(|error| error.to_string())?;
        serde_json::to_vec(&response).map_err(|error| error.to_string())
    }

    /// Creates the group-specific HTTP/2 OpenRaft transport.
    fn network(
        &self,
        group: &str,
    ) -> Result<RaftHttpTransport<verglas_cluster::StaticRaftAddressResolver>, reqwest::Error> {
        RaftHttpTransport::new(
            group,
            self.ring.raft_addresses(),
            self.ring.raft_secret().to_owned(),
        )
    }
}

/// Opens every configured voter concurrently and returns the successful Raft quorum.
///
/// Each request has an independent hard deadline.  The selected bootstrap is
/// the lowest successful voter, making initialization deterministic without
/// making it depend on a specific unavailable node.
async fn open_voters(
    network: &RaftHttpTransport<verglas_cluster::StaticRaftAddressResolver>,
    voters: Vec<u64>,
) -> Result<BTreeSet<u64>, PlaneError> {
    let quorum = voters.len() / 2 + 1;
    let openings = voters.into_iter().map(|voter| async move {
        (
            voter,
            tokio::time::timeout(GROUP_OPEN_TIMEOUT, network.open_group(voter)).await,
        )
    });
    let opened = join_all(openings)
        .await
        .into_iter()
        .filter_map(|(voter, result)| result.ok().and_then(Result::ok).map(|()| voter))
        .collect::<BTreeSet<_>>();
    if opened.len() < quorum {
        return Err(format!(
            "opened {} consensus voters, but a Raft quorum of {quorum} is required",
            opened.len()
        )
        .into());
    }
    Ok(opened)
}

/// Rejects invalid dynamic group identities before they reach persistent storage.
fn validate_group(group: &str) -> Result<(), PlaneError> {
    if group.is_empty() || group.len() > 512 {
        Err("consensus group identity is empty or too long".into())
    } else {
        Ok(())
    }
}
