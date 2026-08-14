//! Multi-Raft group lifecycle hosted by one cache-node process.
//!
//! Group creation opens durable local state concurrently on explicit voters,
//! requires a Raft quorum, then asks the smallest successful voter to initialize
//! Raft exactly once. The ring's gossip membership never changes these voter sets.

use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    net::SocketAddr,
    path::PathBuf,
    sync::{Arc, Mutex as StdMutex, mpsc},
    thread,
    time::Duration,
};

use futures::future::join_all;
use openraft::BasicNode;
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;
use tokio::sync::oneshot;
use verglas_cluster::{
    BootstrapGroupFn, GroupCommandFn, MembershipReplace, MembershipReplaceFn, OpenGroupFn,
    RaftHttpTransport,
};
use verglas_consensus::{
    ConsensusGroup, DistributedPayloadStore, GroupError, GroupRequest, GroupResponse,
    PersistentLogStore, PersistentStateMachine, ReplicationMode, VerglasRaftConfig,
};
use verglas_writeback::{ConsensusCommitter, ObjectCommit, StagedObject};

use crate::ring::RingPlane;

type Raft = openraft::Raft<VerglasRaftConfig>;
type PlaneError = Box<dyn std::error::Error + Send + Sync>;
/// Bounds one peer's group-open request so an unreachable voter cannot block a quorum.
const GROUP_OPEN_TIMEOUT: Duration = Duration::from_secs(5);
/// Caps a client submission without undercutting the latest legal ranked
/// election window after a leader loss.
const GROUP_SUBMIT_TIMEOUT: Duration = Duration::from_secs(25);
/// Raft heartbeat interval for fsynced multi-megabyte WAL replication.
///
/// This leaves room for compact Raft metadata fsync under canonical 8 MiB WAL
/// traffic without treating an overloaded live voter as dead.
const RAFT_HEARTBEAT_INTERVAL_MS: u64 = 250;
/// The first voter may start an election after four missed heartbeats.
///
/// The ranked windows must also leave enough of the five-second WAL ingress
/// deadline for the successor's current-term ReadIndex fence and exact
/// reconstruction. A one-voter loss always leaves rank zero or one alive.
const RAFT_ELECTION_FIRST_TIMEOUT_MS: u64 = 1_000;
/// Each later stable voter receives a non-overlapping campaign slot.
const RAFT_ELECTION_RANK_STEP_MS: u64 = 750;
/// Election window width. It stays smaller than the rank step so candidate
/// campaigns never overlap in a healthy deployment.
const RAFT_ELECTION_WINDOW_MS: u64 = 250;

/// Maps a stable fragment holder identity to the corresponding Raft voter id.
fn numeric_node_id(node: &str) -> u64 {
    node.bytes().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3)
    })
}

/// Builds one node's fixed ranked election profile from its committed voters.
///
/// The peer-RPC boundary is where a future deployment may negotiate timing
/// during version skew. This prototype has one fixed profile: every node sorts
/// the same durable voter identities and gives its rank a distinct campaign
/// slot after a leader loss.
fn raft_config(voters: &[u64], local: u64) -> Result<openraft::Config, PlaneError> {
    let mut ranks = voters.to_vec();
    ranks.sort_unstable();
    ranks.dedup();
    if ranks.len() != voters.len() {
        return Err("consensus voter configuration contains duplicate identities".into());
    }
    let rank = ranks
        .binary_search(&local)
        .map_err(|_| "local cache node is not a configured consensus voter")?;
    let rank = u64::try_from(rank).map_err(|_| "consensus voter rank exceeds u64")?;
    let offset = rank
        .checked_mul(RAFT_ELECTION_RANK_STEP_MS)
        .ok_or("consensus voter rank overflows election timing")?;
    let minimum = RAFT_ELECTION_FIRST_TIMEOUT_MS
        .checked_add(offset)
        .ok_or("consensus election timing overflows")?;
    let maximum = minimum
        .checked_add(RAFT_ELECTION_WINDOW_MS)
        .ok_or("consensus election timing overflows")?;
    openraft::Config {
        heartbeat_interval: RAFT_HEARTBEAT_INTERVAL_MS,
        election_timeout_min: minimum,
        election_timeout_max: maximum,
        ..Default::default()
    }
    .validate()
    .map_err(Into::into)
}

/// Owns the one-thread Tokio scheduler on which OpenRaft creates its core and replication tasks.
///
/// OpenRaft uses the currently entered Tokio runtime in [`Raft::new`].  The
/// public HTTP runtime must therefore never create a Raft instance: a slow
/// public request must not postpone vote processing, heartbeats, or replication.
struct ConsensusRuntime {
    handle: tokio::runtime::Handle,
    shutdown: StdMutex<Option<oneshot::Sender<()>>>,
    thread: StdMutex<Option<thread::JoinHandle<()>>>,
}

impl ConsensusRuntime {
    /// Starts an owned multi-worker runtime and waits until it can accept group creation.
    fn start() -> Result<Arc<Self>, PlaneError> {
        let (ready, started) = mpsc::sync_channel(1);
        let (shutdown, stopping) = oneshot::channel();
        let thread = thread::Builder::new()
            .name("verglas-consensus".to_owned())
            .spawn(move || {
                let runtime = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(runtime) => runtime,
                    Err(error) => {
                        let _ = ready.send(Err(error.to_string()));
                        return;
                    }
                };
                if ready.send(Ok(runtime.handle().clone())).is_err() {
                    return;
                }
                runtime.block_on(async move {
                    let _ = stopping.await;
                });
            })
            .map_err(|error| format!("start consensus runtime: {error}"))?;
        let handle = match started.recv() {
            Ok(Ok(handle)) => handle,
            Ok(Err(error)) => {
                let _ = thread.join();
                return Err(format!("initialize consensus runtime: {error}").into());
            }
            Err(error) => {
                let _ = thread.join();
                return Err(format!("consensus runtime stopped during startup: {error}").into());
            }
        };
        Ok(Arc::new(Self {
            handle,
            shutdown: StdMutex::new(Some(shutdown)),
            thread: StdMutex::new(Some(thread)),
        }))
    }

    /// Runs work on the owned scheduler and returns task or runtime failures to the caller.
    async fn run<T, F>(&self, work: F) -> Result<T, PlaneError>
    where
        T: Send + 'static,
        F: Future<Output = Result<T, PlaneError>> + Send + 'static,
    {
        self.handle
            .spawn(work)
            .await
            .map_err(|error| format!("consensus runtime task stopped: {error}"))?
    }

    /// Stops the scheduler after its Raft cores have been shut down.
    async fn shutdown(&self) -> Result<(), PlaneError> {
        if let Some(shutdown) = self
            .shutdown
            .lock()
            .map_err(|_| "consensus runtime shutdown lock is poisoned")?
            .take()
        {
            let _ = shutdown.send(());
        }
        let thread = self
            .thread
            .lock()
            .map_err(|_| "consensus runtime thread lock is poisoned")?
            .take();
        if let Some(thread) = thread {
            tokio::task::spawn_blocking(move || thread.join())
                .await
                .map_err(|error| format!("join consensus runtime task: {error}"))?
                .map_err(|_| "consensus runtime thread panicked")?;
        }
        Ok(())
    }
}

impl Drop for ConsensusRuntime {
    /// Releases the runtime thread if startup later fails before normal shutdown.
    fn drop(&mut self) {
        if let Ok(shutdown) = self.shutdown.get_mut()
            && let Some(shutdown) = shutdown.take()
        {
            let _ = shutdown.send(());
        }
        if let Ok(thread) = self.thread.get_mut()
            && let Some(thread) = thread.take()
        {
            let _ = thread::Builder::new()
                .name("verglas-consensus-join".to_owned())
                .spawn(move || {
                    let _ = thread.join();
                });
        }
    }
}

/// Process-local registry for the many groups hosted on the cache fleet.
pub struct ConsensusPlane {
    root: PathBuf,
    ring: Arc<RingPlane>,
    k: usize,
    m: usize,
    runtime: Arc<ConsensusRuntime>,
    groups: Arc<Mutex<BTreeMap<String, LocalGroup>>>,
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
            runtime: ConsensusRuntime::start()?,
            groups: Arc::new(Mutex::new(BTreeMap::new())),
        });
        plane
            .ring
            .raft_registry()
            .register_target(plane.ring.safekeeper_id())
            .await;
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
        tokio::time::timeout(GROUP_SUBMIT_TIMEOUT, async {
            loop {
                if let Some(leader) = local.leader_id().await {
                    if leader == self.ring.safekeeper_id() {
                        match local.execute(request.clone()).await {
                            Ok(response) => return Ok(response),
                            Err(GroupError::Raft(_)) => {}
                            Err(error) => return Err(error.into()),
                        }
                    }
                    let encoded = serde_json::to_vec(&request)?;
                    if let Ok(response) = self.network(group)?.command(leader, encoded).await {
                        return serde_json::from_slice(&response).map_err(Into::into);
                    }
                }
                // A leader may die after this replica observes it but before the
                // command reaches it. Re-observe Raft instead of treating that
                // transport race as a client-visible command rejection.
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
        })
        .await?
    }

    /// Returns the locally opened authoritative group names in deterministic order.
    ///
    /// A lifecycle coordinator uses this inventory after closing admission; it
    /// never guesses groups from cache-ring membership or client traffic.
    pub async fn hosted_groups(&self) -> Vec<String> {
        self.groups.lock().await.keys().cloned().collect()
    }

    /// Shuts down every hosted Raft core before stopping its private scheduler.
    ///
    /// Call this during cache-node teardown.  The public runtime can await the
    /// handles across runtimes, while the OpenRaft core itself remains owned by
    /// the consensus scheduler until its shutdown completes.
    pub async fn shutdown(&self) -> Result<(), PlaneError> {
        let rafts = self
            .groups
            .lock()
            .await
            .values()
            .map(|local| local.raft.clone())
            .collect::<Vec<_>>();
        self.runtime
            .run(async move {
                for raft in rafts {
                    raft.shutdown().await?;
                }
                Ok(())
            })
            .await?;
        self.runtime.shutdown().await
    }

    /// Opens one persistent local replica idempotently without initializing it.
    async fn open_local(&self, group: &str) -> Result<Arc<ConsensusGroup>, PlaneError> {
        let root = self.root.clone();
        let ring = Arc::clone(&self.ring);
        let groups = Arc::clone(&self.groups);
        let k = self.k;
        let m = self.m;
        let group = group.to_owned();
        self.runtime
            .run(async move {
                Self::open_local_on_consensus_runtime(root, ring, k, m, groups, &group).await
            })
            .await
    }

    /// Opens one persistent local replica while the consensus scheduler is entered.
    async fn open_local_on_consensus_runtime(
        root: PathBuf,
        ring: Arc<RingPlane>,
        k: usize,
        m: usize,
        groups: Arc<Mutex<BTreeMap<String, LocalGroup>>>,
        group: &str,
    ) -> Result<Arc<ConsensusGroup>, PlaneError> {
        validate_group(group)?;
        let mut groups = groups.lock().await;
        if let Some(local) = groups.get(group) {
            return Ok(Arc::clone(&local.group));
        }
        let directory = root.join(hex::encode(Sha256::digest(group.as_bytes())));
        std::fs::create_dir_all(&directory)?;
        let log = PersistentLogStore::open(directory.join("raft-log.json")).await?;
        let state_machine = PersistentStateMachine::open(directory.join("state.json")).await?;
        let committed_voters = state_machine.committed_voters().await;
        let voters = if committed_voters.len() == k + m {
            committed_voters.into_iter().collect()
        } else {
            ring.consensus_voters()
        };
        let payloads = Arc::new(DistributedPayloadStore::new(
            k,
            m,
            voters.clone(),
            ring.consensus_payload_transport(),
        )?);
        state_machine.attach_payload_store(payloads.clone()).await?;
        let network =
            RaftHttpTransport::new(group, ring.raft_addresses(), ring.raft_secret().to_owned())?;
        let raft = Raft::new(
            ring.safekeeper_id(),
            Arc::new(raft_config(&voters, ring.safekeeper_id())?),
            network,
            log,
            state_machine.clone(),
        )
        .await?;
        ring.raft_registry()
            .register(group, ring.safekeeper_id(), raft.clone())
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
        let request: GroupRequest =
            serde_json::from_slice(body).map_err(|error| error.to_string())?;
        if local.leader_id().await != Some(self.ring.safekeeper_id()) {
            return Err("forwarded command reached a non-leader".to_owned());
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::{Vote, raft::VoteRequest};

    /// Staggered election windows ensure only the earliest surviving voter
    /// starts a replacement campaign after the leader is killed.
    #[test]
    fn raft_timing_uses_stable_sorted_voter_ranks() {
        let voters = [44, 11, 33, 22];
        let config = raft_config(&voters, 33).expect("valid Raft timing");

        assert_eq!(config.heartbeat_interval, 250);
        assert_eq!(config.election_timeout_min, 2_500);
        assert_eq!(config.election_timeout_max, 2_750);
    }

    /// Keeps the first legal successor's campaign inside the WAL request deadline.
    ///
    /// A follower must elect and then establish its current-term read fence
    /// before the five-second wire deadline expires. The earliest surviving
    /// rank therefore needs a material part of that deadline left for fencing
    /// and exact payload reconstruction.
    #[test]
    fn leader_loss_election_leaves_time_for_the_wal_read_fence() {
        let voters = [44, 11, 33, 22];
        for failed in voters {
            let earliest_survivor_timeout = voters
                .iter()
                .copied()
                .filter(|voter| *voter != failed)
                .map(|voter| {
                    raft_config(&voters, voter)
                        .expect("valid ranked voter")
                        .election_timeout_max
                })
                .min()
                .expect("a four-voter group retains three voters after one loss");
            assert!(
                earliest_survivor_timeout <= 2_500,
                "a one-voter loss must leave at least half of the fixed five-second ReadWal deadline for the current-term fence"
            );
        }
    }

    /// A vote remains serviceable while every public-runtime worker is blocked.
    ///
    /// This is intentionally a real OpenRaft vote, not a scheduler-only probe:
    /// [`Raft::new`] must run while the private scheduler is entered, otherwise
    /// its core task inherits the saturated public runtime and this deadline
    /// expires.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn real_vote_survives_public_runtime_saturation() {
        let directory = tempfile::tempdir().expect("temporary consensus state");
        let runtime = ConsensusRuntime::start().expect("start consensus runtime");
        let log = PersistentLogStore::open(directory.path().join("raft-log.json"))
            .await
            .expect("open durable log");
        let state = PersistentStateMachine::open(directory.path().join("state.json"))
            .await
            .expect("open durable state machine");
        let raft = runtime
            .run(async move {
                Raft::new(
                    1,
                    Arc::new(openraft::Config::default().validate()?),
                    RaftHttpTransport::new(
                        "runtime-isolation",
                        verglas_cluster::StaticRaftAddressResolver::default(),
                        "test-secret",
                    )?,
                    log,
                    state,
                )
                .await
                .map_err(Into::into)
            })
            .await
            .expect("create Raft on the consensus runtime");

        let (entered, entered_rx) = std::sync::mpsc::channel();
        for _ in 0..2 {
            let entered = entered.clone();
            tokio::spawn(async move {
                entered.send(()).expect("report public worker saturation");
                std::thread::sleep(Duration::from_millis(300));
            });
        }
        drop(entered);
        entered_rx
            .recv_timeout(Duration::from_millis(100))
            .expect("first public worker blocked");
        entered_rx
            .recv_timeout(Duration::from_millis(100))
            .expect("second public worker blocked");

        let (vote_tx, vote_rx) = std::sync::mpsc::sync_channel(1);
        let vote_raft = raft.clone();
        std::thread::spawn(move || {
            let vote_runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("start vote caller runtime");
            let result = vote_runtime.block_on(async move {
                vote_raft
                    .vote(VoteRequest::new(Vote::new(1, 2), None))
                    .await
            });
            let _ = vote_tx.send(result);
        });
        let response = vote_rx
            .recv_timeout(Duration::from_millis(150))
            .expect("private Raft core answers during public saturation")
            .expect("real Raft vote succeeds");
        assert!(response.vote_granted);

        let shutdown_raft = raft.clone();
        runtime
            .run(async move {
                shutdown_raft.shutdown().await?;
                Ok(())
            })
            .await
            .expect("stop private Raft core");
        runtime.shutdown().await.expect("stop consensus runtime");
    }
}
