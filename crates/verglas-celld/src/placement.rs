//! Resource-aware placement of one DO replica on each tenant cell host.

/// Stable identity of one tenant-scoped cell host.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct HostId(String);

impl HostId {
    /// Creates a host identity from its scheduler name.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the scheduler-visible host name.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Hard resources installed on one cell host.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HostCapacity {
    cpu_millis: u32,
    memory_mib: u64,
}

impl HostCapacity {
    /// Creates hard CPU and memory ceilings for one host.
    pub fn new(cpu_millis: u32, memory_mib: u64) -> Self {
        Self {
            cpu_millis,
            memory_mib,
        }
    }
}

/// Current resource and leadership pressure on one host.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostLoad {
    id: HostId,
    capacity: HostCapacity,
    cpu_used_millis: u32,
    memory_used_mib: u64,
    leader_count: u32,
    recent_transaction_load: u64,
}

impl HostLoad {
    /// Creates one placement-agent load sample.
    pub fn new(
        id: HostId,
        capacity: HostCapacity,
        cpu_used_millis: u32,
        memory_used_mib: u64,
        leader_count: u32,
        recent_transaction_load: u64,
    ) -> Self {
        Self {
            id,
            capacity,
            cpu_used_millis,
            memory_used_mib,
            leader_count,
            recent_transaction_load,
        }
    }

    /// Returns CPU headroom without allowing underflow from a bad sample.
    fn available_cpu(&self) -> u32 {
        self.capacity
            .cpu_millis
            .saturating_sub(self.cpu_used_millis)
    }

    /// Returns memory headroom without allowing underflow from a bad sample.
    fn available_memory(&self) -> u64 {
        self.capacity
            .memory_mib
            .saturating_sub(self.memory_used_mib)
    }

    /// Returns whether this host can reserve a follower replica.
    fn follower_eligible(&self, load: &DoLoad) -> bool {
        self.available_cpu() >= load.follower_cpu_millis()
            && self.available_memory() >= load.memory_mib
    }

    /// Returns whether this host can reserve leadership and recent transaction work.
    fn leader_eligible(&self, load: &DoLoad) -> bool {
        self.available_cpu() >= load.leader_cpu_millis()
            && self.available_memory() >= load.memory_mib
    }

    /// Computes a deterministic lower-is-better leadership pressure score.
    fn leader_score(&self, load: &DoLoad) -> u128 {
        let cpu_pressure = if self.capacity.cpu_millis == 0 {
            u128::MAX / 4
        } else {
            u128::from(self.cpu_used_millis) * 10_000 / u128::from(self.capacity.cpu_millis)
        };
        let memory_pressure = if self.capacity.memory_mib == 0 {
            u128::MAX / 4
        } else {
            u128::from(self.memory_used_mib) * 10_000 / u128::from(self.capacity.memory_mib)
        };
        cpu_pressure
            .saturating_add(memory_pressure)
            .saturating_add(u128::from(self.leader_count) * 2_000)
            .saturating_add(u128::from(self.recent_transaction_load))
            .saturating_add(u128::from(load.recent_transaction_load))
    }
}

/// Resource reservation and recent transaction pressure for one DO.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DoLoad {
    cpu_millis: u32,
    memory_mib: u64,
    recent_transaction_load: u64,
}

impl DoLoad {
    /// Creates one DO placement sample.
    pub fn new(cpu_millis: u32, memory_mib: u64, recent_transaction_load: u64) -> Self {
        Self {
            cpu_millis,
            memory_mib,
            recent_transaction_load,
        }
    }

    /// Returns the lower idle CPU reservation for a promotable follower.
    fn follower_cpu_millis(self) -> u32 {
        self.cpu_millis.div_ceil(2).max(1)
    }

    /// Returns CPU required to lead the current transaction workload.
    fn leader_cpu_millis(self) -> u32 {
        let recent = u32::try_from(self.recent_transaction_load).unwrap_or(u32::MAX);
        self.cpu_millis.saturating_add(recent)
    }
}

/// Role assigned to one child process in a DO group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaRole {
    /// Accepts Worker transactions and stateful events.
    Leader,
    /// Applies committed entries and remains ready for promotion.
    Follower,
}

/// One child-process assignment on one host.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaPlacement {
    host: HostId,
    role: ReplicaRole,
}

impl ReplicaPlacement {
    /// Returns the cell host that runs this replica.
    pub fn host(&self) -> &HostId {
        &self.host
    }

    /// Returns whether this replica leads or follows.
    pub fn role(&self) -> ReplicaRole {
        self.role
    }
}

/// Complete three-replica placement of one active DO.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupPlacement {
    replicas: Vec<ReplicaPlacement>,
    leader_index: usize,
}

impl GroupPlacement {
    /// Returns all three distinct host assignments.
    pub fn replicas(&self) -> &[ReplicaPlacement] {
        &self.replicas
    }

    /// Returns the exactly one leader assignment.
    pub fn leader(&self) -> &ReplicaPlacement {
        &self.replicas[self.leader_index]
    }
}

/// A placement request that cannot preserve the three-replica contract.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum PlacementError {
    /// Fewer than three hosts can reserve even a follower replica.
    #[error("only {eligible} hosts can reserve a DO replica; three are required")]
    InsufficientEligibleHosts {
        /// Number of hosts below their hard ceilings.
        eligible: usize,
    },
    /// Three followers fit but no host can reserve leadership work.
    #[error("no replica host has enough headroom to lead the DO")]
    NoEligibleLeader,
}

/// Deterministic placement planner used by the tenant cell supervisor.
pub struct PlacementPlanner;

impl PlacementPlanner {
    /// Places one replica on each of three eligible hosts and balances leadership.
    pub fn place(
        hosts: &[HostLoad],
        load: &DoLoad,
        active_runtime_host: Option<&HostId>,
    ) -> Result<GroupPlacement, PlacementError> {
        let mut eligible = hosts
            .iter()
            .filter(|host| host.follower_eligible(load))
            .collect::<Vec<_>>();
        if eligible.len() < 3 {
            return Err(PlacementError::InsufficientEligibleHosts {
                eligible: eligible.len(),
            });
        }
        eligible.sort_by(|left, right| left.id.cmp(&right.id));
        eligible.truncate(3);

        let preferred = active_runtime_host.and_then(|preferred| {
            eligible
                .iter()
                .copied()
                .find(|host| &host.id == preferred && host.leader_eligible(load))
        });
        let leader = preferred.or_else(|| {
            eligible
                .iter()
                .copied()
                .filter(|host| host.leader_eligible(load))
                .min_by(|left, right| {
                    left.leader_score(load)
                        .cmp(&right.leader_score(load))
                        .then_with(|| left.id.cmp(&right.id))
                })
        });
        let leader = leader.ok_or(PlacementError::NoEligibleLeader)?;
        let leader_index = eligible
            .iter()
            .position(|host| host.id == leader.id)
            .ok_or(PlacementError::NoEligibleLeader)?;
        let replicas = eligible
            .into_iter()
            .map(|host| ReplicaPlacement {
                host: host.id.clone(),
                role: if host.id == leader.id {
                    ReplicaRole::Leader
                } else {
                    ReplicaRole::Follower
                },
            })
            .collect();
        Ok(GroupPlacement {
            replicas,
            leader_index,
        })
    }
}
