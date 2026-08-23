//! Safety state machine for independently suspending and restoring one DO child.

use crate::ReplicaRole;

/// Durable watermarks required before all replicas of a DO may stop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SuspendFence {
    applied: u64,
    archived: u64,
    checkpointed: u64,
}

impl SuspendFence {
    /// Creates the applied, transaction-archive, and checkpoint watermarks.
    pub fn new(applied: u64, archived: u64, checkpointed: u64) -> Self {
        Self {
            applied,
            archived,
            checkpointed,
        }
    }
}

/// Lifecycle state of one supervised `verglasd` child.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChildState {
    /// Replica process is online in its current Raft role.
    Running(ReplicaRole),
    /// Process is admitted to no new work while its durability fence is assembled.
    Suspending(ReplicaRole),
    /// Process is stopped after a safe checkpoint and archive.
    Suspended,
    /// Process is restoring concurrently with its peer replicas.
    Restoring {
        /// Applied fence required before routing can resume.
        required: u64,
    },
}

/// A lifecycle transition that would lose or expose stale state.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum LifecycleError {
    /// The transaction archive has not covered all applied commands.
    #[error("applied sequence {applied} exceeds archive sequence {archived}")]
    Unarchived {
        /// Highest applied command.
        applied: u64,
        /// Highest verified S3 transaction archive.
        archived: u64,
    },
    /// The object checkpoint has not covered all applied commands.
    #[error("applied sequence {applied} exceeds checkpoint sequence {checkpointed}")]
    Uncheckpointed {
        /// Highest applied command.
        applied: u64,
        /// Highest committed object checkpoint.
        checkpointed: u64,
    },
    /// Restored SQLite and WAL state is behind the request fence.
    #[error("restored sequence {restored} is behind required sequence {required}")]
    RestoreBehind {
        /// Fence held by ingress for the triggering request.
        required: u64,
        /// Sequence recovered and applied by this child.
        restored: u64,
    },
    /// The requested transition is illegal from the current state.
    #[error("invalid child lifecycle transition")]
    InvalidTransition,
}

/// Supervisor-owned lifecycle and applied fence for one child process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChildLifecycle {
    state: ChildState,
    applied: u64,
}

impl ChildLifecycle {
    /// Creates a running leader or follower at one applied sequence.
    pub fn running(role: ReplicaRole, applied: u64) -> Self {
        Self {
            state: ChildState::Running(role),
            applied,
        }
    }

    /// Returns the current process lifecycle state.
    pub fn state(self) -> ChildState {
        self.state
    }

    /// Fences admissions before the supervisor starts the asynchronous drain protocol.
    pub fn begin_suspend(&mut self) -> Result<(), LifecycleError> {
        let ChildState::Running(role) = self.state else {
            return Err(LifecycleError::InvalidTransition);
        };
        self.state = ChildState::Suspending(role);
        Ok(())
    }

    /// Rolls back an admission fence when orchestration cannot complete safely.
    pub fn rollback_suspend(&mut self) -> Result<(), LifecycleError> {
        let ChildState::Suspending(role) = self.state else {
            return Err(LifecycleError::InvalidTransition);
        };
        self.state = ChildState::Running(role);
        Ok(())
    }

    /// Completes a suspended transition after archive and checkpoint coverage is verified.
    pub fn finish_suspend(&mut self, fence: SuspendFence) -> Result<(), LifecycleError> {
        if !matches!(self.state, ChildState::Suspending(_)) {
            return Err(LifecycleError::InvalidTransition);
        }
        let applied = self.applied.max(fence.applied);
        if fence.archived < applied {
            return Err(LifecycleError::Unarchived {
                applied,
                archived: fence.archived,
            });
        }
        if fence.checkpointed < applied {
            return Err(LifecycleError::Uncheckpointed {
                applied,
                checkpointed: fence.checkpointed,
            });
        }
        self.applied = applied;
        self.state = ChildState::Suspended;
        Ok(())
    }

    /// Stops a running durable child only after archive and checkpoint cover it.
    pub fn suspend(&mut self, fence: SuspendFence) -> Result<(), LifecycleError> {
        if !matches!(self.state, ChildState::Running(_)) {
            return Err(LifecycleError::InvalidTransition);
        }
        let applied = self.applied.max(fence.applied);
        if fence.archived < applied {
            return Err(LifecycleError::Unarchived {
                applied,
                archived: fence.archived,
            });
        }
        if fence.checkpointed < applied {
            return Err(LifecycleError::Uncheckpointed {
                applied,
                checkpointed: fence.checkpointed,
            });
        }
        self.applied = applied;
        self.state = ChildState::Suspended;
        Ok(())
    }

    /// Moves a crashed running process behind a restore fence at its applied state.
    pub fn begin_crash_recovery(&mut self) -> Result<(), LifecycleError> {
        if !matches!(self.state, ChildState::Running(_)) {
            return Err(LifecycleError::InvalidTransition);
        }
        self.state = ChildState::Restoring {
            required: self.applied,
        };
        Ok(())
    }

    /// Starts concurrent restore and records the triggering request fence.
    pub fn begin_restore(&mut self, required: u64) -> Result<(), LifecycleError> {
        if self.state != ChildState::Suspended {
            return Err(LifecycleError::InvalidTransition);
        }
        self.state = ChildState::Restoring { required };
        Ok(())
    }

    /// Makes a restored replica routable after it reaches the required fence.
    pub fn finish_restore(
        &mut self,
        role: ReplicaRole,
        restored: u64,
    ) -> Result<(), LifecycleError> {
        let ChildState::Restoring { required } = self.state else {
            return Err(LifecycleError::InvalidTransition);
        };
        if restored < required {
            return Err(LifecycleError::RestoreBehind { required, restored });
        }
        self.applied = restored;
        self.state = ChildState::Running(role);
        Ok(())
    }

    /// Returns whether this child may run a stateful Worker event.
    pub fn may_execute_stateful_event(self) -> bool {
        self.state == ChildState::Running(ReplicaRole::Leader)
    }

    /// Returns whether this child can serve a snapshot at the requested fence.
    pub fn may_serve_snapshot(self, requested: u64) -> bool {
        matches!(self.state, ChildState::Running(_)) && self.applied >= requested
    }
}
