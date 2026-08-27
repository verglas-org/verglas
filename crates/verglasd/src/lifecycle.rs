//! Lifecycle fencing for one resident Durable Object process.
//!
//! A process may stop only after its caller confirms the durable storage
//! checkpoint, drained outbox, and clean event-endpoint shutdown. Placement and
//! lease validation remain outside this process.

/// Confirmations required before a Durable Object process may stop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SuspendFence {
    pushed: bool,
    outbox_drained: bool,
    event_shutdown_clean: bool,
}

impl SuspendFence {
    /// Creates a fence from storage and event shutdown confirmations.
    pub const fn new(pushed: bool, outbox_drained: bool, event_shutdown_clean: bool) -> Self {
        Self {
            pushed,
            outbox_drained,
            event_shutdown_clean,
        }
    }

    /// Returns whether durable storage reached its checkpoint boundary.
    pub const fn pushed(self) -> bool {
        self.pushed
    }

    /// Returns whether all committed outbox rows were drained.
    pub const fn outbox_drained(self) -> bool {
        self.outbox_drained
    }

    /// Returns whether the event endpoint shut down without a dirty termination.
    pub const fn event_shutdown_clean(self) -> bool {
        self.event_shutdown_clean
    }

    /// Returns whether every required stop confirmation is present.
    pub const fn is_complete(self) -> bool {
        self.pushed && self.outbox_drained && self.event_shutdown_clean
    }
}

/// Lifecycle state of one supervised `verglas-runtime` process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChildState {
    /// Process is online and accepts serialized events.
    Running,
    /// Process is stopped after the storage/event shutdown fence.
    Suspended,
    /// Process is being restarted and is not routable.
    Restoring,
}

/// A lifecycle transition that would expose stale or incomplete state.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum LifecycleError {
    /// The required storage checkpoint, outbox drain, or clean shutdown confirmation is absent.
    #[error(
        "durable storage checkpoint, drained outbox, and clean event shutdown are all required before suspend"
    )]
    SuspendUnconfirmed,
    /// The requested transition is illegal from the current state.
    #[error("invalid child lifecycle transition")]
    InvalidTransition,
}

/// Supervisor-owned lifecycle state for one child process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChildLifecycle {
    state: ChildState,
}

impl ChildLifecycle {
    /// Creates a running child lifecycle.
    pub const fn running() -> Self {
        Self {
            state: ChildState::Running,
        }
    }

    /// Returns the current process lifecycle state.
    pub const fn state(self) -> ChildState {
        self.state
    }

    /// Stops a child only after storage and event shutdown confirmations exist.
    pub fn suspend(&mut self, fence: SuspendFence) -> Result<(), LifecycleError> {
        if self.state != ChildState::Running {
            return Err(LifecycleError::InvalidTransition);
        }
        if !fence.is_complete() {
            return Err(LifecycleError::SuspendUnconfirmed);
        }
        self.state = ChildState::Suspended;
        Ok(())
    }

    /// Starts a suspended child without making it routable before readiness.
    pub fn begin_restore(&mut self) -> Result<(), LifecycleError> {
        if self.state != ChildState::Suspended {
            return Err(LifecycleError::InvalidTransition);
        }
        self.state = ChildState::Restoring;
        Ok(())
    }

    /// Publishes a restored child after its event socket is ready.
    pub fn finish_restore(&mut self) -> Result<(), LifecycleError> {
        if self.state != ChildState::Restoring {
            return Err(LifecycleError::InvalidTransition);
        }
        self.state = ChildState::Running;
        Ok(())
    }

    /// Marks a crashed child unroutable until a caller starts restoration.
    pub fn begin_crash_recovery(&mut self) -> Result<(), LifecycleError> {
        if self.state != ChildState::Running {
            return Err(LifecycleError::InvalidTransition);
        }
        self.state = ChildState::Restoring;
        Ok(())
    }

    /// Returns whether this child may receive a serialized event.
    pub fn may_execute_event(self) -> bool {
        self.state == ChildState::Running
    }
}
