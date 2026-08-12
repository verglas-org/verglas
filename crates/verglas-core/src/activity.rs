//! Foreground-operation accounting for a host-managed scale-to-zero fence.
//!
//! Callers take one guard after accepting work and hold it through the last
//! response byte or protocol acknowledgement. Durable background recovery and
//! propagation deliberately do not take guards: retained storage, not a
//! continuously running process, owns that state.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

/// One foreground protocol plane served by a cache node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActivityPlane {
    /// Authenticated S3 plus cache-owned local HTTP data requests.
    Http,
    /// Attached block-device commands.
    Nbd,
    /// Erasure-coded fragment RPC operations from ring peers.
    Fragment,
    /// Neon safekeeper client connections.
    Safekeeper,
}

/// Monotonic acceptance and current in-flight counts for one protocol plane.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlaneActivity {
    /// Operations accepted since this process started.
    pub accepted: u64,
    /// Operations whose response or connection has not finished.
    pub inflight: u64,
}

/// Per-plane diagnostics accompanying the aggregate stop fence.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActivityPlanes {
    /// S3 and local HTTP data activity.
    pub http: PlaneActivity,
    /// Block-device activity.
    pub nbd: PlaneActivity,
    /// Fragment-ring RPC activity.
    pub fragment: PlaneActivity,
    /// Neon safekeeper activity.
    pub safekeeper: PlaneActivity,
}

/// One point-in-time foreground activity report.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActivitySnapshot {
    /// Whether a foreground protocol may accept a new operation.
    pub accepting: bool,
    /// Lease generation of the current or most recent admission fence.
    pub generation: u64,
    /// True exactly when the aggregate in-flight counter is zero.
    pub idle: bool,
    /// Operations accepted across every plane since process start.
    pub accepted: u64,
    /// Operations currently holding the stop fence across every plane.
    pub inflight: u64,
    /// Wall-clock timestamp of the most recently accepted operation.
    pub last_activity_unix_ms: Option<u64>,
    /// Diagnostic counters split by protocol plane.
    pub planes: ActivityPlanes,
}

/// Shared foreground activity recorder.
#[derive(Clone)]
pub struct ActivityTracker(Arc<ActivityInner>);

/// Atomic counters behind [`ActivityTracker`].
struct ActivityInner {
    accepting: AtomicBool,
    generation: AtomicU64,
    fence_lock: Mutex<()>,
    accepted: AtomicU64,
    inflight: AtomicU64,
    last_activity_unix_ms: AtomicU64,
    http: PlaneCounters,
    nbd: PlaneCounters,
    fragment: PlaneCounters,
    safekeeper: PlaneCounters,
}

/// Atomic counters for one protocol plane.
#[derive(Default)]
struct PlaneCounters {
    accepted: AtomicU64,
    inflight: AtomicU64,
}

impl PlaneCounters {
    /// Records one accepted operation before its handler starts.
    fn begin(&self) {
        self.accepted.fetch_add(1, Ordering::Relaxed);
        self.inflight.fetch_add(1, Ordering::Release);
    }

    /// Releases one operation after its response or connection ends.
    fn end(&self) {
        self.inflight.fetch_sub(1, Ordering::Release);
    }

    /// Reads this plane's diagnostic counters.
    fn snapshot(&self) -> PlaneActivity {
        PlaneActivity {
            accepted: self.accepted.load(Ordering::Relaxed),
            inflight: self.inflight.load(Ordering::Acquire),
        }
    }
}

impl ActivityTracker {
    /// Creates an idle tracker with no accepted operations.
    pub fn new() -> Self {
        Self(Arc::new(ActivityInner::new()))
    }

    /// Attempts to admit foreground work under the current host fence.
    ///
    /// The aggregate counter is raised before the acceptance check. A fence
    /// racing this call therefore either observes the operation in flight or
    /// makes this call reject; it cannot report idle while hidden work starts.
    pub fn try_begin(&self, plane: ActivityPlane) -> Result<ActivityGuard, ActivityRejected> {
        self.0.inflight.fetch_add(1, Ordering::SeqCst);
        if !self.0.accepting.load(Ordering::SeqCst) {
            self.0.inflight.fetch_sub(1, Ordering::SeqCst);
            return Err(ActivityRejected);
        }
        let counters = self.0.plane(plane);
        counters.begin();
        self.0.accepted.fetch_add(1, Ordering::Relaxed);
        self.0
            .last_activity_unix_ms
            .store(unix_time_ms(), Ordering::Relaxed);
        Ok(ActivityGuard {
            tracker: Arc::clone(&self.0),
            plane,
        })
    }

    /// Closes foreground admission and returns its idempotent lease generation.
    pub fn fence(&self) -> u64 {
        let _guard = self
            .0
            .fence_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.0.accepting.load(Ordering::SeqCst) {
            let generation = self.0.generation.fetch_add(1, Ordering::SeqCst) + 1;
            self.0.accepting.store(false, Ordering::SeqCst);
            generation
        } else {
            self.0.generation.load(Ordering::SeqCst)
        }
    }

    /// Reopens admission only when `generation` owns the current fence.
    pub fn unfence(&self, generation: u64) -> bool {
        let _guard = self
            .0
            .fence_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.0.accepting.load(Ordering::SeqCst)
            || self.0.generation.load(Ordering::SeqCst) != generation
        {
            return false;
        }
        self.0.accepting.store(true, Ordering::SeqCst);
        true
    }

    /// Reads the aggregate stop fence and per-plane diagnostics.
    pub fn snapshot(&self) -> ActivitySnapshot {
        let inflight = self.0.inflight.load(Ordering::Acquire);
        let last_activity_unix_ms = self.0.last_activity_unix_ms.load(Ordering::Relaxed);
        ActivitySnapshot {
            accepting: self.0.accepting.load(Ordering::SeqCst),
            generation: self.0.generation.load(Ordering::SeqCst),
            idle: inflight == 0,
            accepted: self.0.accepted.load(Ordering::Relaxed),
            inflight,
            last_activity_unix_ms: (last_activity_unix_ms != 0).then_some(last_activity_unix_ms),
            planes: ActivityPlanes {
                http: self.0.http.snapshot(),
                nbd: self.0.nbd.snapshot(),
                fragment: self.0.fragment.snapshot(),
                safekeeper: self.0.safekeeper.snapshot(),
            },
        }
    }
}

impl ActivityInner {
    /// Creates an accepting tracker with no fence lease or foreground work.
    fn new() -> Self {
        Self {
            accepting: AtomicBool::new(true),
            generation: AtomicU64::new(0),
            fence_lock: Mutex::new(()),
            accepted: AtomicU64::new(0),
            inflight: AtomicU64::new(0),
            last_activity_unix_ms: AtomicU64::new(0),
            http: PlaneCounters::default(),
            nbd: PlaneCounters::default(),
            fragment: PlaneCounters::default(),
            safekeeper: PlaneCounters::default(),
        }
    }

    /// Selects the counter set for one protocol plane.
    fn plane(&self, plane: ActivityPlane) -> &PlaneCounters {
        match plane {
            ActivityPlane::Http => &self.http,
            ActivityPlane::Nbd => &self.nbd,
            ActivityPlane::Fragment => &self.fragment,
            ActivityPlane::Safekeeper => &self.safekeeper,
        }
    }
}

impl Default for ActivityTracker {
    /// Defaults to an accepting, idle tracker.
    fn default() -> Self {
        Self::new()
    }
}

/// Foreground admission was closed by the host's scale-to-zero fence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActivityRejected;

/// A foreground operation's contribution to the stop fence.
pub struct ActivityGuard {
    tracker: Arc<ActivityInner>,
    plane: ActivityPlane,
}

impl Drop for ActivityGuard {
    /// Releases the plane counter before the aggregate counter can report idle.
    fn drop(&mut self) {
        self.tracker.plane(self.plane).end();
        self.tracker.inflight.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Returns the current Unix timestamp in milliseconds for idle-age reporting.
fn unix_time_ms() -> u64 {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
}
