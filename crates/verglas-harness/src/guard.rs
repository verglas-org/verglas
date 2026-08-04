//! Runaway-worker guard policy for the harnesses (WHITEPAPER §7.2; #325).
//!
//! Lifted verbatim out of `verglas-memory-jobs` (the #297 memory-consolidation
//! controls) into shared harness policy, so the MV and sink harnesses apply the
//! same limits — no new config knobs, the fixed limits the memory pipeline
//! already runs with. The pieces:
//!
//! - [`CHILD_MARKER_ENV`] / [`is_child`] — child-marker suppression: a
//!   Verglas-spawned child process never drives a harness, so a Job that spawns
//!   a subagent can never recurse into itself.
//! - [`SessionLock`] — per-window single-flight. A second trigger for a window
//!   already running gets no lock and exits; a stale lock left by a dead process
//!   is taken over.
//! - [`SlotLock`] / [`live_lock_count`] / [`mark_pending`] / [`take_pending`] —
//!   the host-wide concurrency cap. Beyond the cap a trigger marks the window
//!   pending and exits; a finishing runner drains pending windows in-process.
//!   Nothing ever queues as a waiting OS process.
//! - [`RetryState`] / [`should_attempt`] — per-window exponential backoff with a
//!   hard failure cap. A dead origin produces a handful of bounded attempts,
//!   then silence, never a storm.
//!
//! All state is plain files under one guard directory, so the controls hold
//! across processes without a daemon.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// The environment variable a harness-spawned child process carries, so its own
/// hooks and triggers stay inert (child-marker suppression). A process with this
/// set must never drive a harness.
pub const CHILD_MARKER_ENV: &str = "VERGLAS_CONSOLIDATION_CHILD";

/// Whether this process is a harness-spawned child — read the marker from the
/// environment. The first control every guarded run checks.
pub fn is_child() -> bool {
    std::env::var_os(CHILD_MARKER_ENV).is_some()
}

/// Sanitises a window/session id into the filename form the guard files use.
fn safe_name(session_id: &str) -> String {
    session_id
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// Whether `pid` names a live process (signal 0 probe).
#[cfg(unix)]
fn pid_alive(pid: i32) -> bool {
    if pid <= 0 {
        return false;
    }
    // Safety: kill with signal 0 performs only a liveness/permission check.
    unsafe { libc::kill(pid, 0) == 0 }
}

/// Whether `pid` names a live process, on platforms without `libc::kill`
/// (Windows). No cheap portable probe exists here, so this errs conservative
/// and reports the holder as alive: a lock is never stolen from a pid that may
/// still be running. The trade-off is that a crashed holder's lock is not
/// reclaimed automatically on these platforms — acceptable until a native
/// backend lands.
#[cfg(not(unix))]
fn pid_alive(pid: i32) -> bool {
    pid > 0
}

/// A held per-window lock (single-flight). Dropping it releases the lock file.
pub struct SessionLock {
    path: PathBuf,
}

impl SessionLock {
    /// Tries to take the lock for `session_id` under `guard_dir`.
    ///
    /// Returns `Ok(None)` when another live process holds it. A lock whose pid
    /// is dead is stale — a crashed worker must not wedge its window forever —
    /// and is taken over.
    pub fn acquire(guard_dir: &Path, session_id: &str) -> std::io::Result<Option<SessionLock>> {
        std::fs::create_dir_all(guard_dir)?;
        let path = guard_dir.join(format!("{}.lock", safe_name(session_id)));
        loop {
            match std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)
            {
                Ok(mut f) => {
                    write!(f, "{}", std::process::id())?;
                    return Ok(Some(SessionLock { path }));
                }
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                    let holder: i32 = std::fs::read_to_string(&path)
                        .ok()
                        .and_then(|s| s.trim().parse().ok())
                        .unwrap_or(0);
                    if pid_alive(holder) {
                        return Ok(None);
                    }
                    // Stale: remove and retry the create-new race.
                    let _ = std::fs::remove_file(&path);
                }
                Err(e) => return Err(e),
            }
        }
    }
}

impl Drop for SessionLock {
    /// Releases the lock file. A crash skips this; the stale-pid takeover above
    /// is the recovery path.
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// A held host-wide concurrency slot. At most `max` slot files can exist, and
/// acquisition is `O_EXCL`-atomic per slot, so simultaneous triggers cannot all
/// pass. Dropping the slot releases it.
pub struct SlotLock {
    path: PathBuf,
}

impl SlotLock {
    /// Tries each slot `0..max` in turn; returns `Ok(None)` when every slot is
    /// held by a live process. A slot whose pid is dead is taken over.
    pub fn acquire(guard_dir: &Path, max: usize) -> std::io::Result<Option<SlotLock>> {
        std::fs::create_dir_all(guard_dir)?;
        for slot in 0..max {
            let path = guard_dir.join(format!("slot-{slot}.slot"));
            loop {
                match std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&path)
                {
                    Ok(mut f) => {
                        write!(f, "{}", std::process::id())?;
                        return Ok(Some(SlotLock { path }));
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                        let holder: i32 = std::fs::read_to_string(&path)
                            .ok()
                            .and_then(|s| s.trim().parse().ok())
                            .unwrap_or(0);
                        if pid_alive(holder) {
                            break; // this slot is genuinely busy; try the next
                        }
                        let _ = std::fs::remove_file(&path);
                    }
                    Err(e) => return Err(e),
                }
            }
        }
        Ok(None)
    }
}

impl Drop for SlotLock {
    /// Releases the slot file. A crash skips this; stale-pid takeover recovers.
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Counts locks held by live processes under `guard_dir`. Stale locks are
/// cleaned as they are found.
pub fn live_lock_count(guard_dir: &Path) -> std::io::Result<usize> {
    let mut live = 0;
    let entries = match std::fs::read_dir(guard_dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e),
    };
    for entry in entries {
        let path = entry?.path();
        if path.extension().and_then(|e| e.to_str()) != Some("lock") {
            continue;
        }
        let holder: i32 = std::fs::read_to_string(&path)
            .ok()
            .and_then(|s| s.trim().parse().ok())
            .unwrap_or(0);
        if pid_alive(holder) {
            live += 1;
        } else {
            let _ = std::fs::remove_file(&path);
        }
    }
    Ok(live)
}

/// Marks `session_id` pending: the cap was reached, so the trigger exits and a
/// finishing runner picks the window up later. Idempotent.
pub fn mark_pending(guard_dir: &Path, session_id: &str) -> std::io::Result<()> {
    std::fs::create_dir_all(guard_dir)?;
    let path = guard_dir.join(format!("{}.pend", safe_name(session_id)));
    std::fs::write(path, session_id)?;
    Ok(())
}

/// Takes one pending window, removing its marker. Returns `None` when none
/// remain. The marker body carries the original (unsanitised) id.
pub fn take_pending(guard_dir: &Path) -> std::io::Result<Option<String>> {
    let entries = match std::fs::read_dir(guard_dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    for entry in entries {
        let path = entry?.path();
        if path.extension().and_then(|e| e.to_str()) != Some("pend") {
            continue;
        }
        let session = std::fs::read_to_string(&path)?;
        std::fs::remove_file(&path)?;
        if !session.trim().is_empty() {
            return Ok(Some(session.trim().to_owned()));
        }
    }
    Ok(None)
}

/// Per-window failure record, persisted as JSON in the guard dir.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetryState {
    /// Consecutive failures so far.
    pub failures: u32,
    /// When the last attempt was made.
    pub last_attempt: DateTime<Utc>,
}

/// The backoff policy: `base * 2^(failures-1)` between attempts, and a hard stop
/// at `max_failures`.
#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    /// The first backoff window.
    pub base: Duration,
    /// Consecutive failures after which the window is given up on.
    pub max_failures: u32,
}

impl Default for RetryPolicy {
    /// One minute doubling, five attempts. A dead origin costs five bounded
    /// tries spread over ~half an hour, then silence.
    fn default() -> Self {
        RetryPolicy {
            base: Duration::from_secs(60),
            max_failures: 5,
        }
    }
}

/// The decision for a trigger.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Attempt {
    /// Run now.
    Go,
    /// Inside the backoff window: exit immediately.
    Backoff,
    /// The failure cap is reached: never retry automatically.
    GiveUp,
}

/// Decides whether a trigger may run, from the persisted state.
pub fn should_attempt(
    state: Option<&RetryState>,
    now: DateTime<Utc>,
    policy: &RetryPolicy,
) -> Attempt {
    let Some(state) = state else {
        return Attempt::Go;
    };
    if state.failures >= policy.max_failures {
        return Attempt::GiveUp;
    }
    if state.failures == 0 {
        return Attempt::Go;
    }
    let exponent = state.failures.saturating_sub(1).min(16);
    let window = policy.base.saturating_mul(1u32 << exponent);
    let window = chrono::Duration::from_std(window).unwrap_or(chrono::Duration::MAX);
    if now - state.last_attempt >= window {
        Attempt::Go
    } else {
        Attempt::Backoff
    }
}

/// The retry-state path for a window.
fn retry_path(guard_dir: &Path, session_id: &str) -> PathBuf {
    guard_dir.join(format!("{}.retry", safe_name(session_id)))
}

/// Loads the persisted retry state, if any.
pub fn load_retry_state(guard_dir: &Path, session_id: &str) -> std::io::Result<Option<RetryState>> {
    match std::fs::read_to_string(retry_path(guard_dir, session_id)) {
        Ok(s) => Ok(serde_json::from_str(&s).ok()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

/// Records one more failure for the window, stamping now.
pub fn record_failure(guard_dir: &Path, session_id: &str) -> std::io::Result<()> {
    std::fs::create_dir_all(guard_dir)?;
    let next = RetryState {
        failures: load_retry_state(guard_dir, session_id)?
            .map(|s| s.failures)
            .unwrap_or(0)
            + 1,
        last_attempt: Utc::now(),
    };
    std::fs::write(
        retry_path(guard_dir, session_id),
        serde_json::to_string(&next)?,
    )?;
    Ok(())
}

/// Clears the retry state after a success.
pub fn clear_retry_state(guard_dir: &Path, session_id: &str) -> std::io::Result<()> {
    match std::fs::remove_file(retry_path(guard_dir, session_id)) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    /// Single-flight: a second acquire for a held window is refused; releasing
    /// frees it; a stale (dead-pid) lock is taken over.
    #[test]
    fn session_lock_single_flight_and_stale_takeover() {
        let dir = TempDir::new().expect("tempdir");
        let first = SessionLock::acquire(dir.path(), "w-a").expect("io");
        assert!(first.is_some(), "first trigger takes the lock");
        assert!(
            SessionLock::acquire(dir.path(), "w-a")
                .expect("io")
                .is_none(),
            "second is refused while held"
        );
        drop(first);
        assert!(
            SessionLock::acquire(dir.path(), "w-a")
                .expect("io")
                .is_some(),
            "released lock reacquirable"
        );
        std::fs::write(dir.path().join("w-b.lock"), "999999999").expect("write");
        assert!(
            SessionLock::acquire(dir.path(), "w-b")
                .expect("io")
                .is_some(),
            "stale lock taken over"
        );
    }

    /// Host-wide cap: at most `max` live slots; beyond it a trigger marks the
    /// window pending, and pending drains once.
    #[test]
    fn slot_cap_and_pending_drain() {
        let dir = TempDir::new().expect("tempdir");
        let s1 = SlotLock::acquire(dir.path(), 2)
            .expect("io")
            .expect("slot 0");
        let s2 = SlotLock::acquire(dir.path(), 2)
            .expect("io")
            .expect("slot 1");
        assert!(
            SlotLock::acquire(dir.path(), 2).expect("io").is_none(),
            "third is capped"
        );
        mark_pending(dir.path(), "w-c").expect("mark");
        assert_eq!(
            take_pending(dir.path()).expect("take"),
            Some("w-c".to_owned())
        );
        assert_eq!(
            take_pending(dir.path()).expect("take"),
            None,
            "drained once"
        );
        drop(s1);
        assert!(
            SlotLock::acquire(dir.path(), 2).expect("io").is_some(),
            "a freed slot is reusable"
        );
        drop(s2);
    }

    /// Backoff: no state runs; a failure inside the window backs off; past the
    /// window runs again; past the cap gives up.
    #[test]
    fn backoff_and_retry_cap() {
        let policy = RetryPolicy {
            base: Duration::from_secs(60),
            max_failures: 3,
        };
        let now = Utc::now();
        assert_eq!(should_attempt(None, now, &policy), Attempt::Go);
        let recent = RetryState {
            failures: 1,
            last_attempt: now,
        };
        assert_eq!(
            should_attempt(Some(&recent), now, &policy),
            Attempt::Backoff
        );
        let old = RetryState {
            failures: 1,
            last_attempt: now - chrono::Duration::minutes(5),
        };
        assert_eq!(should_attempt(Some(&old), now, &policy), Attempt::Go);
        let capped = RetryState {
            failures: 3,
            last_attempt: now - chrono::Duration::hours(1),
        };
        assert_eq!(should_attempt(Some(&capped), now, &policy), Attempt::GiveUp);
    }

    /// Failure records accumulate and clear.
    #[test]
    fn record_and_clear_failures() {
        let dir = TempDir::new().expect("tempdir");
        record_failure(dir.path(), "w-d").expect("record");
        record_failure(dir.path(), "w-d").expect("record");
        assert_eq!(
            load_retry_state(dir.path(), "w-d")
                .expect("load")
                .expect("state")
                .failures,
            2
        );
        clear_retry_state(dir.path(), "w-d").expect("clear");
        assert!(load_retry_state(dir.path(), "w-d").expect("load").is_none());
    }

    /// The child marker is read from the environment.
    #[test]
    fn child_marker_reads_env() {
        // The var is not set in the normal test process.
        assert_eq!(is_child(), std::env::var_os(CHILD_MARKER_ENV).is_some());
    }
}
