//! The guard policy: the runaway-worker controls applied around a guarded run.
//!
//! The mechanisms live in [`crate::guard`]; this module is the policy that
//! composes them in a fixed order, with fixed limits — no config knobs. It was
//! lifted here from the former MV harness so the one shared home outlives the
//! Source/Sink/MV crates that are gone. Every guarded run checks, in order:
//!
//! 1. **Child-marker suppression** — a Verglas-spawned child never runs.
//! 2. **Backoff** — a window in its backoff window or past the retry cap is
//!    skipped before any work.
//! 3. **Host-wide cap** — beyond the concurrency cap the window is marked
//!    pending (a finishing runner drains it) and this trigger exits.
//! 4. **Per-window single-flight** — a window already running is skipped.
//!
//! A run that passes all four executes the body; success clears the window's
//! retry state, failure records one more. The cap is the same fixed 2 the memory
//! pipeline uses.

use std::path::Path;

use crate::guard::{
    Attempt, RetryPolicy, SessionLock, SlotLock, clear_retry_state, is_child, load_retry_state,
    mark_pending, record_failure, should_attempt,
};

/// The host-wide concurrency cap: the fixed limit every guarded run shares.
/// Not a config knob — one shared budget, first come first served.
pub const MAX_CONCURRENT: usize = 2;

/// Why a guarded run did not execute its body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Skipped {
    /// This process is a Verglas-spawned child (control 1).
    Child,
    /// The window is inside its backoff window (control 2).
    Backoff,
    /// The window is past its retry cap (control 2).
    RetryCap,
    /// The host-wide cap is reached; the window was marked pending (control 3).
    Capped,
    /// The window is already running (control 4).
    AlreadyRunning,
    /// A guard file operation failed.
    GuardError(String),
}

/// The result of a guarded run.
#[derive(Debug)]
pub enum Guarded<T> {
    /// The body ran and produced `T`.
    Ran(T),
    /// The body did not run; here is why.
    Skipped(Skipped),
}

/// Runs `body` under the guard policy for `window`, keyed to `guard_dir`.
///
/// The four controls are checked in the documented order; only a run that passes
/// them all executes `body`. On success the window's retry state is cleared; on
/// an `Err` result one failure is recorded (feeding the backoff). The `Ok`/`Err`
/// of `body` is preserved inside [`Guarded::Ran`].
pub async fn run_guarded<T, E, Fut, F>(
    guard_dir: &Path,
    window: &str,
    body: F,
) -> Guarded<Result<T, E>>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
{
    // Control 1: a child never runs.
    if is_child() {
        return Guarded::Skipped(Skipped::Child);
    }

    // Control 2: backoff / retry cap.
    let policy = RetryPolicy::default();
    let state = load_retry_state(guard_dir, window).ok().flatten();
    match should_attempt(state.as_ref(), chrono::Utc::now(), &policy) {
        Attempt::Go => {}
        Attempt::Backoff => return Guarded::Skipped(Skipped::Backoff),
        Attempt::GiveUp => return Guarded::Skipped(Skipped::RetryCap),
    }

    // Control 3: host-wide cap. Beyond it the window is pending, not queued.
    let slot = match SlotLock::acquire(guard_dir, MAX_CONCURRENT) {
        Ok(Some(slot)) => slot,
        Ok(None) => {
            let _ = mark_pending(guard_dir, window);
            return Guarded::Skipped(Skipped::Capped);
        }
        Err(e) => return Guarded::Skipped(Skipped::GuardError(e.to_string())),
    };

    // Control 4: single-flight per window.
    let lock = match SessionLock::acquire(guard_dir, window) {
        Ok(Some(lock)) => lock,
        Ok(None) => {
            drop(slot);
            return Guarded::Skipped(Skipped::AlreadyRunning);
        }
        Err(e) => {
            drop(slot);
            return Guarded::Skipped(Skipped::GuardError(e.to_string()));
        }
    };

    let result = body().await;
    match &result {
        Ok(_) => {
            let _ = clear_retry_state(guard_dir, window);
        }
        Err(_) => {
            let _ = record_failure(guard_dir, window);
        }
    }
    drop(lock);
    drop(slot);
    Guarded::Ran(result)
}
