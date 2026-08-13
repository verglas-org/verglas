//! Error classification, the throttle hint, and the backoff schedule for the
//! backend retry layer (issue #20).
//!
//! ## What `object_store` already does vs. what this layer adds
//!
//! `object_store`'s built-in [`RetryConfig`](object_store::RetryConfig) retries
//! transient HTTP failures *inside a single trait call*, with jittered
//! exponential backoff and its own budget. We deliberately do **not** rely on
//! it for the fill path, for two reasons the library cannot address:
//!
//! * **Permit starvation.** That retry runs while our per-bucket concurrency
//!   permit is held (the trait call has not returned), so under a throttling
//!   origin every permit ends up parked in a backoff sleep and fresh fills
//!   starve. Our retry sits *above* the limiter, so each attempt re-acquires a
//!   permit and the backoff sleep holds none.
//! * **Breaker blindness.** The library's retry is opaque; it cannot feed
//!   per-request outcomes to the per-bucket circuit breaker.
//!
//! So the production S3 client is built with library retries **disabled**, and
//! this layer owns retry end-to-end. What we keep from `object_store` is its
//! *classification*: it maps 404→`NotFound`, 403→`PermissionDenied`, etc., and
//! buckets every transient condition (5xx, 429, 503 `SlowDown` including the
//! 200-body S3 quirk, timeouts, dropped connections) into `Generic`. Our
//! [`classify`] reads exactly that: `Generic` is retryable, the named
//! client-error variants are not.

use std::time::Duration;

/// A hint that a transient failure was an origin throttle, optionally carrying
/// the server's requested wait. Attach it as the `source` of an
/// `object_store::Error::Generic` and the retry layer will honour the
/// `retry_after` instead of its own backoff.
///
/// This is the extension point a future HTTP-layer throttle detector (that can read a
/// `Retry-After` header) would populate; today it is produced by tests and left
/// as the documented contract.
#[derive(Debug, Clone)]
pub struct ThrottleHint {
    /// The server-requested wait before retrying, if it named one.
    pub retry_after: Option<Duration>,
}

impl std::fmt::Display for ThrottleHint {
    /// Renders the throttle and any requested wait for operator logs.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.retry_after {
            Some(d) => write!(f, "origin throttled (retry after {} ms)", d.as_millis()),
            None => write!(f, "origin throttled"),
        }
    }
}

impl std::error::Error for ThrottleHint {}

/// Whether a backend error should be retried, and any server-requested wait.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Retryability {
    /// Transient (throttle/5xx/timeout): retry, waiting `retry_after` if the
    /// origin named one, else the layer's backoff.
    Yes {
        /// A server-requested wait, from a [`ThrottleHint`] on the error.
        retry_after: Option<Duration>,
    },
    /// Terminal (404/403/precondition/…): surface to the client immediately.
    No,
}

/// Classifies a backend error. `Generic` is `object_store`'s bucket for every
/// transient condition, so it is retryable — except a
/// [`crate::credentials::OriginCredentialError`], which must surface without
/// retrying. A [`ThrottleHint`] carries the requested wait through. Every named
/// client-error variant is terminal. Unknown future variants are treated as
/// terminal: never retry something we cannot reason about.
pub(crate) fn classify(err: &object_store::Error) -> Retryability {
    match err {
        object_store::Error::Generic { source, .. } => {
            if source
                .downcast_ref::<crate::credentials::OriginCredentialError>()
                .is_some()
            {
                return Retryability::No;
            }
            let retry_after = source
                .downcast_ref::<ThrottleHint>()
                .and_then(|hint| hint.retry_after);
            Retryability::Yes { retry_after }
        }
        _ => Retryability::No,
    }
}

/// Decorrelated-jitter exponential backoff, mirroring `object_store`'s shape:
/// each delay is a random value between `initial` and twice the previous,
/// capped at `max`. When `initial == max` the schedule is deterministic (a
/// fixed delay) — handy for tests that pin timing.
pub(crate) struct Backoff {
    /// The floor (and first) delay, in milliseconds.
    initial_ms: f64,
    /// The ceiling on any single delay, in milliseconds.
    max_ms: f64,
    /// The previous delay, seeding the next range.
    prev_ms: f64,
    /// xorshift64 state for jitter (no external RNG dependency needed).
    rng: u64,
}

impl Backoff {
    /// A schedule from the policy's initial/max delays.
    pub(crate) fn new(initial_ms: u64, max_ms: u64) -> Self {
        // Seed from the clock so concurrent retriers don't march in lockstep.
        let seed = std::time::Instant::now().elapsed().as_nanos() as u64
            ^ (std::ptr::addr_of!(initial_ms) as u64)
            ^ 0x9E37_79B9_7F4A_7C15;
        Backoff {
            initial_ms: initial_ms as f64,
            max_ms: max_ms as f64,
            prev_ms: initial_ms as f64,
            rng: seed | 1,
        }
    }

    /// A uniform value in `[0, 1)` from the xorshift state.
    fn unit(&mut self) -> f64 {
        let mut x = self.rng;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.rng = x;
        ((x >> 11) as f64) / ((1u64 << 53) as f64)
    }

    /// The next delay: a jittered pick in `[initial, min(max, 2·prev)]`.
    pub(crate) fn next(&mut self) -> Duration {
        let hi = (self.prev_ms * 2.0).min(self.max_ms).max(self.initial_ms);
        let span = (hi - self.initial_ms).max(0.0);
        let ms = self.initial_ms + span * self.unit();
        self.prev_ms = ms;
        Duration::from_secs_f64(ms / 1000.0)
    }
}

/// Classifies a raw-client error, mirroring [`classify`]'s philosophy:
/// [`crate::RawError::Backend`] is the raw path's bucket for every transient
/// condition (transport failures, 5xx, throttles), so it is retryable; every
/// named S3-shaped variant (404/403/304/416) is a terminal answer from the
/// origin.
pub(crate) fn classify_raw(err: &crate::RawError) -> Retryability {
    match err {
        crate::RawError::Backend(_) => Retryability::Yes { retry_after: None },
        _ => Retryability::No,
    }
}
