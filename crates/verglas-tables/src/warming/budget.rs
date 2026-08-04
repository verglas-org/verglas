//! A reusable byte-rate token bucket for backend-fill politeness.
//!
//! The eager warming pipeline (#168) and the snapshot prefetcher (#51) both
//! flood the backend with speculative fills; left unbounded they would starve
//! organic traffic and violate the "polite tenant" budget invariant. This
//! bucket bounds the *byte rate* those producers may pull from the origin.
//! #51 builds its shared executor on top of this exact component — it lives
//! here (not inside the warming job) so both producers charge one budget.
//!
//! The bucket is a classic leaky/token bucket: tokens (bytes) refill at a
//! steady rate up to a burst capacity; a producer waits until enough tokens
//! exist, then consumes them. It is deliberately boring.

use std::sync::Mutex;

use tokio::time::{Duration, Instant};

/// A byte-budget token bucket shared by warming (#168) and prefetch (#51).
///
/// Tokens are bytes. They accrue at `refill_per_sec` up to `capacity` (the
/// burst ceiling) and are consumed by [`acquire`](TokenBucket::acquire) before
/// a fill is issued. Cheap to share: all state is behind one small mutex held
/// only for the arithmetic, never across an await.
pub struct TokenBucket {
    /// Burst ceiling in bytes; also the largest single grant.
    capacity: u64,
    /// Steady refill rate in bytes per second. Zero only for [`unlimited`].
    refill_per_sec: u64,
    /// `true` disables all accounting: every acquire returns immediately.
    unlimited: bool,
    /// Mutable accounting; guarded, never held across `.await`.
    state: Mutex<State>,
}

/// The bucket's mutable accounting.
struct State {
    /// Currently available tokens (bytes), fractional so slow rates still
    /// accrue.
    tokens: f64,
    /// When `tokens` was last refilled.
    last: Instant,
}

impl TokenBucket {
    /// Builds a bucket that refills at `refill_bytes_per_sec` up to a burst of
    /// `capacity_bytes`, starting full so warming can begin immediately.
    ///
    /// A `capacity_bytes` of zero is promoted to `refill_bytes_per_sec` (one
    /// second of burst) so a single fill is never permanently un-grantable.
    pub fn new(capacity_bytes: u64, refill_bytes_per_sec: u64) -> TokenBucket {
        let capacity = capacity_bytes.max(refill_bytes_per_sec).max(1);
        TokenBucket {
            capacity,
            refill_per_sec: refill_bytes_per_sec.max(1),
            unlimited: false,
            state: Mutex::new(State {
                tokens: capacity as f64,
                last: Instant::now(),
            }),
        }
    }

    /// Builds a bucket that never throttles — every acquire returns at once.
    /// Used when a byte budget is configured off, and in tests that isolate
    /// the concurrency semaphore from the rate limiter.
    pub fn unlimited() -> TokenBucket {
        TokenBucket {
            capacity: u64::MAX,
            refill_per_sec: 0,
            unlimited: true,
            state: Mutex::new(State {
                tokens: 0.0,
                last: Instant::now(),
            }),
        }
    }

    /// The burst ceiling (largest single grant) in bytes.
    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    /// Refills the bucket for elapsed time, saturating at `capacity`. Assumes
    /// `state` is already locked and stamps the refill time.
    fn refill_locked(&self, state: &mut State, now: Instant) {
        let elapsed = now.saturating_duration_since(state.last).as_secs_f64();
        if elapsed > 0.0 {
            state.tokens =
                (state.tokens + elapsed * self.refill_per_sec as f64).min(self.capacity as f64);
            state.last = now;
        }
    }

    /// Consumes `bytes` tokens if available right now, returning whether it
    /// succeeded. Never blocks. A request larger than `capacity` is clamped to
    /// `capacity` so it can succeed once the bucket is full (never a deadlock).
    pub fn try_acquire(&self, bytes: u64) -> bool {
        if self.unlimited {
            return true;
        }
        let want = bytes.min(self.capacity) as f64;
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        self.refill_locked(&mut state, Instant::now());
        if state.tokens >= want {
            state.tokens -= want;
            true
        } else {
            false
        }
    }

    /// Waits until `bytes` tokens are available, then consumes them.
    ///
    /// A request larger than `capacity` is clamped to `capacity` (documented
    /// on [`try_acquire`](TokenBucket::try_acquire)); the caller still gets its
    /// bytes, the budget just cannot represent a burst bigger than its ceiling.
    /// The lock is released before every sleep, so many warmers share one
    /// bucket without serializing on each other's waits.
    pub async fn acquire(&self, bytes: u64) {
        if self.unlimited {
            return;
        }
        let want = bytes.min(self.capacity) as f64;
        loop {
            let wait = {
                let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
                self.refill_locked(&mut state, Instant::now());
                if state.tokens >= want {
                    state.tokens -= want;
                    return;
                }
                let deficit = want - state.tokens;
                Duration::from_secs_f64(deficit / self.refill_per_sec as f64)
            };
            tokio::time::sleep(wait).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A full bucket grants up to its capacity immediately, then refuses the
    /// next byte until time passes.
    #[tokio::test(start_paused = true)]
    async fn full_bucket_grants_capacity_then_refuses() {
        let bucket = TokenBucket::new(1000, 1000);
        assert!(bucket.try_acquire(1000), "starts full");
        assert!(!bucket.try_acquire(1), "drained: no tokens left");
    }

    /// Tokens accrue at the refill rate: after half a second at 1000 B/s, 500
    /// bytes are grantable and no more.
    #[tokio::test(start_paused = true)]
    async fn refills_at_the_configured_rate() {
        let bucket = TokenBucket::new(1000, 1000);
        assert!(bucket.try_acquire(1000), "drain");
        tokio::time::advance(Duration::from_millis(500)).await;
        assert!(bucket.try_acquire(500), "half a second refilled 500 bytes");
        assert!(!bucket.try_acquire(1), "not a byte more");
    }

    /// `acquire` blocks until enough tokens refill, and paused time proves it
    /// waited rather than over-granting.
    #[tokio::test(start_paused = true)]
    async fn acquire_waits_for_refill() {
        let bucket = TokenBucket::new(1000, 1000);
        bucket.acquire(1000).await; // drains instantly
        let start = Instant::now();
        bucket.acquire(250).await; // needs 250ms of refill
        assert_eq!(
            start.elapsed(),
            Duration::from_millis(250),
            "acquire waited exactly for the deficit to refill"
        );
    }

    /// A single request larger than capacity is clamped so it cannot deadlock,
    /// and still eventually succeeds from a full bucket.
    #[tokio::test(start_paused = true)]
    async fn oversized_request_clamps_to_capacity() {
        let bucket = TokenBucket::new(100, 100);
        assert!(
            bucket.try_acquire(10_000),
            "oversized request clamps to capacity and succeeds when full"
        );
    }

    /// The unlimited bucket never throttles.
    #[tokio::test(start_paused = true)]
    async fn unlimited_never_blocks() {
        let bucket = TokenBucket::unlimited();
        for _ in 0..1000 {
            assert!(bucket.try_acquire(u64::MAX));
        }
        bucket.acquire(u64::MAX).await;
    }
}
