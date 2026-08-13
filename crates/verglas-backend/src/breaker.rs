//! A per-bucket circuit breaker (issue #20): the load-shedding half of backend
//! resilience.
//!
//! Retry/backoff makes a *single* request polite; the breaker makes the *fleet*
//! polite. When a bucket's origin is sustainedly unhealthy — throttling or 5xx
//! at a rate over the configured threshold — the breaker trips **open** and
//! sheds fill load for a cooldown, so a struggling origin is given room to
//! recover instead of being hammered by a retry storm. Cache hits keep serving
//! (that is upstream of this breaker); only misses, which would have to fill, fail
//! fast. After the cooldown it goes **half-open**, admitting a small number of
//! probes: a run of successes **closes** it, a single failure re-**opens** it.
//!
//! The breaker's [`state`](CircuitBreaker::state) is a reusable integration point other
//! subsystems consult — e.g. #51's snapshot-driven prefetch yields to an open
//! breaker rather than piling speculative fills onto a sick origin.

use std::collections::VecDeque;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use verglas_core::config::BreakerPolicy;

/// A monotonic millisecond clock. The trait exists so the cooldown transition is
/// testable without wall-clock sleeps; production uses [`SystemClock`].
pub trait Clock: Send + Sync {
    /// Milliseconds elapsed on some arbitrary monotonic origin. Only
    /// differences are meaningful.
    fn now_ms(&self) -> u64;
}

/// The production clock: milliseconds since the breaker was constructed.
#[derive(Debug)]
pub struct SystemClock {
    /// The monotonic origin all readings are measured from.
    origin: Instant,
}

impl SystemClock {
    /// A clock whose zero is now.
    pub fn new() -> Self {
        SystemClock {
            origin: Instant::now(),
        }
    }
}

impl Default for SystemClock {
    /// Same as [`SystemClock::new`].
    fn default() -> Self {
        SystemClock::new()
    }
}

impl Clock for SystemClock {
    /// Milliseconds since construction, saturating (a node up for 5×10^8 years
    /// would wrap a u64 of ms — not a concern).
    fn now_ms(&self) -> u64 {
        self.origin.elapsed().as_millis() as u64
    }
}

/// The breaker's coarse state, as reported to callers and the prefetch integration point.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BreakerState {
    /// Requests flow; outcomes are sampled for the trip decision.
    Closed,
    /// Load is shed: admissions are refused until the cooldown elapses.
    Open,
    /// Probing recovery: a bounded number of requests are let through.
    HalfOpen,
}

/// The outcome of asking the breaker whether a request may proceed.
pub enum Admission {
    /// Proceed; the [`Ticket`] must be handed back to
    /// [`record`](CircuitBreaker::record) with the request's outcome.
    Proceed(Ticket),
    /// The breaker is shedding load — fail the request fast.
    Reject,
}

/// Proof of admission, returned by [`CircuitBreaker::admit`] and consumed by
/// [`CircuitBreaker::record`]. Carries whether the admission was a half-open
/// probe, so the recorded outcome is routed to the right transition. Opaque by
/// design: only the breaker mints and interprets it.
pub struct Ticket {
    /// True if this admission was a half-open probe.
    probe: bool,
}

/// The mutable interior, guarded by one mutex. The breaker is off the hot path
/// (consulted once per *fill*, not per cache hit), so a mutex is fine.
#[derive(Debug)]
enum StateInner {
    /// Closed: sampling outcomes into the rolling window.
    Closed,
    /// Open until `until_ms` (clock time), shedding load.
    Open {
        /// Clock time at which the breaker becomes eligible to half-open.
        until_ms: u64,
    },
    /// Half-open: `in_flight` probes outstanding, `successes` recorded so far.
    HalfOpen {
        /// Probes currently admitted and not yet recorded.
        in_flight: u32,
        /// Consecutive probe successes accumulated toward closing.
        successes: u32,
    },
}

/// The breaker's guarded interior.
#[derive(Debug)]
struct Inner {
    /// Current state-machine node.
    state: StateInner,
    /// Rolling window of recent closed-state outcomes (true = success), capped
    /// at `min_samples`. Cleared on every state transition so a fresh window
    /// decides each new trip.
    window: VecDeque<bool>,
}

/// A point-in-time view of the breaker for logging/metrics (#46 will scrape it).
#[derive(Debug, Clone, Copy)]
pub struct BreakerSnapshot {
    /// Current coarse state.
    pub state: BreakerState,
    /// Times the breaker has opened (initial trips + re-opens from failed
    /// probes).
    pub trips: u64,
    /// Requests shed while open or over the half-open probe budget.
    pub rejections: u64,
}

/// A per-bucket circuit breaker driving the closed→open→half-open→closed state
/// machine from a [`BreakerPolicy`] and a [`Clock`].
pub struct CircuitBreaker {
    /// Tuning: trip threshold, sample floor, cooldown, probe budget.
    policy: BreakerPolicy,
    /// Time source (real in production, hand-cranked in tests).
    clock: std::sync::Arc<dyn Clock>,
    /// Guarded state + outcome window.
    inner: Mutex<Inner>,
    /// Monotonic count of opens.
    trips: AtomicU64,
    /// Monotonic count of shed requests.
    rejections: AtomicU64,
}

impl std::fmt::Debug for CircuitBreaker {
    /// Names the policy and current snapshot; the clock is not printable.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CircuitBreaker")
            .field("policy", &self.policy)
            .field("snapshot", &self.snapshot())
            .finish_non_exhaustive()
    }
}

impl CircuitBreaker {
    /// Builds a breaker with an explicit clock (tests inject a manual one).
    pub fn new(policy: BreakerPolicy, clock: std::sync::Arc<dyn Clock>) -> Self {
        CircuitBreaker {
            policy,
            clock,
            inner: Mutex::new(Inner {
                state: StateInner::Closed,
                window: VecDeque::new(),
            }),
            trips: AtomicU64::new(0),
            rejections: AtomicU64::new(0),
        }
    }

    /// Builds a breaker on the real monotonic clock — the production path.
    pub fn production(policy: BreakerPolicy) -> Self {
        CircuitBreaker::new(policy, std::sync::Arc::new(SystemClock::new()))
    }

    /// Locks the interior, recovering from a poisoned mutex (a panic elsewhere
    /// must not block every future fill).
    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Decides whether a request may proceed, driving the time-based
    /// open→half-open transition and reserving a half-open probe slot when it
    /// admits one. A [`Reject`](Admission::Reject) is the load-shed signal.
    pub fn admit(&self) -> Admission {
        let now = self.clock.now_ms();
        let mut inner = self.lock();
        match &mut inner.state {
            StateInner::Closed => Admission::Proceed(Ticket { probe: false }),
            StateInner::Open { until_ms } => {
                if now >= *until_ms {
                    inner.state = StateInner::HalfOpen {
                        in_flight: 1,
                        successes: 0,
                    };
                    Admission::Proceed(Ticket { probe: true })
                } else {
                    self.rejections.fetch_add(1, Ordering::Relaxed);
                    Admission::Reject
                }
            }
            StateInner::HalfOpen { in_flight, .. } => {
                if *in_flight < self.policy.half_open_max_probes {
                    *in_flight += 1;
                    Admission::Proceed(Ticket { probe: true })
                } else {
                    self.rejections.fetch_add(1, Ordering::Relaxed);
                    Admission::Reject
                }
            }
        }
    }

    /// Records the outcome of an admitted request. A probe outcome drives the
    /// half-open decision (a full run of successes closes, one failure
    /// re-opens); a closed-state outcome feeds the rolling window and may trip
    /// the breaker. `ok` means "the origin is healthy" — a 404/403 is `ok`
    /// (the origin answered), only throttle/5xx is `!ok`.
    pub fn record(&self, ticket: Ticket, ok: bool) {
        let now = self.clock.now_ms();
        let mut inner = self.lock();
        if ticket.probe {
            // A late probe outcome after the state already moved on is ignored.
            if let StateInner::HalfOpen {
                in_flight,
                successes,
            } = &mut inner.state
            {
                *in_flight = in_flight.saturating_sub(1);
                if ok {
                    *successes += 1;
                    if *successes >= self.policy.half_open_max_probes {
                        inner.state = StateInner::Closed;
                        inner.window.clear();
                    }
                } else {
                    self.open(&mut inner, now);
                }
            }
        } else if matches!(inner.state, StateInner::Closed) {
            let floor = self.policy.min_samples as usize;
            inner.window.push_back(ok);
            while inner.window.len() > floor {
                inner.window.pop_front();
            }
            if inner.window.len() >= floor {
                let failures = inner.window.iter().filter(|ok| !**ok).count();
                let rate = failures as f64 / inner.window.len() as f64;
                if rate >= self.policy.failure_rate {
                    self.open(&mut inner, now);
                }
            }
        }
    }

    /// Trips (or re-trips) the breaker: opens it for the cooldown, clears the
    /// window, and counts the trip.
    fn open(&self, inner: &mut Inner, now: u64) {
        inner.state = StateInner::Open {
            until_ms: now + self.policy.cooldown_ms,
        };
        inner.window.clear();
        self.trips.fetch_add(1, Ordering::Relaxed);
    }

    /// The current coarse state, without driving any transition. Note an
    /// `Open` whose cooldown has elapsed still reads `Open` until the next
    /// [`admit`](CircuitBreaker::admit) promotes it to half-open — the integration point
    /// reports committed state, not a prediction.
    pub fn state(&self) -> BreakerState {
        match self.lock().state {
            StateInner::Closed => BreakerState::Closed,
            StateInner::Open { .. } => BreakerState::Open,
            StateInner::HalfOpen { .. } => BreakerState::HalfOpen,
        }
    }

    /// A snapshot of state and counters for logging/metrics.
    pub fn snapshot(&self) -> BreakerSnapshot {
        BreakerSnapshot {
            state: self.state(),
            trips: self.trips.load(Ordering::Relaxed),
            rejections: self.rejections.load(Ordering::Relaxed),
        }
    }
}
