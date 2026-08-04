//! The per-bucket circuit breaker (issue #20): sustained throttle/5xx failures
//! trip it open, it sheds load (rejects admissions) for a cooldown, then
//! half-opens to probe recovery — closing only once a run of probes succeeds,
//! reopening the moment one fails. Driven through a manual clock so the
//! cooldown transition is deterministic and does not sleep.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::SeqCst};

use verglas_backend::{Admission, BreakerState, CircuitBreaker, Clock};
use verglas_core::config::BreakerPolicy;

/// A hand-cranked monotonic clock: tests advance it explicitly, so the breaker's
/// cooldown fires exactly when the test says and never on wall time.
#[derive(Default)]
struct ManualClock(AtomicU64);

impl ManualClock {
    /// Moves the clock forward by `ms` milliseconds.
    fn advance(&self, ms: u64) {
        self.0.fetch_add(ms, SeqCst);
    }
}

impl Clock for ManualClock {
    fn now_ms(&self) -> u64 {
        self.0.load(SeqCst)
    }
}

/// A small breaker: trips at 50% over 4 samples, 1 s cooldown, 2 probes to
/// recover — the same shape as production, just small enough to drive by hand.
fn small_breaker() -> (Arc<CircuitBreaker>, Arc<ManualClock>) {
    let clock = Arc::new(ManualClock::default());
    let policy = BreakerPolicy {
        failure_rate: 0.5,
        min_samples: 4,
        cooldown_ms: 1_000,
        half_open_max_probes: 2,
    };
    let breaker = Arc::new(CircuitBreaker::new(policy, clock.clone()));
    (breaker, clock)
}

/// Admits a request and immediately records `ok` for it — the common closed-path
/// motion in these tests.
fn feed(breaker: &CircuitBreaker, ok: bool) {
    match breaker.admit() {
        Admission::Proceed(ticket) => breaker.record(ticket, ok),
        Admission::Reject => panic!("expected admission while closed"),
    }
}

#[test]
fn closed_breaker_admits_and_does_not_trip_below_min_samples() {
    let (breaker, _clock) = small_breaker();
    // Three failures is below the 4-sample floor: the rate is not yet trusted.
    feed(&breaker, false);
    feed(&breaker, false);
    feed(&breaker, false);
    assert_eq!(
        breaker.state(),
        BreakerState::Closed,
        "too few samples to trip"
    );
    assert!(
        matches!(breaker.admit(), Admission::Proceed(_)),
        "a closed breaker keeps admitting"
    );
}

#[test]
fn sustained_failures_trip_the_breaker_open() {
    let (breaker, _clock) = small_breaker();
    for _ in 0..4 {
        feed(&breaker, false);
    }
    assert_eq!(breaker.state(), BreakerState::Open, "4/4 failures trip it");
    // While open it sheds load: admission is refused.
    assert!(
        matches!(breaker.admit(), Admission::Reject),
        "an open breaker rejects to shed load"
    );
    assert_eq!(breaker.snapshot().trips, 1, "one trip recorded");
    assert!(
        breaker.snapshot().rejections >= 1,
        "the shed load is counted"
    );
}

#[test]
fn healthy_traffic_keeps_the_breaker_closed() {
    let (breaker, _clock) = small_breaker();
    // One failure per three successes: 25% < 50% threshold, never trips.
    for _ in 0..3 {
        feed(&breaker, true);
        feed(&breaker, true);
        feed(&breaker, true);
        feed(&breaker, false);
    }
    assert_eq!(breaker.state(), BreakerState::Closed);
}

#[test]
fn open_breaker_half_opens_after_cooldown_and_closes_on_probe_successes() {
    let (breaker, clock) = small_breaker();
    for _ in 0..4 {
        feed(&breaker, false);
    }
    assert_eq!(breaker.state(), BreakerState::Open);

    // Before the cooldown elapses, still shedding.
    clock.advance(999);
    assert!(
        matches!(breaker.admit(), Admission::Reject),
        "still cooling down"
    );

    // Cooldown elapsed: the next admission is a probe.
    clock.advance(1);
    let ticket = match breaker.admit() {
        Admission::Proceed(t) => t,
        Admission::Reject => panic!("cooldown elapsed: a probe must be admitted"),
    };
    assert_eq!(breaker.state(), BreakerState::HalfOpen);
    breaker.record(ticket, true);

    // A second successful probe (max_probes = 2) closes the breaker.
    let ticket = match breaker.admit() {
        Admission::Proceed(t) => t,
        Admission::Reject => panic!("half-open admits up to the probe budget"),
    };
    breaker.record(ticket, true);
    assert_eq!(
        breaker.state(),
        BreakerState::Closed,
        "probes succeeded: recovered"
    );
    assert!(
        matches!(breaker.admit(), Admission::Proceed(_)),
        "a recovered breaker admits normally"
    );
}

#[test]
fn a_failed_probe_reopens_the_breaker() {
    let (breaker, clock) = small_breaker();
    for _ in 0..4 {
        feed(&breaker, false);
    }
    clock.advance(1_000);
    let ticket = match breaker.admit() {
        Admission::Proceed(t) => t,
        Admission::Reject => panic!("cooldown elapsed: probe expected"),
    };
    assert_eq!(breaker.state(), BreakerState::HalfOpen);
    // The probe fails: back to open for another cooldown.
    breaker.record(ticket, false);
    assert_eq!(
        breaker.state(),
        BreakerState::Open,
        "a failed probe reopens"
    );
    assert!(
        matches!(breaker.admit(), Admission::Reject),
        "shedding again"
    );
    assert_eq!(breaker.snapshot().trips, 2, "the reopen counts as a trip");
}

#[test]
fn half_open_sheds_beyond_the_probe_budget() {
    let (breaker, clock) = small_breaker();
    for _ in 0..4 {
        feed(&breaker, false);
    }
    clock.advance(1_000);
    // Two probes are the budget; a third concurrent admission is shed.
    let _p1 = match breaker.admit() {
        Admission::Proceed(t) => t,
        Admission::Reject => panic!("first probe admitted"),
    };
    let _p2 = match breaker.admit() {
        Admission::Proceed(t) => t,
        Admission::Reject => panic!("second probe admitted"),
    };
    assert!(
        matches!(breaker.admit(), Admission::Reject),
        "half-open admits at most `half_open_max_probes` at once"
    );
}
