//! Foreground activity accounting for the cache-node scale-to-zero fence.

use verglas_core::activity::{ActivityPlane, ActivityTracker};

/// A guard records one accepted operation, keeps its plane non-idle while it
/// lives, and releases the count on every drop path.
#[test]
fn activity_guard_tracks_acceptance_and_inflight_lifetime() {
    let tracker = ActivityTracker::new();
    assert!(tracker.snapshot().idle);

    let http = tracker.try_begin(ActivityPlane::Http).expect("accept http");
    let fragment = tracker
        .try_begin(ActivityPlane::Fragment)
        .expect("accept fragment");
    let active = tracker.snapshot();
    assert!(!active.idle);
    assert_eq!(active.accepted, 2);
    assert_eq!(active.inflight, 2);
    assert_eq!(active.planes.http.accepted, 1);
    assert_eq!(active.planes.http.inflight, 1);
    assert_eq!(active.planes.fragment.accepted, 1);
    assert_eq!(active.planes.fragment.inflight, 1);
    assert!(active.last_activity_unix_ms.is_some());

    drop(http);
    assert_eq!(tracker.snapshot().inflight, 1);
    drop(fragment);

    let idle = tracker.snapshot();
    assert!(idle.idle);
    assert_eq!(idle.accepted, 2, "acceptance is monotonic across idle gaps");
    assert_eq!(idle.inflight, 0);
}

/// Every foreground plane contributes to the same stop fence while retaining
/// an independent diagnostic count.
#[test]
fn every_cache_node_plane_participates_in_the_fence() {
    let tracker = ActivityTracker::new();
    let guards = [
        tracker.try_begin(ActivityPlane::Http).expect("http"),
        tracker.try_begin(ActivityPlane::Nbd).expect("nbd"),
        tracker
            .try_begin(ActivityPlane::Fragment)
            .expect("fragment"),
        tracker
            .try_begin(ActivityPlane::Safekeeper)
            .expect("safekeeper"),
    ];

    let snapshot = tracker.snapshot();
    assert_eq!(snapshot.accepted, 4);
    assert_eq!(snapshot.inflight, 4);
    assert_eq!(snapshot.planes.http.inflight, 1);
    assert_eq!(snapshot.planes.nbd.inflight, 1);
    assert_eq!(snapshot.planes.fragment.inflight, 1);
    assert_eq!(snapshot.planes.safekeeper.inflight, 1);

    drop(guards);
    assert!(tracker.snapshot().idle);
}

/// Fencing is atomic with foreground admission and only the current lease may
/// reopen a node after a failed stop attempt.
#[test]
fn fence_rejects_new_work_and_generation_fences_reopen() {
    let tracker = ActivityTracker::new();
    let pre_fence = tracker
        .try_begin(ActivityPlane::Http)
        .expect("accepting before fence");

    let generation = tracker.fence();
    let fenced = tracker.snapshot();
    assert!(!fenced.accepting);
    assert_eq!(fenced.generation, generation);
    assert_eq!(fenced.inflight, 1, "pre-fence work remains visible");
    assert!(tracker.try_begin(ActivityPlane::Nbd).is_err());
    assert_eq!(
        tracker.snapshot().accepted,
        1,
        "rejected work is not accepted"
    );

    drop(pre_fence);
    assert!(tracker.snapshot().idle);
    assert!(!tracker.unfence(generation.saturating_sub(1)));
    assert!(!tracker.snapshot().accepting);
    assert!(tracker.unfence(generation));
    assert!(tracker.snapshot().accepting);
    drop(
        tracker
            .try_begin(ActivityPlane::Safekeeper)
            .expect("reopened current fence"),
    );
}
