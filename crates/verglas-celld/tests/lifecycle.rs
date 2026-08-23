//! Durable child suspension, restore, and role-gating tests.

use verglas_celld::{ChildLifecycle, ChildState, LifecycleError, ReplicaRole, SuspendFence};

#[test]
fn durable_child_stops_only_after_archive_and_checkpoint_cover_applied_state() {
    let mut child = ChildLifecycle::running(ReplicaRole::Leader, 12);
    let incomplete = SuspendFence::new(12, 11, 12);
    assert!(matches!(
        child.suspend(incomplete),
        Err(LifecycleError::Unarchived {
            applied: 12,
            archived: 11
        })
    ));
    assert_eq!(child.state(), ChildState::Running(ReplicaRole::Leader));

    child
        .suspend(SuspendFence::new(12, 12, 12))
        .expect("safe suspend");
    assert_eq!(child.state(), ChildState::Suspended);
}

#[test]
fn restore_must_reach_the_requested_fence_before_events_run() {
    let mut child = ChildLifecycle::running(ReplicaRole::Follower, 8);
    child
        .suspend(SuspendFence::new(8, 8, 8))
        .expect("safe suspend");
    child.begin_restore(10).expect("begin restore");
    assert!(matches!(
        child.finish_restore(ReplicaRole::Leader, 9),
        Err(LifecycleError::RestoreBehind {
            required: 10,
            restored: 9
        })
    ));
    child
        .finish_restore(ReplicaRole::Leader, 10)
        .expect("restore catches up");
    assert!(child.may_execute_stateful_event());
}

#[test]
fn follower_never_executes_stateful_worker_events() {
    let follower = ChildLifecycle::running(ReplicaRole::Follower, 5);
    assert!(!follower.may_execute_stateful_event());
    assert!(follower.may_serve_snapshot(5));
    assert!(!follower.may_serve_snapshot(6));
}
