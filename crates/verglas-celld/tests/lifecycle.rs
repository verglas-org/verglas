//! Durable storage checkpoint and clean event shutdown lifecycle tests.

use verglas_celld::{ChildLifecycle, ChildState, LifecycleError, SuspendFence};

#[test]
fn child_stops_only_after_storage_checkpoint_outbox_drain_and_clean_shutdown() {
    let mut child = ChildLifecycle::running();
    assert_eq!(
        child.suspend(SuspendFence::new(true, false, true)),
        Err(LifecycleError::SuspendUnconfirmed)
    );
    assert_eq!(child.state(), ChildState::Running);
    child
        .suspend(SuspendFence::new(true, true, true))
        .expect("safe suspend");
    assert_eq!(child.state(), ChildState::Suspended);
}

#[test]
fn restore_is_unroutable_until_event_socket_ready() {
    let mut child = ChildLifecycle::running();
    child
        .suspend(SuspendFence::new(true, true, true))
        .expect("safe suspend");
    child.begin_restore().expect("begin restore");
    assert_eq!(child.state(), ChildState::Restoring);
    assert!(!child.may_execute_event());
    child.finish_restore().expect("finish restore");
    assert!(child.may_execute_event());
}
