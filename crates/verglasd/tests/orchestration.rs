//! Removed lifecycle orchestration has no compatibility path.

use verglasd::SuspendFence;

#[test]
fn suspend_fence_contains_only_storage_and_event_confirmations() {
    let fence = SuspendFence::new(true, true, true);
    assert!(fence.pushed());
    assert!(fence.outbox_drained());
    assert!(fence.event_shutdown_clean());
    assert!(fence.is_complete());
}
