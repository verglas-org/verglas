//! Acceptance tests for exact cron reconciliation.

use chrono::{TimeZone, Utc};
use verglas_scheduler::plan_cron;
use verglas_sdk::worker::Catchup;

/// Cron planning emits logical half-open intervals and identifies the next
/// exact deadline rather than requiring a minute polling loop.
#[test]
fn cron_plan_returns_due_intervals_and_next_deadline() {
    let cursor = Utc
        .with_ymd_and_hms(2026, 8, 1, 9, 30, 0)
        .single()
        .expect("cursor");
    let now = Utc
        .with_ymd_and_hms(2026, 8, 1, 12, 15, 0)
        .single()
        .expect("now");
    let plan = plan_cron(Some(cursor), now, None, Catchup::Sequential, "0 * * * *").expect("plan");

    assert_eq!(plan.intervals.len(), 3);
    assert_eq!(plan.intervals[0].interval_start, cursor.to_rfc3339());
    assert_eq!(
        plan.intervals[2].interval_end,
        Utc.with_ymd_and_hms(2026, 8, 1, 12, 0, 0)
            .single()
            .expect("end")
            .to_rfc3339()
    );
    assert_eq!(
        plan.next_wake_at,
        Utc.with_ymd_and_hms(2026, 8, 1, 13, 0, 0)
            .single()
            .expect("next")
    );
}
