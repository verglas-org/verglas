//! Cron reconciliation into logical intervals and an exact future deadline.

use chrono::{DateTime, Utc};
use verglas_harness::cron::CronSchedule;
use verglas_sdk::worker::{Catchup, CronInterval};

use crate::SchedulerError;

/// The due logical intervals and earliest future scheduler deadline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CronPlan {
    /// Logical intervals due at the reconciliation time.
    pub intervals: Vec<CronInterval>,
    /// First scheduled instant strictly after the reconciliation time.
    pub next_wake_at: DateTime<Utc>,
}

/// Plans due cron intervals and the next wake without a continuous polling loop.
pub fn plan_cron(
    cursor: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
    start_date: Option<DateTime<Utc>>,
    catchup: Catchup,
    expression: &str,
) -> Result<CronPlan, SchedulerError> {
    let schedule =
        CronSchedule::parse(expression).map_err(|error| SchedulerError::Cron(error.to_string()))?;
    let from = cursor
        .or(if catchup == Catchup::None {
            None
        } else {
            start_date
        })
        .unwrap_or(now);
    let mut instants = Vec::new();
    let mut candidate = schedule.next_after(&from);
    while let Some(instant) = candidate {
        if instant > now {
            break;
        }
        instants.push(instant);
        candidate = schedule.next_after(&instant);
    }
    if catchup == Catchup::None && instants.len() > 1 {
        instants = instants.last().copied().into_iter().collect::<Vec<_>>();
    }
    let mut previous = from;
    let intervals = instants
        .into_iter()
        .map(|instant| {
            let interval = CronInterval {
                logical_date: instant.to_rfc3339(),
                interval_start: previous.to_rfc3339(),
                interval_end: instant.to_rfc3339(),
            };
            previous = instant;
            interval
        })
        .collect();
    let next_wake_at = schedule.next_after(&now).ok_or_else(|| {
        SchedulerError::Cron(format!(
            "`{expression}` has no occurrence in the next four years"
        ))
    })?;
    Ok(CronPlan {
        intervals,
        next_wake_at,
    })
}
