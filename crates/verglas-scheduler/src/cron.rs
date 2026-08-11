//! Cron reconciliation into logical intervals and an exact future deadline.

use chrono::{DateTime, Utc};
use verglas_harness::cron::CronSchedule;
use verglas_sdk::worker::{Catchup, CronInterval};

use crate::SchedulerError;

/// The logical intervals to materialize and earliest future scheduler deadline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CronPlan {
    /// Logical intervals that must exist as durable jobs.
    pub intervals: Vec<CronInterval>,
    /// First scheduled instant strictly after the reconciliation time.
    pub next_wake_at: DateTime<Utc>,
}

/// Plans cron jobs to materialize and the next wake without a polling loop.
pub fn plan_cron(
    cursor: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
    start_date: Option<DateTime<Utc>>,
    catchup: Catchup,
    expression: &str,
) -> Result<CronPlan, SchedulerError> {
    let schedule =
        CronSchedule::parse(expression).map_err(|error| SchedulerError::Cron(error.to_string()))?;
    if cursor.is_none() && catchup == Catchup::None {
        let next_wake_at = schedule.next_after(&now).ok_or_else(|| {
            SchedulerError::Cron(format!(
                "`{expression}` has no occurrence in the next four years"
            ))
        })?;
        return Ok(CronPlan {
            intervals: vec![CronInterval {
                logical_date: next_wake_at.to_rfc3339(),
                interval_start: now.to_rfc3339(),
                interval_end: next_wake_at.to_rfc3339(),
            }],
            next_wake_at,
        });
    }
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
    let sequential_backlog_remaining = catchup == Catchup::Sequential && instants.len() > 1;
    if catchup == Catchup::None && instants.len() > 1 {
        instants = instants.last().copied().into_iter().collect::<Vec<_>>();
    } else if sequential_backlog_remaining {
        instants.truncate(1);
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
    let next_wake_at = if sequential_backlog_remaining {
        now
    } else {
        schedule.next_after(&now).ok_or_else(|| {
            SchedulerError::Cron(format!(
                "`{expression}` has no occurrence in the next four years"
            ))
        })?
    };
    Ok(CronPlan {
        intervals,
        next_wake_at,
    })
}
