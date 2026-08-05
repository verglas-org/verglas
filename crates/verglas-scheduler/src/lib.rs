//! Stateless scheduling contracts and Postgres persistence for Verglas workers.
//!
//! A scheduler process can disappear between calls because jobs, fenced leases,
//! retries, and results live in Postgres. The process owns timing and execution,
//! while producers submit complete bounded events over HTTP.

mod cron;
mod queue;

pub use cron::{CronPlan, plan_cron};
pub use queue::{
    Attempt, ClaimRequest, ClaimedJob, CompleteRequest, Completion, EnqueueOutcome, Invocation,
    Job, Lease, LeaseError, NextWakeRequest, PgQueue, RenewRequest, RunQueue, SchedulerError,
};
