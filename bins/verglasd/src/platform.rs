//! The daemon-hosted worker runtime (WHITEPAPER §7.3/§7.4).
//!
//! The daemon runs the agent-data platform's workers as internal tasks. A worker
//! is code plus triggers ([`verglas_sdk::worker`]); the runtime is the local
//! executor for those triggers, replacing the former source/MV/sink executors:
//!
//! - **cron** — a local scheduler. Each tick reads the active workers, and for a
//!   worker with a cron trigger it computes the logical intervals due since a
//!   durable cursor (Airflow-style catchup: sequential, parallel, or none) and
//!   runs one worker subprocess per interval.
//! - **data_change** — driven off the daemon's own commit path.
//!   [`WorkerSupervisor::on_table_commit`] fans a completed table commit out to
//!   every worker whose `data_change` trigger names that table.
//! - **webhook** — routed in over the admin API (`POST /v1/hooks/<name>`), which
//!   runs the named worker now.
//! - **websocket** — a local stub: the daemon reports it is unsupported locally
//!   rather than pretending to serve it.
//!
//! # Cache-pathed I/O (§7.4)
//!
//! Each worker's client connects back to the daemon's own loopback endpoint, so
//! every read and commit a worker does flows through the cache and the ownership
//! ring as a side effect. This is a wiring property, not a code path: the worker
//! subprocess is transport-agnostic.

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use iceberg::Catalog;

use verglas_harness::commit::{HarnessError, WatermarkStore};
use verglas_harness::cron::CronSchedule;
use verglas_harness::worker::{WorkerExec, run_worker};
use verglas_platform::{SystemCatalog, SystemState, WorkerRow};
use verglas_sdk::worker::{Catchup, ChangeEvent, CronInterval, TriggerEvent, TriggerSpec};

/// A durable [`WatermarkStore`] over `verglas_sys.watermarks`.
///
/// The worker runtime stores each cron worker's scheduling cursor (the last
/// instant it evaluated the schedule to) here, through the same append-only
/// revision table the `/v1/watermark` routes serve. The value is opaque — the
/// runtime serializes the cursor as an RFC 3339 string.
pub struct SysWatermarkStore {
    sys: Arc<SystemCatalog>,
}

impl SysWatermarkStore {
    /// Wraps the daemon's system catalog handle.
    pub fn new(sys: Arc<SystemCatalog>) -> SysWatermarkStore {
        SysWatermarkStore { sys }
    }
}

#[async_trait]
impl WatermarkStore for SysWatermarkStore {
    /// Reads the deployment's highest-revision watermark, or `None`.
    async fn get(&self, deployment: &str) -> Result<Option<String>, HarnessError> {
        self.sys
            .get_watermark(deployment)
            .await
            .map(|row| row.map(|r| r.watermark))
            .map_err(|e| HarnessError::Watermark(e.to_string()))
    }

    /// Appends the deployment's watermark as the next revision.
    async fn set(&self, deployment: &str, watermark: String) -> Result<(), HarnessError> {
        self.sys
            .set_watermark(deployment, watermark)
            .await
            .map(|_| ())
            .map_err(|e| HarnessError::Watermark(e.to_string()))
    }
}

/// Parses a worker's `triggers` JSON into trigger specs. A malformed triggers
/// column yields no triggers rather than failing the tick — the worker simply
/// never fires until it is redeclared.
pub fn parse_triggers(worker: &WorkerRow) -> Vec<TriggerSpec> {
    serde_json::from_str::<Vec<TriggerSpec>>(&worker.triggers).unwrap_or_else(|e| {
        tracing::warn!(
            "worker {}: triggers column is not a TriggerSpec array ({e}); no triggers active",
            worker.name
        );
        Vec::new()
    })
}

/// The first cron trigger on a worker, if any.
fn cron_trigger(worker: &WorkerRow) -> Option<(String, Option<String>, Catchup)> {
    parse_triggers(worker).into_iter().find_map(|t| match t {
        TriggerSpec::Cron {
            schedule,
            start_date,
            catchup,
        } => Some((schedule, start_date, catchup.unwrap_or_default())),
        _ => None,
    })
}

/// The cron intervals a worker owes at `now`, plus the cursor to store after.
///
/// `cursor` is the last instant the schedule was evaluated to. On the first tick
/// (`cursor` is `None`) the lower bound is `start_date` when catchup backfills,
/// otherwise `now` — so a fresh worker with no catchup fires no backlog. The due
/// instants are the schedule points in `(from, now]`; with catchup `None` only
/// the most recent is kept (the backlog is skipped). Each interval is half-open
/// `[previous_instant, instant)` with `logical_date` the instant, mirroring the
/// TS cron trigger event. The new cursor is always `now`.
pub fn plan_cron_runs(
    cursor: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
    start_date: Option<DateTime<Utc>>,
    catchup: Catchup,
    schedule: &CronSchedule,
) -> (Vec<CronInterval>, DateTime<Utc>) {
    let from = cursor
        .or(if catchup == Catchup::None {
            None
        } else {
            start_date
        })
        .unwrap_or(now);

    let mut instants = Vec::new();
    let mut c = schedule.next_after(&from);
    while let Some(instant) = c {
        if instant > now {
            break;
        }
        instants.push(instant);
        c = schedule.next_after(&instant);
    }
    if catchup == Catchup::None && instants.len() > 1 {
        instants = vec![*instants.last().expect("non-empty")];
    }

    let mut intervals = Vec::with_capacity(instants.len());
    let mut prev = from;
    for instant in instants {
        intervals.push(CronInterval {
            logical_date: Some(instant.to_rfc3339()),
            interval_start: Some(prev.to_rfc3339()),
            interval_end: Some(instant.to_rfc3339()),
        });
        prev = instant;
    }
    (intervals, now)
}

/// The tables a worker's `data_change` triggers follow.
fn data_change_tables(worker: &WorkerRow) -> Vec<String> {
    parse_triggers(worker)
        .into_iter()
        .filter_map(|t| match t {
            TriggerSpec::DataChange { table } => Some(table.tables()),
            _ => None,
        })
        .flatten()
        .collect()
}

/// The worker runtime: the config the background tick loop and the commit
/// fan-out run with. Held by the daemon for the process lifetime.
pub struct WorkerSupervisor {
    /// The loopback catalog run logging commits through.
    pub catalog: Arc<dyn Catalog>,
    /// The system registry the workers are read from.
    pub sys: Arc<SystemCatalog>,
    /// The guard directory `run_guarded` single-flight and backoff files live
    /// under.
    pub guard_dir: PathBuf,
    /// The loopback data-plane endpoint a worker's client connects back to.
    pub endpoint: String,
}

impl WorkerSupervisor {
    /// Runs one supervisor tick at `now`: every active worker's due cron
    /// intervals, under the guard. A single worker's failure never poisons the
    /// tick.
    pub async fn tick(&self, now: &DateTime<Utc>) {
        let workers = match self.sys.list_active_workers().await {
            Ok(workers) => workers,
            Err(e) => {
                tracing::warn!("worker runtime: reading the workers registry failed: {e}");
                return;
            }
        };
        for worker in workers {
            if worker.state != SystemState::Running {
                continue;
            }
            self.tick_worker(&worker, now).await;
        }
    }

    /// Runs the due cron intervals for one worker and advances its cursor.
    async fn tick_worker(&self, worker: &WorkerRow, now: &DateTime<Utc>) {
        let Some((expr, start_date, catchup)) = cron_trigger(worker) else {
            return; // no cron trigger: driven by data_change / webhook instead
        };
        let schedule = match CronSchedule::parse(&expr) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!("worker {}: bad cron schedule `{expr}`: {e}", worker.name);
                return;
            }
        };
        let watermarks = SysWatermarkStore::new(self.sys.clone());
        let cursor = watermarks
            .get(&worker.name)
            .await
            .ok()
            .flatten()
            .and_then(|raw| DateTime::parse_from_rfc3339(&raw).ok())
            .map(|dt| dt.with_timezone(&Utc));
        let start = start_date
            .as_deref()
            .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
            .map(|dt| dt.with_timezone(&Utc));

        let (intervals, next_cursor) = plan_cron_runs(cursor, *now, start, catchup, &schedule);

        for interval in intervals {
            let event = TriggerEvent::Cron(interval.clone());
            let window = match (catchup, &interval.logical_date) {
                // Parallel catchup keys the guard by logical date so the backlog
                // fans out (still bounded by the host-wide cap); otherwise the
                // worker name single-flights.
                (Catchup::Parallel, Some(ld)) => format!("{}:{ld}", worker.name),
                _ => worker.name.clone(),
            };
            self.guarded_run(worker, &window, &event).await;
        }

        if let Err(e) = watermarks.set(&worker.name, next_cursor.to_rfc3339()).await {
            tracing::warn!(
                "worker {}: advancing the cron cursor failed: {e}",
                worker.name
            );
        }
    }

    /// Runs one worker for `event` under the guard policy, logging the outcome.
    async fn guarded_run(&self, worker: &WorkerRow, window: &str, event: &TriggerEvent) {
        let exec = match WorkerExec::from_config(&worker.name, &worker.code) {
            Ok(exec) => exec,
            Err(e) => {
                tracing::warn!("worker {}: {e}", worker.name);
                return;
            }
        };
        let output = worker.output.clone().unwrap_or_default();
        let run = verglas_harness::worker::WorkerRun {
            deployment: &worker.name,
            output: &output,
            endpoint: &self.endpoint,
            token: "",
        };
        let guarded = verglas_harness::run_guarded(&self.guard_dir, window, || {
            run_worker(&run, &exec, event)
        })
        .await;
        match guarded {
            verglas_harness::Guarded::Ran(Ok(outcome)) => {
                tracing::info!(
                    "worker {} ({}) wrote {} row(s)",
                    worker.name,
                    event.kind(),
                    outcome.rows_produced
                )
            }
            verglas_harness::Guarded::Ran(Err(e)) => {
                tracing::warn!("worker {} failed: {e}", worker.name)
            }
            verglas_harness::Guarded::Skipped(reason) => {
                tracing::debug!("worker {} skipped: {reason:?}", worker.name)
            }
        }
    }

    /// Runs one named worker now, regardless of its triggers — the webhook and
    /// manual-run route executor. An unknown or paused worker is reported to the
    /// caller.
    pub async fn run_worker_now(
        &self,
        name: &str,
        event: TriggerEvent,
    ) -> Result<u64, HarnessError> {
        let worker = self
            .sys
            .get_worker(name)
            .await
            .map_err(|e| HarnessError::Job(e.to_string()))?
            .ok_or_else(|| HarnessError::Job(format!("no worker named {name}")))?;
        if worker.state != SystemState::Running {
            return Err(HarnessError::Job(format!(
                "worker {name} is {}, not running",
                worker.state.as_str()
            )));
        }
        let exec = WorkerExec::from_config(&worker.name, &worker.code)?;
        let output = worker.output.clone().unwrap_or_default();
        let run = verglas_harness::worker::WorkerRun {
            deployment: &worker.name,
            output: &output,
            endpoint: &self.endpoint,
            token: "",
        };
        let guarded = verglas_harness::run_guarded(&self.guard_dir, name, || {
            run_worker(&run, &exec, &event)
        })
        .await;
        match guarded {
            verglas_harness::Guarded::Ran(result) => result.map(|outcome| outcome.rows_produced),
            verglas_harness::Guarded::Skipped(reason) => Err(HarnessError::Job(format!(
                "worker {name} not run: {reason:?}"
            ))),
        }
    }

    /// Fans a completed commit on `table` out to every worker whose
    /// `data_change` trigger names it. Each match runs once with a
    /// [`TriggerEvent::DataChange`]; failures are logged, never surfaced to the
    /// committing caller. Called off the daemon's commit path.
    pub async fn on_table_commit(&self, table: &str, snapshot_id: Option<i64>) {
        let workers = match self.sys.list_active_workers().await {
            Ok(workers) => workers,
            Err(e) => {
                tracing::warn!("worker runtime: reading workers for data_change failed: {e}");
                return;
            }
        };
        for worker in workers {
            if worker.state != SystemState::Running {
                continue;
            }
            if !data_change_tables(&worker).iter().any(|t| t == table) {
                continue;
            }
            let event = TriggerEvent::DataChange {
                change: ChangeEvent {
                    seq: 0,
                    table: table.to_owned(),
                    snapshot_id: snapshot_id.map(|s| s.to_string()).unwrap_or_default(),
                    committed_at: Utc::now().to_rfc3339(),
                },
            };
            self.guarded_run(&worker, &worker.name, &event).await;
        }
    }
}

/// The tick period: one minute, aligned with cron's one-minute resolution. Each
/// tick reads the registry fresh so a pause or resume takes effect at the next
/// boundary.
const TICK_PERIOD: std::time::Duration = std::time::Duration::from_secs(60);

/// Spawns the worker runtime's background tick loop, detached for the process
/// lifetime. It starts after recovery has opened the loopback catalog and never
/// blocks startup.
pub fn spawn_supervisor(supervisor: Arc<WorkerSupervisor>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(TICK_PERIOD);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            supervisor.tick(&Utc::now()).await;
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn at(y: i32, mo: u32, d: u32, h: u32, mi: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, mo, d, h, mi, 0)
            .single()
            .expect("ts")
    }

    /// A fresh worker with no catchup fires no backlog: the first evaluation
    /// runs nothing and just sets the cursor to now.
    #[test]
    fn first_tick_without_catchup_runs_no_backlog() {
        let schedule = CronSchedule::parse("0 * * * *").expect("cron");
        let now = at(2026, 8, 1, 12, 0);
        let (intervals, cursor) = plan_cron_runs(None, now, None, Catchup::None, &schedule);
        assert!(intervals.is_empty(), "no backlog on first boot");
        assert_eq!(cursor, now);
    }

    /// With a cursor set, the due hourly instants in the window are the runs; each
    /// interval is half-open with logical_date the instant.
    #[test]
    fn due_instants_since_cursor_become_intervals() {
        let schedule = CronSchedule::parse("0 * * * *").expect("cron");
        let cursor = at(2026, 8, 1, 9, 30);
        let now = at(2026, 8, 1, 12, 15);
        let (intervals, _) =
            plan_cron_runs(Some(cursor), now, None, Catchup::Sequential, &schedule);
        let ends: Vec<_> = intervals
            .iter()
            .map(|i| i.interval_end.clone().expect("interval_end"))
            .collect();
        assert_eq!(
            ends,
            vec![
                at(2026, 8, 1, 10, 0).to_rfc3339(),
                at(2026, 8, 1, 11, 0).to_rfc3339(),
                at(2026, 8, 1, 12, 0).to_rfc3339(),
            ]
        );
        // First interval starts at the cursor; logical_date == interval_end.
        assert_eq!(
            intervals[0].interval_start.as_deref(),
            Some(cursor.to_rfc3339().as_str())
        );
        assert_eq!(intervals[0].logical_date, intervals[0].interval_end);
    }

    /// Catchup `none` after a gap collapses the backlog to the single latest
    /// instant.
    #[test]
    fn catchup_none_keeps_only_the_latest() {
        let schedule = CronSchedule::parse("0 * * * *").expect("cron");
        let cursor = at(2026, 8, 1, 9, 30);
        let now = at(2026, 8, 1, 12, 15);
        let (intervals, _) = plan_cron_runs(Some(cursor), now, None, Catchup::None, &schedule);
        assert_eq!(intervals.len(), 1, "backlog skipped");
        assert_eq!(
            intervals[0].interval_end.as_deref(),
            Some(at(2026, 8, 1, 12, 0).to_rfc3339().as_str())
        );
    }

    /// A worker's data_change tables are read off its triggers JSON.
    #[test]
    fn data_change_tables_parse() {
        let worker = WorkerRow {
            name: "w".to_owned(),
            code: "{}".to_owned(),
            triggers: r#"[{"type":"data_change","table":["a.b","c.d"]}]"#.to_owned(),
            output: None,
            config: "{}".to_owned(),
            state: SystemState::Running,
            placement: "local".to_owned(),
            created_by: "t".to_owned(),
            created_at: Utc::now(),
            revision: 1,
        };
        assert_eq!(data_change_tables(&worker), vec!["a.b", "c.d"]);
        assert!(cron_trigger(&worker).is_none());
    }
}
