//! The follow-worker manager: keeps a long-lived follow runner alive for every
//! active worker whose trigger is `follow`.
//!
//! Cron, event, and webhook workers run as one-shot subprocesses per fire (see
//! [`crate::platform`]). A `follow` worker is different: it runs continuously,
//! tailing a file or wrapping a command, and streams captured lines into its
//! target table as rows. This manager reconciles the set of running follow
//! runners against the registry on a short interval, so a newly declared follow
//! worker starts promptly and a paused/removed one is torn down.
//!
//! A follow runner writes through the server's own catalog into the configured
//! lakehouse.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use iceberg::Catalog;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use verglas_harness::follow::{FollowEnd, FollowSource, follow_table_ident, run_follow};
use verglas_harness::worker::WorkerExec;
use verglas_platform::{SystemCatalog, SystemState, WorkerRow};
use verglas_sdk::worker::TriggerSpec;

use crate::platform::parse_triggers;

/// How often the manager reconciles running runners against the registry. Short
/// so a throwaway follow worker starts and stops promptly.
const RECONCILE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(2);

/// A running follow runner and the handle to stop it.
struct RunnerHandle {
    /// Flipping this to `true` asks the runner to flush and stop.
    shutdown: watch::Sender<bool>,
    /// The runner task; its output says whether it completed or was stopped.
    join: JoinHandle<FollowEnd>,
    /// The worker revision this runner was started for, so a redeclare restarts.
    revision: i64,
    /// Whether a stop has already been requested (so it is not sent twice).
    stopping: bool,
}

/// What one active follow worker wants running.
struct Desired {
    revision: i64,
    ident: iceberg::TableIdent,
    source: FollowSource,
}

/// Reconciles follow runners against the worker registry.
pub struct FollowManager {
    /// The catalog runners write their rows through.
    catalog: Arc<dyn Catalog>,
    /// The registry the active follow workers are read from.
    sys: Arc<SystemCatalog>,
    /// The live runners, keyed by worker name.
    runners: Mutex<HashMap<String, RunnerHandle>>,
}

impl FollowManager {
    /// Builds a manager over the server's private catalog and system registry.
    pub fn new(catalog: Arc<dyn Catalog>, sys: Arc<SystemCatalog>) -> FollowManager {
        FollowManager {
            catalog,
            sys,
            runners: Mutex::new(HashMap::new()),
        }
    }

    /// Runs one reconcile pass: start runners for new follow workers, stop those
    /// for paused/removed/redeclared ones, and mark a self-finished worker
    /// completed so it is not restarted. A single bad worker never poisons the
    /// pass.
    pub async fn reconcile(&self) {
        let workers = match self.sys.list_active_workers().await {
            Ok(workers) => workers,
            Err(e) => {
                tracing::warn!("follow manager: reading the workers registry failed: {e}");
                return;
            }
        };
        let desired = self.desired_set(&workers);

        // Apply the start/stop/reap decisions under the lock (no await held), and
        // collect the self-finished workers to mark completed afterwards.
        let to_complete = {
            let mut runners = self.runners.lock().expect("follow runners lock");

            // Reap runners that ended on their own. A runner that finished without
            // a stop request completed its source (its command exited); a worker
            // still declared for it is marked completed below so it is not
            // restarted on the next pass.
            let finished: Vec<String> = runners
                .iter()
                .filter(|(_, h)| h.join.is_finished())
                .map(|(name, _)| name.clone())
                .collect();
            let mut to_complete = Vec::new();
            for name in finished {
                let handle = runners.remove(&name).expect("finished runner present");
                if !handle.stopping && desired.contains_key(&name) {
                    to_complete.push(name);
                }
            }

            // Stop runners no longer wanted, or whose worker was redeclared.
            for (name, handle) in runners.iter_mut() {
                let stop = match desired.get(name) {
                    None => true,
                    Some(d) => d.revision != handle.revision,
                };
                if stop && !handle.stopping {
                    let _ = handle.shutdown.send(true);
                    handle.stopping = true;
                }
            }

            // Start runners for wanted workers without a live one.
            for (name, d) in &desired {
                if runners.contains_key(name) {
                    continue;
                }
                let (tx, rx) = watch::channel(false);
                let join = tokio::spawn(run_follow(
                    self.catalog.clone(),
                    d.ident.clone(),
                    name.clone(),
                    d.source.clone(),
                    rx,
                ));
                runners.insert(
                    name.clone(),
                    RunnerHandle {
                        shutdown: tx,
                        join,
                        revision: d.revision,
                        stopping: false,
                    },
                );
                tracing::info!("follow worker {name}: started");
            }
            to_complete
        };

        for name in to_complete {
            if let Err(e) = self
                .sys
                .set_worker_state(&name, SystemState::Completed)
                .await
            {
                tracing::warn!("follow worker {name}: marking completed failed: {e}");
            } else {
                tracing::info!("follow worker {name}: source ended; marked completed");
            }
        }
    }

    /// The follow workers that should be running now: every `Running` worker with
    /// a follow trigger, a target table, and a runnable source.
    fn desired_set(&self, workers: &[WorkerRow]) -> HashMap<String, Desired> {
        let mut desired = HashMap::new();
        for worker in workers {
            if worker.state != SystemState::Running {
                continue;
            }
            let Some(file) = follow_target(worker) else {
                continue; // not a follow worker
            };
            let Some(output) = worker.output.as_deref().filter(|s| !s.is_empty()) else {
                tracing::warn!(
                    "follow worker {}: no target table; not starting",
                    worker.name
                );
                continue;
            };
            let ident = match follow_table_ident(output) {
                Ok(ident) => ident,
                Err(e) => {
                    tracing::warn!("follow worker {}: {e}", worker.name);
                    continue;
                }
            };
            let source = match file {
                Some(path) => FollowSource::File(PathBuf::from(path)),
                None => match WorkerExec::from_config(&worker.name, &worker.code) {
                    Ok(exec) => FollowSource::Command(exec),
                    Err(e) => {
                        tracing::warn!("follow worker {}: {e}", worker.name);
                        continue;
                    }
                },
            };
            desired.insert(
                worker.name.clone(),
                Desired {
                    revision: worker.revision,
                    ident,
                    source,
                },
            );
        }
        desired
    }
}

/// A follow worker's target: `Some(Some(path))` tails a file, `Some(None)` wraps
/// the worker's command, `None` means the worker has no follow trigger.
fn follow_target(worker: &WorkerRow) -> Option<Option<String>> {
    parse_triggers(worker)
        .ok()?
        .into_iter()
        .find_map(|t| match t {
            TriggerSpec::Follow { file } => Some(file),
            _ => None,
        })
}

/// Spawns the manager's reconcile loop, detached for the process lifetime. Like
/// the cron supervisor it starts after the catalog is open and never blocks
/// startup; its first pass runs immediately so a follow worker declared before
/// boot streams without waiting a full interval.
pub fn spawn_follow_manager(manager: Arc<FollowManager>) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(RECONCILE_INTERVAL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            manager.reconcile().await;
        }
    })
}
