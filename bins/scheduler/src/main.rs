//! Standalone on-prem worker scheduler for one Verglas-owned queue.
//!
//! Verglas pushes complete worker events to this service. The service persists
//! them through `verglas-rest`, immediately claims ready work, and executes the
//! local worker harness. It mounts no state and contains no connection-broker
//! behavior.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::{Component, Path};
use std::sync::Arc;
use std::time::Duration;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use clap::Parser;
use serde::{Deserialize, Serialize};
use verglas_harness::worker::{WorkerExec, WorkerRun, run_worker};
use verglas_scheduler::{
    ClaimRequest, ClaimedJob, CompleteRequest, Completion, EnqueueOutcome, Invocation, Lease,
    NextWakeRequest, PgQueue, RenewRequest, RunQueue, plan_cron,
};
use verglas_sdk::worker::{Catchup, CloudEvent, TriggerSpec};

/// CloudEvent type emitted for a planned cron interval.
const CRON_EVENT_TYPE: &str = "org.verglas.schedule.tick";

/// Standalone scheduler process configuration.
#[derive(Debug, Parser)]
#[command(name = "verglas-scheduler", version)]
struct Args {
    /// Postgres database that owns all durable scheduler state.
    #[arg(long, env = "VERGLAS_SCHEDULER_DATABASE_URL")]
    database_url: String,
    /// Verglas REST API that owns worker declarations.
    #[arg(long, env = "VERGLAS_SCHEDULER_VERGLAS_URL")]
    verglas_url: String,
    /// Service credential accepted by the tenant-local Verglas REST API.
    #[arg(long, env = "VERGLAS_SCHEDULER_CONTROL_TOKEN", hide_env_values = true)]
    verglas_token: String,
    /// Data-plane endpoint injected into worker subprocesses.
    #[arg(long, env = "VERGLAS_WORKER_ENDPOINT")]
    worker_endpoint: String,
    /// Queue identity served by the configured Verglas instance.
    #[arg(long, env = "VERGLAS_SCHEDULER_QUEUE")]
    queue: String,
    /// Stable consumer identity used in fenced lease objects.
    #[arg(long, env = "VERGLAS_SCHEDULER_CONSUMER", default_value = "scheduler")]
    consumer: String,
    /// Worker lease duration in seconds.
    #[arg(long, env = "VERGLAS_SCHEDULER_LEASE_SECS", default_value_t = 300)]
    lease_seconds: u64,
    /// Address receiving pushed worker events from Verglas.
    #[arg(long, env = "VERGLAS_SCHEDULER_LISTEN", default_value = "0.0.0.0:8340")]
    listen: SocketAddr,
}

/// The worker fields returned by Verglas's registry endpoint.
#[derive(Debug, Deserialize)]
struct WorkerRecord {
    /// Worker deployment name.
    name: String,
    /// Subprocess execution JSON.
    code: String,
    /// Deployment-configured output table.
    output: Option<String>,
    /// Trigger declarations stored in the deployment record.
    triggers: String,
    /// Lifecycle state; only `running` declarations are reconciled.
    state: String,
    /// Portable environment and bundled text files.
    config: String,
}

/// The portable portion of a worker registry config.
#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkerConfig {
    /// Plain subprocess environment bindings.
    #[serde(default)]
    env: BTreeMap<String, String>,
    /// Text files materialized into one isolated run directory.
    #[serde(default)]
    files: BTreeMap<String, String>,
}

/// One prepared subprocess and the temporary bundle that keeps its files live.
struct PreparedWorker {
    /// Executable subprocess contract.
    exec: WorkerExec,
    /// Isolated directory removed after the run finishes.
    _root: tempfile::TempDir,
}

/// Validates that a bundled file path stays below the isolated run directory.
fn safe_bundle_path(path: &Path) -> Result<&Path, String> {
    if path.as_os_str().is_empty()
        || path.is_absolute()
        || path
            .components()
            .any(|part| !matches!(part, Component::Normal(_)))
    {
        return Err(format!(
            "worker bundle path `{}` is not a relative file path",
            path.display()
        ));
    }
    Ok(path)
}

/// Materializes one portable worker config into an isolated run directory.
fn prepare_worker(worker: &WorkerRecord) -> Result<PreparedWorker, String> {
    let config: WorkerConfig = serde_json::from_str(&worker.config)
        .map_err(|error| format!("worker {} config: {error}", worker.name))?;
    if let Some((name, _)) = config
        .env
        .iter()
        .find(|(_, value)| value.starts_with("@secret:"))
    {
        return Err(format!(
            "worker {} secret binding {name} is unresolved",
            worker.name
        ));
    }
    let root = tempfile::tempdir()
        .map_err(|error| format!("worker {} bundle directory: {error}", worker.name))?;
    for (name, contents) in &config.files {
        let relative = safe_bundle_path(Path::new(name))?;
        let target = root.path().join(relative);
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|error| format!("worker {} bundle directory: {error}", worker.name))?;
        }
        std::fs::write(&target, contents)
            .map_err(|error| format!("worker {} bundle file {name}: {error}", worker.name))?;
    }
    let mut exec =
        WorkerExec::from_config(&worker.name, &worker.code).map_err(|error| error.to_string())?;
    if let Some(cwd) = exec.cwd.as_deref() {
        let cwd = Path::new(cwd);
        if !cwd.is_absolute() {
            let relative = if cwd == Path::new(".") {
                Path::new("")
            } else {
                safe_bundle_path(cwd)?
            };
            exec.cwd = Some(root.path().join(relative).to_string_lossy().into_owned());
        } else if !config.files.is_empty() {
            return Err(format!(
                "worker {} has bundled files and an absolute cwd",
                worker.name
            ));
        }
    } else if !config.files.is_empty() {
        exec.cwd = Some(root.path().to_string_lossy().into_owned());
    }
    exec.env = config.env;
    Ok(PreparedWorker { exec, _root: root })
}

/// Queue enqueue response returned by `verglas-rest` and this service.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct EnqueueResponse {
    /// Deterministic run identity.
    job_id: String,
    /// Whether this call created the run rather than joining it.
    created: bool,
}

/// Thin client for worker-registry operations hosted by Verglas.
#[derive(Clone)]
struct VerglasClient {
    http: reqwest::Client,
    base: String,
    token: Arc<str>,
}

impl VerglasClient {
    /// Builds a client after normalizing the base URI once.
    fn new(base: &str, token: impl Into<Arc<str>>) -> VerglasClient {
        VerglasClient {
            http: reqwest::Client::new(),
            base: base.trim_end_matches('/').to_owned(),
            token: token.into(),
        }
    }

    /// Reads the current worker declaration from the Verglas registry.
    async fn worker(&self, name: &str) -> Result<WorkerRecord, String> {
        self.http
            .get(format!("{}/v1/workers/{name}", self.base))
            .bearer_auth(self.token.as_ref())
            .send()
            .await
            .map_err(|error| format!("worker request: {error}"))?
            .error_for_status()
            .map_err(|error| format!("worker response: {error}"))?
            .json()
            .await
            .map_err(|error| format!("worker JSON: {error}"))
    }

    /// Lists the current worker registry projection owned by Verglas.
    async fn workers(&self) -> Result<Vec<WorkerRecord>, String> {
        self.http
            .get(format!("{}/v1/workers?view=active", self.base))
            .bearer_auth(self.token.as_ref())
            .send()
            .await
            .map_err(|error| format!("workers request: {error}"))?
            .error_for_status()
            .map_err(|error| format!("workers response: {error}"))?
            .json()
            .await
            .map_err(|error| format!("workers JSON: {error}"))
    }
}

/// State shared by pushed-event handlers and the execution loop.
#[derive(Clone)]
struct EventIngress {
    queue: Arc<dyn RunQueue>,
    ready: Arc<tokio::sync::Notify>,
}

/// Accepts one complete worker event, persists it, and wakes execution now.
async fn submit_event(
    State(ingress): State<EventIngress>,
    Json(invocation): Json<Invocation>,
) -> Response {
    match ingress.queue.enqueue(&invocation).await {
        Ok(outcome) => {
            ingress.ready.notify_one();
            let (status, response) = match outcome {
                EnqueueOutcome::Created(job_id) => (
                    StatusCode::ACCEPTED,
                    EnqueueResponse {
                        job_id,
                        created: true,
                    },
                ),
                EnqueueOutcome::Existing(job_id) => (
                    StatusCode::OK,
                    EnqueueResponse {
                        job_id,
                        created: false,
                    },
                ),
            };
            (status, Json(response)).into_response()
        }
        Err(error) => (StatusCode::SERVICE_UNAVAILABLE, error.to_string()).into_response(),
    }
}

/// Reports that the scheduler event receiver is alive.
async fn healthz() -> &'static str {
    "ok"
}

/// Builds the scheduler's pushed-event API.
fn event_router(ingress: EventIngress) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/v1/events", post(submit_event))
        .with_state(ingress)
}

/// Reconstructs cron progress from run objects and materializes every due run.
async fn reconcile_cron(
    client: &VerglasClient,
    queue: &dyn RunQueue,
    now: DateTime<Utc>,
) -> Result<Option<DateTime<Utc>>, String> {
    let workers = client.workers().await?;
    let jobs = queue.jobs().await.map_err(|error| error.to_string())?;
    let mut next_wake_at = None;
    for worker in workers
        .into_iter()
        .filter(|worker| worker.state == "running")
    {
        let triggers: Vec<TriggerSpec> = serde_json::from_str(&worker.triggers)
            .map_err(|error| format!("worker {} triggers: {error}", worker.name))?;
        for (index, trigger) in triggers.into_iter().enumerate() {
            let TriggerSpec::Cron {
                schedule,
                start_date,
                catchup,
            } = trigger
            else {
                continue;
            };
            let trigger_id = format!("cron-{index}");
            let cron_source = format!("urn:verglas:scheduler:{}:{trigger_id}", worker.name);
            let cursor = jobs
                .iter()
                .filter(|job| {
                    job.worker == worker.name
                        && job.event.source == cron_source
                        && job.event.event_type == CRON_EVENT_TYPE
                })
                .filter_map(|job| job.event.data.as_ref())
                .filter_map(|data| data.get("logicalDate"))
                .filter_map(serde_json::Value::as_str)
                .filter_map(|value| DateTime::parse_from_rfc3339(value).ok())
                .map(|value| value.with_timezone(&Utc))
                .max();
            let start_date = start_date
                .as_deref()
                .map(DateTime::parse_from_rfc3339)
                .transpose()
                .map_err(|error| format!("worker {} startDate: {error}", worker.name))?
                .map(|value| value.with_timezone(&Utc));
            let plan = plan_cron(
                cursor,
                now,
                start_date,
                catchup.unwrap_or(Catchup::None),
                &schedule,
            )
            .map_err(|error| format!("worker {}: {error}", worker.name))?;
            next_wake_at = Some(next_wake_at.map_or(plan.next_wake_at, |current| {
                std::cmp::min(current, plan.next_wake_at)
            }));
            for interval in plan.intervals {
                let logical_time = DateTime::parse_from_rfc3339(&interval.logical_date)
                    .map(|time| time.with_timezone(&Utc))
                    .map_err(|error| format!("worker {} logical date: {error}", worker.name))?;
                let mut event = CloudEvent::new(
                    format!("{trigger_id}:{}", interval.logical_date),
                    cron_source.clone(),
                    CRON_EVENT_TYPE,
                );
                event.time = Some(interval.logical_date.clone());
                event.datacontenttype = Some("application/json".to_owned());
                event.data = Some(
                    serde_json::to_value(&interval)
                        .map_err(|error| format!("serialize cron interval: {error}"))?,
                );
                queue
                    .enqueue(&Invocation::new(&worker.name, event, logical_time))
                    .await
                    .map_err(|error| error.to_string())?;
            }
        }
    }
    Ok(next_wake_at)
}

/// Executes one claimed run while renewing its lease until the worker returns.
async fn execute_claimed(
    args: &Args,
    client: &VerglasClient,
    queue: &dyn RunQueue,
    claimed: ClaimedJob,
) -> Result<(Lease, Completion), String> {
    let worker = client.worker(&claimed.job.worker).await?;
    let prepared = prepare_worker(&worker)?;
    let output = worker.output.unwrap_or_default();
    let run = WorkerRun {
        deployment: &worker.name,
        output: &output,
        endpoint: &args.worker_endpoint,
        token: "",
    };
    let mut lease = claimed.lease;
    let renew_every = Duration::from_secs((args.lease_seconds / 2).max(1));
    let mut renewal_error = None;
    let mut execution = Box::pin(run_worker(&run, &prepared.exec, &claimed.job.event));
    let outcome = loop {
        tokio::select! {
            result = &mut execution => break result,
            () = tokio::time::sleep(renew_every), if renewal_error.is_none() => {
                match queue.renew(&RenewRequest {
                    lease: lease.clone(),
                    now: Utc::now(),
                    lease_seconds: args.lease_seconds,
                }).await {
                    Ok(renewed) => lease = renewed,
                    Err(error) => renewal_error = Some(error.to_string()),
                }
            }
        }
    };
    if let Some(error) = renewal_error {
        return Err(error);
    }
    let completion = match outcome {
        Ok(result) => Completion::Succeeded {
            rows_produced: result.rows_produced,
        },
        Err(error) => Completion::Failed {
            message: error.to_string(),
            retry_at: Some(Utc::now() + chrono::Duration::seconds(30)),
        },
    };
    Ok((lease, completion))
}

/// Claims, executes, and completes one ready run.
async fn run_one(
    args: &Args,
    client: &VerglasClient,
    queue: &dyn RunQueue,
) -> Result<bool, String> {
    let claim = ClaimRequest {
        owner: format!("{}:{}", args.queue, args.consumer),
        now: Utc::now(),
        lease_seconds: args.lease_seconds,
    };
    let Some(claimed) = queue
        .claim(&claim)
        .await
        .map_err(|error| error.to_string())?
    else {
        return Ok(false);
    };
    let (lease, completion) = match execute_claimed(args, client, queue, claimed).await {
        Ok(result) => result,
        Err(error) => return Err(format!("execute claimed run: {error}")),
    };
    queue
        .complete(&CompleteRequest {
            lease,
            completion,
            now: Utc::now(),
        })
        .await
        .map_err(|error| error.to_string())?;
    Ok(true)
}

/// Returns a non-negative sleep duration until an absolute queue deadline.
fn until(deadline: DateTime<Utc>) -> Duration {
    match deadline.signed_duration_since(Utc::now()).to_std() {
        Ok(duration) => duration,
        Err(_) => Duration::ZERO,
    }
}

/// Reconciles durable state, drains ready work, and waits for an exact deadline
/// or a pushed-event notification.
async fn scheduler_loop(
    args: &Args,
    client: &VerglasClient,
    queue: &dyn RunQueue,
    ready: &tokio::sync::Notify,
) -> Result<(), String> {
    loop {
        let iteration: Result<(), String> = async {
            let now = Utc::now();
            let cron_deadline = reconcile_cron(client, queue, now).await?;
            while run_one(args, client, queue).await? {}
            let queue_deadline = queue
                .next_wake_at(&NextWakeRequest { now: Utc::now() })
                .await
                .map_err(|error| error.to_string())?;
            let deadline = match (cron_deadline, queue_deadline) {
                (Some(cron), Some(queue)) => Some(std::cmp::min(cron, queue)),
                (Some(deadline), None) | (None, Some(deadline)) => Some(deadline),
                (None, None) => None,
            };
            match deadline {
                Some(deadline) => {
                    tokio::select! {
                        () = tokio::time::sleep(until(deadline)) => {}
                        () = ready.notified() => {}
                    }
                }
                None => ready.notified().await,
            }
            Ok(())
        }
        .await;
        if let Err(error) = iteration {
            eprintln!(
                "verglas-scheduler: reconciliation unavailable: {error}; retrying in 5 seconds"
            );
            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(5)) => {}
                () = ready.notified() => {}
            }
        }
    }
}

/// Runs the pushed-event service and exact-timer execution loop.
#[tokio::main]
async fn main() {
    let args = Args::parse();
    let queue = match PgQueue::connect(&args.database_url, &args.queue).await {
        Ok(queue) => Arc::new(queue),
        Err(error) => {
            eprintln!("verglas-scheduler: connect Postgres: {error}");
            std::process::exit(1);
        }
    };
    let client = VerglasClient::new(&args.verglas_url, args.verglas_token.clone());
    let ready = Arc::new(tokio::sync::Notify::new());
    let app = event_router(EventIngress {
        queue: queue.clone(),
        ready: ready.clone(),
    });
    let listener = match tokio::net::TcpListener::bind(args.listen).await {
        Ok(listener) => listener,
        Err(error) => {
            eprintln!("verglas-scheduler: bind {}: {error}", args.listen);
            std::process::exit(1);
        }
    };
    eprintln!(
        "verglas-scheduler {} queue={} verglas={} listen={}",
        env!("CARGO_PKG_VERSION"),
        args.queue,
        args.verglas_url,
        args.listen,
    );
    let server = axum::serve(listener, app);
    tokio::select! {
        result = server => {
            eprintln!("verglas-scheduler: event API: {}", result
                .err()
                .map_or_else(|| "stopped".to_owned(), |error| error.to_string()));
        }
        result = scheduler_loop(&args, &client, queue.as_ref(), &ready) => {
            eprintln!("verglas-scheduler: {}", result
                .err()
                .unwrap_or_else(|| "scheduler loop stopped".to_owned()));
        }
    }
    std::process::exit(1);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use verglas_scheduler::{Attempt, Job, LeaseError, SchedulerError};

    /// Minimal durable queue double used to prove ingress persistence ordering.
    #[derive(Default)]
    struct TestQueue {
        invocation: Mutex<Option<Invocation>>,
    }

    #[async_trait::async_trait]
    impl RunQueue for TestQueue {
        /// Stores the invocation before returning the acknowledgement.
        async fn enqueue(&self, invocation: &Invocation) -> Result<EnqueueOutcome, SchedulerError> {
            self.invocation
                .lock()
                .expect("queue lock")
                .replace(invocation.clone());
            Ok(EnqueueOutcome::Created("job-1".to_owned()))
        }

        /// No cron history is needed by these focused tests.
        async fn jobs(&self) -> Result<Vec<Job>, SchedulerError> {
            Ok(Vec::new())
        }

        /// No work is claimed by these focused tests.
        async fn claim(
            &self,
            _request: &ClaimRequest,
        ) -> Result<Option<ClaimedJob>, SchedulerError> {
            Ok(None)
        }

        /// Returns the supplied lease for fast executions that never need renewal.
        async fn renew(&self, request: &RenewRequest) -> Result<Lease, LeaseError> {
            Ok(request.lease.clone())
        }

        /// Accepts a completion produced by the harness test.
        async fn complete(&self, _request: &CompleteRequest) -> Result<(), LeaseError> {
            Ok(())
        }

        /// No attempt history is needed by these focused tests.
        async fn attempts(&self, _job_id: &str) -> Result<Vec<Attempt>, SchedulerError> {
            Ok(Vec::new())
        }

        /// No durable timer is needed by these focused tests.
        async fn next_wake_at(
            &self,
            _request: &NextWakeRequest,
        ) -> Result<Option<DateTime<Utc>>, SchedulerError> {
            Ok(None)
        }
    }

    /// Starts an in-process Verglas API and returns its base URL.
    async fn serve(app: Router) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock API");
        let address = listener.local_addr().expect("mock API address");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("mock API");
        });
        format!("http://{address}")
    }

    /// Bundled files and plain environment bindings are materialized for one
    /// run, and a relative cwd resolves inside that isolated bundle.
    #[test]
    fn prepares_a_portable_worker_bundle() {
        let worker = WorkerRecord {
            name: "market-data-ingest".to_owned(),
            code: r#"{"exec":["python3","ingest.py"],"cwd":"."}"#.to_owned(),
            output: Some("market.ohlcv".to_owned()),
            triggers: "[]".to_owned(),
            state: "running".to_owned(),
            config: r#"{
                "env":{"SYMBOL":"SPY"},
                "files":{"ingest.py":"print('ready')\n"}
            }"#
            .to_owned(),
        };

        let prepared = prepare_worker(&worker).expect("prepare bundle");
        assert_eq!(
            prepared.exec.env.get("SYMBOL").map(String::as_str),
            Some("SPY")
        );
        assert_eq!(
            std::fs::read_to_string(prepared._root.path().join("ingest.py")).expect("bundled file"),
            "print('ready')\n"
        );
        assert_eq!(
            prepared.exec.cwd.as_deref(),
            Some(prepared._root.path().join("").to_string_lossy().as_ref())
        );
    }

    /// The pushed-event API acknowledges only after the queue persists the event.
    #[tokio::test]
    async fn pushed_event_is_persisted_before_acceptance() {
        let queue = Arc::new(TestQueue::default());
        let ingress = EventIngress {
            queue: queue.clone(),
            ready: Arc::new(tokio::sync::Notify::new()),
        };
        let invocation = Invocation::new(
            "http-worker",
            CloudEvent::new("request-1", "urn:verglas:http", "org.verglas.http.request"),
            Utc::now(),
        );

        let response = submit_event(State(ingress), Json(invocation)).await;
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let stored = queue
            .invocation
            .lock()
            .expect("queue lock")
            .clone()
            .expect("stored event");
        assert_eq!(stored.worker, "http-worker");
        assert_eq!(stored.event.event_type, "org.verglas.http.request");
    }

    /// A claimed worker executes through the harness and becomes a completion.
    #[tokio::test]
    async fn claimed_worker_executes_as_one_bounded_run() {
        let code = serde_json::json!({
            "exec": ["sh", "-c", "printf '{\"rows\":3,\"error\":null}' > \"$RESULT_PATH\""]
        })
        .to_string();
        let api = Router::new().route(
            "/v1/workers/{name}",
            get(move |headers: axum::http::HeaderMap| {
                assert_eq!(
                    headers
                        .get(axum::http::header::AUTHORIZATION)
                        .and_then(|value| value.to_str().ok()),
                    Some("Bearer scheduler-test-token")
                );
                let code = code.clone();
                async move {
                    Json(serde_json::json!({
                        "name": "worker-a",
                        "code": code,
                        "output": "app.output",
                        "triggers": "[]",
                        "state": "running",
                        "config": "{}"
                    }))
                }
            }),
        );
        let client = VerglasClient::new(&serve(api).await, "scheduler-test-token");
        let now = Utc::now();
        let claimed = ClaimedJob {
            job: Job {
                id: "job-1".to_owned(),
                queue: "local".to_owned(),
                worker: "worker-a".to_owned(),
                event: CloudEvent::new(
                    "request-1",
                    "urn:verglas:rest",
                    "org.verglas.worker.manual",
                ),
                ready_at: now,
            },
            lease: Lease {
                job_id: "job-1".to_owned(),
                owner: "consumer-1".to_owned(),
                generation: 1,
                expires_at: now + chrono::Duration::minutes(5),
            },
        };
        let args = Args {
            database_url: "postgres://unused".to_owned(),
            verglas_url: "unused".to_owned(),
            verglas_token: "scheduler-test-token".to_owned(),
            worker_endpoint: "http://127.0.0.1:8334".to_owned(),
            queue: "local".to_owned(),
            consumer: "consumer-1".to_owned(),
            lease_seconds: 300,
            listen: "127.0.0.1:0".parse().expect("listen"),
        };

        let queue = TestQueue::default();
        let (_, completion) = execute_claimed(&args, &client, &queue, claimed)
            .await
            .expect("execute");
        assert_eq!(completion, Completion::Succeeded { rows_produced: 3 });
    }

    /// A dynamic deployment may start the scheduler before any catalog owns a
    /// worker registry. Reconciliation failure must not discard the durable
    /// process or turn Compose into a restart loop.
    #[tokio::test]
    async fn unavailable_worker_registry_keeps_scheduler_alive_for_retry() {
        let client = VerglasClient::new(&serve(Router::new()).await, "scheduler-test-token");
        let args = Args {
            database_url: "postgres://unused".to_owned(),
            verglas_url: "unused".to_owned(),
            verglas_token: "scheduler-test-token".to_owned(),
            worker_endpoint: "http://127.0.0.1:8334".to_owned(),
            queue: "local".to_owned(),
            consumer: "consumer-1".to_owned(),
            lease_seconds: 300,
            listen: "127.0.0.1:0".parse().expect("listen"),
        };
        let queue = TestQueue::default();
        let ready = tokio::sync::Notify::new();

        assert!(
            tokio::time::timeout(
                Duration::from_millis(100),
                scheduler_loop(&args, &client, &queue, &ready),
            )
            .await
            .is_err(),
            "the scheduler must remain alive while the dynamic registry is unavailable"
        );
    }
}
