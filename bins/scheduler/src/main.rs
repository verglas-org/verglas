//! Standalone on-prem worker scheduler for one Verglas-owned queue.
//!
//! Verglas pushes complete worker events to this service. The service persists
//! declarations, jobs, and leases in its Postgres control database, immediately
//! claims ready work, and delegates tenant code to the container runtime. It
//! mounts no state and contains no connection-broker behavior.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Path as AxumPath, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router, extract::Request, middleware};
use chrono::{DateTime, Utc};
use clap::Parser;
use serde::{Deserialize, Serialize};
use verglas_container_runtime::{
    WorkerInvocation, WorkerProjectSpec, WorkerResources, WorkerRunResult,
};
use verglas_scheduler::{
    ClaimRequest, ClaimedJob, CompleteRequest, Completion, EnqueueOutcome, Invocation, Lease,
    NextWakeRequest, PgQueue, PgWorkerRegistry, RenewRequest, RunQueue, WorkerBuildStatus,
    WorkerRecord, WorkerSpec, plan_cron,
};
use verglas_sdk::worker::{Catchup, CloudEvent, TriggerSpec};

/// CloudEvent type emitted for a planned cron interval.
const CRON_EVENT_TYPE: &str = "org.verglas.schedule.tick";

/// Maximum runtime error response retained in durable scheduler state.
const RUNTIME_ERROR_BODY_LIMIT: usize = 4_096;

/// Standalone scheduler process configuration.
#[derive(Debug, Parser)]
#[command(name = "verglas-scheduler", version)]
struct Args {
    /// Postgres database that owns all durable scheduler state.
    #[arg(long, env = "VERGLAS_SCHEDULER_DATABASE_URL")]
    database_url: String,
    /// Data-plane endpoint injected into worker containers.
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
    /// Bearer token required by every scheduler control route.
    #[arg(long, env = "VERGLAS_SCHEDULER_CONTROL_TOKEN")]
    control_token: String,
    /// Hex-encoded 256-bit key used to encrypt scheduler runtime secrets.
    #[arg(long, env = "VERGLAS_SECRET_ENCRYPTION_KEY", hide_env_values = true)]
    secret_encryption_key: String,
    /// Authenticated Verglas container runtime endpoint.
    #[arg(long, env = "VERGLAS_CONTAINER_RUNTIME_URL")]
    container_runtime_url: String,
    /// Bearer token accepted by the Verglas container runtime.
    #[arg(long, env = "VERGLAS_CONTAINER_RUNTIME_TOKEN", hide_env_values = true)]
    container_runtime_token: String,
}

/// The portable portion of a worker registry config.
#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkerConfig {
    /// Plain container environment bindings.
    #[serde(default)]
    env: BTreeMap<String, String>,
    /// Hard invocation limits.
    #[serde(default)]
    resources: WorkerResourceConfig,
    /// Absolute container path backed by the runtime's operator-owned scratch root.
    scratch_target: Option<String>,
}

/// Optional portable limits with operator-safe defaults.
#[derive(Default, Deserialize)]
struct WorkerResourceConfig {
    vcpus: Option<f64>,
    mem_mib: Option<u64>,
    pids: Option<i64>,
    timeout_secs: Option<u64>,
}

/// Built worker identity stored with its immutable source declaration.
#[derive(Deserialize)]
struct BuiltWorkerCode {
    image: String,
    entrypoint: Vec<String>,
}

/// Prepared image and runtime-only configuration for one invocation.
struct PreparedWorker {
    image: String,
    entrypoint: Vec<String>,
    environment: BTreeMap<String, String>,
    resources: WorkerResources,
    timeout_seconds: u64,
    scratch_target: Option<String>,
}

async fn prepare_registered_worker(
    registry: &PgWorkerRegistry,
    worker: &WorkerRecord,
) -> Result<PreparedWorker, String> {
    let mut config: WorkerConfig = serde_json::from_str(&worker.config)
        .map_err(|error| format!("worker {} config: {error}", worker.name))?;
    for (binding, value) in &mut config.env {
        let Some(secret_name) = value.strip_prefix("@secret:") else {
            continue;
        };
        *value = registry
            .secret(secret_name)
            .await
            .map_err(|error| format!("worker {} secret binding {binding}: {error}", worker.name))?
            .ok_or_else(|| {
                format!(
                    "worker {} secret binding {binding} references missing secret {secret_name}",
                    worker.name
                )
            })?;
    }
    prepare_worker_config(worker, config)
}

/// Converts persisted built code and resolved runtime config into one invocation.
fn prepare_worker_config(
    worker: &WorkerRecord,
    config: WorkerConfig,
) -> Result<PreparedWorker, String> {
    if worker.build_status != WorkerBuildStatus::Ready {
        return Err(format!(
            "worker {} build is {:?}",
            worker.name, worker.build_status
        ));
    }
    let code: BuiltWorkerCode = serde_json::from_str(&worker.code)
        .map_err(|error| format!("worker {} built code: {error}", worker.name))?;
    let digest = worker
        .image_digest
        .as_deref()
        .ok_or_else(|| format!("worker {} ready build has no image digest", worker.name))?;
    let digest_tag = digest.replacen(':', "-", 1);
    if !code.image.ends_with(&digest_tag) {
        return Err(format!(
            "worker {} image does not match its registered digest",
            worker.name
        ));
    }
    Ok(PreparedWorker {
        image: code.image,
        entrypoint: code.entrypoint,
        environment: config.env,
        resources: WorkerResources {
            vcpus: config.resources.vcpus.unwrap_or(4.0),
            memory_mib: config.resources.mem_mib.unwrap_or(8_192),
            pids: config.resources.pids.unwrap_or(512),
        },
        timeout_seconds: config.resources.timeout_secs.unwrap_or(3_600),
        scratch_target: config.scratch_target,
    })
}

/// Queue enqueue response returned by `verglas-rest` and this service.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct EnqueueResponse {
    /// Deterministic run identity.
    job_id: String,
    /// Whether this call created the run rather than joining it.
    created: bool,
}

/// State shared by pushed-event handlers and the execution loop.
#[derive(Clone)]
struct SchedulerState {
    queue: Arc<PgQueue>,
    registry: Arc<PgWorkerRegistry>,
    ready: Arc<tokio::sync::Notify>,
    runtime: RuntimeClient,
}

/// Authenticated client for immutable worker builds and bounded runs.
#[derive(Clone)]
struct RuntimeClient {
    endpoint: Arc<str>,
    token: Arc<str>,
    http: reqwest::Client,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WorkerProjectView {
    image: String,
    image_digest: String,
}

impl RuntimeClient {
    /// Creates one client without exposing its bearer through request payloads.
    fn new(endpoint: impl Into<String>, token: impl Into<String>) -> Self {
        Self {
            endpoint: Arc::from(endpoint.into().trim_end_matches('/').to_owned()),
            token: Arc::from(token.into()),
            http: reqwest::Client::new(),
        }
    }

    /// Builds one locked project and returns its immutable image and digest.
    async fn build(&self, project: &WorkerProjectSpec) -> Result<WorkerProjectView, String> {
        let response = self
            .http
            .put(format!(
                "{}/v1/worker-projects/{}",
                self.endpoint, project.name
            ))
            .bearer_auth(self.token.as_ref())
            .json(project)
            .send()
            .await
            .map_err(|error| format!("worker build request: {error}"))?;
        let response = runtime_response(response, "worker build").await?;
        response
            .json::<WorkerProjectView>()
            .await
            .map_err(|error| format!("worker build response: {error}"))
    }

    /// Executes one built image through the bounded runtime API.
    async fn run(&self, invocation: &WorkerInvocation) -> Result<WorkerRunResult, String> {
        let response = self
            .http
            .put(format!(
                "{}/v1/worker-runs/{}",
                self.endpoint, invocation.run_id
            ))
            .bearer_auth(self.token.as_ref())
            .timeout(Duration::from_secs(
                invocation.timeout_seconds.saturating_add(30),
            ))
            .json(invocation)
            .send()
            .await
            .map_err(|error| format!("worker run request: {error}"))?;
        runtime_response(response, "worker run")
            .await?
            .json::<WorkerRunResult>()
            .await
            .map_err(|error| format!("worker run response: {error}"))
    }
}

/// Preserves a bounded runtime rejection body so operators can diagnose runs.
async fn runtime_response(
    response: reqwest::Response,
    operation: &str,
) -> Result<reqwest::Response, String> {
    let status = response.status();
    if status.is_success() {
        return Ok(response);
    }
    let body = response
        .text()
        .await
        .unwrap_or_else(|error| format!("failed to read response body: {error}"));
    let body: String = body.chars().take(RUNTIME_ERROR_BODY_LIMIT).collect();
    Err(format!("{operation} rejected with HTTP {status}: {body}"))
}

/// Accepts one complete worker event, persists it, and wakes execution now.
async fn submit_event(
    State(state): State<SchedulerState>,
    Json(invocation): Json<Invocation>,
) -> Response {
    persist_event(state.queue.as_ref(), state.ready.as_ref(), &invocation).await
}

async fn persist_event(
    queue: &dyn RunQueue,
    ready: &tokio::sync::Notify,
    invocation: &Invocation,
) -> Response {
    match queue.enqueue(invocation).await {
        Ok(outcome) => {
            ready.notify_one();
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

#[derive(Debug, Deserialize)]
struct ListView {
    view: Option<String>,
}

#[derive(Debug, Deserialize)]
struct StatePut {
    state: String,
}

#[derive(Debug, Deserialize)]
struct SecretPut {
    value: String,
}

#[derive(Debug, Deserialize)]
struct JobLimit {
    limit: Option<u32>,
}

#[derive(Clone)]
struct ControlAuth(Arc<str>);

async fn authorize_control(
    State(auth): State<ControlAuth>,
    request: Request,
    next: Next,
) -> Response {
    let authorized = request
        .headers()
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value == auth.0.as_ref());
    if !authorized {
        return (
            StatusCode::UNAUTHORIZED,
            "scheduler control token is required",
        )
            .into_response();
    }
    next.run(request).await
}

async fn list_workers(
    State(state): State<SchedulerState>,
    Query(query): Query<ListView>,
) -> Response {
    let include_all = match query.view.as_deref() {
        None | Some("active") => false,
        Some("all") => true,
        Some(other) => {
            return (
                StatusCode::BAD_REQUEST,
                format!("unknown view `{other}`: expected active or all"),
            )
                .into_response();
        }
    };
    match state.registry.list(include_all).await {
        Ok(workers) => Json(workers).into_response(),
        Err(error) => scheduler_error(error),
    }
}

async fn register_worker(
    State(state): State<SchedulerState>,
    Json(spec): Json<WorkerSpec>,
) -> Response {
    let project: WorkerProjectSpec = match serde_json::from_str(&spec.code) {
        Ok(project) => project,
        Err(error) => {
            return (StatusCode::BAD_REQUEST, format!("worker project: {error}")).into_response();
        }
    };
    if project.name != spec.name {
        return (
            StatusCode::BAD_REQUEST,
            "worker project name does not match registry name",
        )
            .into_response();
    }
    let building = match state.registry.begin_build(spec).await {
        Ok(worker) => worker,
        Err(error) => return scheduler_error(error),
    };
    let build = match state.runtime.build(&project).await {
        Ok(build) => build,
        Err(error) => {
            return match state
                .registry
                .finish_build(
                    &building.name,
                    building.revision,
                    WorkerBuildStatus::Failed,
                    None,
                    None,
                )
                .await
            {
                Ok(_) => (StatusCode::BAD_GATEWAY, error).into_response(),
                Err(registry_error) => (
                    StatusCode::BAD_GATEWAY,
                    format!("{error}; recording failed build: {registry_error}"),
                )
                    .into_response(),
            };
        }
    };
    let mut built = match serde_json::to_value(&project) {
        Ok(serde_json::Value::Object(value)) => value,
        Ok(_) => unreachable!("worker project serializes as an object"),
        Err(error) => return (StatusCode::BAD_REQUEST, error.to_string()).into_response(),
    };
    built.insert("image".to_owned(), serde_json::Value::String(build.image));
    let built_code = serde_json::Value::Object(built).to_string();
    match state
        .registry
        .finish_build(
            &building.name,
            building.revision,
            WorkerBuildStatus::Ready,
            Some(&build.image_digest),
            Some(&built_code),
        )
        .await
    {
        Ok(worker) => {
            state.ready.notify_one();
            (StatusCode::CREATED, Json(worker)).into_response()
        }
        Err(error) => scheduler_error(error),
    }
}

async fn get_worker(
    State(state): State<SchedulerState>,
    AxumPath(name): AxumPath<String>,
) -> Response {
    match state.registry.get(&name).await {
        Ok(Some(worker)) => Json(worker).into_response(),
        Ok(None) => (StatusCode::NOT_FOUND, format!("no worker named {name}")).into_response(),
        Err(error) => scheduler_error(error),
    }
}

async fn set_worker_state(
    State(state): State<SchedulerState>,
    AxumPath(name): AxumPath<String>,
    Json(body): Json<StatePut>,
) -> Response {
    match state.registry.set_state(&name, &body.state).await {
        Ok(Some(worker)) => {
            state.ready.notify_one();
            Json(worker).into_response()
        }
        Ok(None) => (StatusCode::NOT_FOUND, format!("no worker named {name}")).into_response(),
        Err(error) => scheduler_error(error),
    }
}

async fn run_worker_now(
    State(state): State<SchedulerState>,
    AxumPath(name): AxumPath<String>,
    headers: HeaderMap,
) -> Response {
    let Some(idempotency_key) = headers
        .get("idempotency-key")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
    else {
        return (
            StatusCode::BAD_REQUEST,
            "idempotency-key header is required",
        )
            .into_response();
    };
    match state.registry.get(&name).await {
        Ok(Some(worker))
            if worker.state == "running" && worker.build_status == WorkerBuildStatus::Ready => {}
        Ok(Some(worker)) if worker.build_status != WorkerBuildStatus::Ready => {
            return (
                StatusCode::CONFLICT,
                format!("worker {name} build is {:?}", worker.build_status),
            )
                .into_response();
        }
        Ok(Some(worker)) => {
            return (
                StatusCode::CONFLICT,
                format!("worker {name} is {}, not running", worker.state),
            )
                .into_response();
        }
        Ok(None) => {
            return (StatusCode::NOT_FOUND, format!("no worker named {name}")).into_response();
        }
        Err(error) => return scheduler_error(error),
    }
    let event = CloudEvent::new(
        idempotency_key,
        "urn:verglas:scheduler",
        "org.verglas.worker.manual",
    );
    match state
        .queue
        .enqueue(&Invocation::new(name, event, Utc::now()))
        .await
    {
        Ok(outcome) => {
            state.ready.notify_one();
            let response = match outcome {
                EnqueueOutcome::Created(job_id) => EnqueueResponse {
                    job_id,
                    created: true,
                },
                EnqueueOutcome::Existing(job_id) => EnqueueResponse {
                    job_id,
                    created: false,
                },
            };
            Json(response).into_response()
        }
        Err(error) => scheduler_error(error),
    }
}

fn scheduler_error(error: verglas_scheduler::SchedulerError) -> Response {
    let status = match error {
        verglas_scheduler::SchedulerError::Invalid(_)
        | verglas_scheduler::SchedulerError::Json(_) => StatusCode::BAD_REQUEST,
        _ => StatusCode::SERVICE_UNAVAILABLE,
    };
    (status, error.to_string()).into_response()
}

async fn list_secrets(State(state): State<SchedulerState>) -> Response {
    match state.registry.secret_names().await {
        Ok(secrets) => Json(serde_json::json!({"secrets": secrets})).into_response(),
        Err(error) => scheduler_error(error),
    }
}

async fn put_secret(
    State(state): State<SchedulerState>,
    AxumPath(name): AxumPath<String>,
    Json(body): Json<SecretPut>,
) -> Response {
    match state.registry.put_secret(&name, &body.value).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => scheduler_error(error),
    }
}

async fn delete_secret(
    State(state): State<SchedulerState>,
    AxumPath(name): AxumPath<String>,
) -> Response {
    match state.registry.delete_secret(&name).await {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => (StatusCode::NOT_FOUND, format!("no secret named {name}")).into_response(),
        Err(error) => scheduler_error(error),
    }
}

async fn list_worker_jobs(
    State(state): State<SchedulerState>,
    AxumPath(name): AxumPath<String>,
    Query(query): Query<JobLimit>,
) -> Response {
    match state
        .queue
        .worker_jobs(&name, query.limit.unwrap_or(20))
        .await
    {
        Ok(jobs) => Json(jobs).into_response(),
        Err(error) => scheduler_error(error),
    }
}

async fn get_job(
    State(state): State<SchedulerState>,
    AxumPath(job_id): AxumPath<String>,
) -> Response {
    match state.queue.job(&job_id).await {
        Ok(Some(job)) => Json(job).into_response(),
        Ok(None) => (StatusCode::NOT_FOUND, format!("no job named {job_id}")).into_response(),
        Err(error) => scheduler_error(error),
    }
}

/// Reports that the scheduler event receiver is alive.
async fn healthz() -> &'static str {
    "ok"
}

/// Builds the scheduler's pushed-event API.
fn scheduler_router(state: SchedulerState, control_token: String) -> Router {
    let control = Router::new()
        .route("/v1/events", post(submit_event))
        .route("/v1/workers", get(list_workers).post(register_worker))
        .route("/v1/workers/{name}", get(get_worker))
        .route("/v1/workers/{name}/state", put(set_worker_state))
        .route("/v1/workers/{name}/run", post(run_worker_now))
        .route("/v1/workers/{name}/jobs", get(list_worker_jobs))
        .route("/v1/jobs/{job_id}", get(get_job))
        .route("/v1/secrets", get(list_secrets))
        .route("/v1/secrets/{name}", put(put_secret).delete(delete_secret))
        .with_state(state)
        .route_layer(middleware::from_fn_with_state(
            ControlAuth(Arc::from(format!("Bearer {control_token}"))),
            authorize_control,
        ));
    Router::new().route("/healthz", get(healthz)).merge(control)
}

/// Reconstructs cron progress from run objects and materializes every due run.
async fn reconcile_cron(
    registry: &PgWorkerRegistry,
    queue: &dyn RunQueue,
    now: DateTime<Utc>,
) -> Result<Option<DateTime<Utc>>, String> {
    let workers = registry
        .list(false)
        .await
        .map_err(|error| error.to_string())?;
    let jobs = queue.jobs().await.map_err(|error| error.to_string())?;
    let mut next_wake_at = None;
    for worker in workers.into_iter().filter(|worker| {
        worker.state == "running" && worker.build_status == WorkerBuildStatus::Ready
    }) {
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
    registry: &PgWorkerRegistry,
    queue: &dyn RunQueue,
    claimed: ClaimedJob,
) -> Result<(Lease, Completion), String> {
    let worker = registry
        .get(&claimed.job.worker)
        .await
        .map_err(|error| error.to_string())?
        .ok_or_else(|| format!("no worker named {}", claimed.job.worker))?;
    let prepared = prepare_registered_worker(registry, &worker).await?;
    let runtime = RuntimeClient::new(
        args.container_runtime_url.clone(),
        args.container_runtime_token.clone(),
    );
    execute_claimed_worker(args, &runtime, &worker, prepared, queue, claimed).await
}

async fn execute_claimed_worker(
    args: &Args,
    runtime: &RuntimeClient,
    worker: &WorkerRecord,
    prepared: PreparedWorker,
    queue: &dyn RunQueue,
    claimed: ClaimedJob,
) -> Result<(Lease, Completion), String> {
    let invocation = WorkerInvocation {
        run_id: format!("run-{}", claimed.job.id),
        worker: worker.name.clone(),
        image: prepared.image,
        entrypoint: prepared.entrypoint,
        environment: prepared.environment,
        target: worker.output.clone().unwrap_or_default(),
        endpoint: args.worker_endpoint.clone(),
        token: String::new(),
        network: None,
        event: serde_json::to_value(&claimed.job.event)
            .map_err(|error| format!("serialize worker event: {error}"))?,
        resources: prepared.resources,
        timeout_seconds: prepared.timeout_seconds,
        scratch_target: prepared.scratch_target,
    };
    let mut lease = claimed.lease;
    let renew_every = Duration::from_secs((args.lease_seconds / 2).max(1));
    let mut renewal_error = None;
    let mut execution = Box::pin(runtime.run(&invocation));
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
    registry: &PgWorkerRegistry,
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
    let (lease, completion) = match execute_claimed(args, registry, queue, claimed).await {
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
    registry: &PgWorkerRegistry,
    queue: &dyn RunQueue,
    ready: &tokio::sync::Notify,
) -> Result<(), String> {
    loop {
        let now = Utc::now();
        let cron_deadline = reconcile_cron(registry, queue, now).await?;
        while run_one(args, registry, queue).await? {}
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
    let encryption_key = match hex::decode(&args.secret_encryption_key) {
        Ok(key) if key.len() == 32 => key,
        _ => {
            eprintln!(
                "verglas-scheduler: VERGLAS_SECRET_ENCRYPTION_KEY must be 64 hexadecimal characters"
            );
            std::process::exit(1);
        }
    };
    let registry =
        match PgWorkerRegistry::connect(&args.database_url, &args.queue, &encryption_key).await {
            Ok(registry) => Arc::new(registry),
            Err(error) => {
                eprintln!("verglas-scheduler: connect worker registry: {error}");
                std::process::exit(1);
            }
        };
    let ready = Arc::new(tokio::sync::Notify::new());
    let app = scheduler_router(
        SchedulerState {
            queue: queue.clone(),
            registry: registry.clone(),
            ready: ready.clone(),
            runtime: RuntimeClient::new(
                args.container_runtime_url.clone(),
                args.container_runtime_token.clone(),
            ),
        },
        args.control_token.clone(),
    );
    let listener = match tokio::net::TcpListener::bind(args.listen).await {
        Ok(listener) => listener,
        Err(error) => {
            eprintln!("verglas-scheduler: bind {}: {error}", args.listen);
            std::process::exit(1);
        }
    };
    eprintln!(
        "verglas-scheduler {} queue={} listen={}",
        env!("CARGO_PKG_VERSION"),
        args.queue,
        args.listen,
    );
    let server = axum::serve(listener, app);
    tokio::select! {
        result = server => {
            eprintln!("verglas-scheduler: event API: {}", result
                .err()
                .map_or_else(|| "stopped".to_owned(), |error| error.to_string()));
        }
        result = scheduler_loop(&args, registry.as_ref(), queue.as_ref(), &ready) => {
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

    /// Built image identity and hard limits are prepared for one container run.
    #[test]
    fn prepares_a_bounded_worker_invocation() {
        let worker = WorkerRecord {
            name: "market-data-ingest".to_owned(),
            code: r#"{"image":"verglas/worker-ingest:sha256-test","entrypoint":["python","ingest.py"]}"#.to_owned(),
            output: Some("market.ohlcv".to_owned()),
            triggers: "[]".to_owned(),
            state: "running".to_owned(),
            placement: "local".to_owned(),
            created_by: "test".to_owned(),
            created_at: Utc::now(),
            revision: 1,
            build_status: WorkerBuildStatus::Ready,
            image_digest: Some("sha256:test".to_owned()),
            config: r#"{
                "env":{"SYMBOL":"SPY"},
                "resources":{"vcpus":2.0,"mem_mib":4096,"pids":128,"timeout_secs":900}
            }"#
            .to_owned(),
        };

        let config: WorkerConfig = serde_json::from_str(&worker.config).expect("config");
        let prepared = prepare_worker_config(&worker, config).expect("prepare worker");
        assert_eq!(
            prepared.environment.get("SYMBOL").map(String::as_str),
            Some("SPY")
        );
        assert_eq!(prepared.image, "verglas/worker-ingest:sha256-test");
        assert_eq!(prepared.resources.memory_mib, 4096);
        assert_eq!(prepared.timeout_seconds, 900);
    }

    /// The pushed-event API acknowledges only after the queue persists the event.
    #[tokio::test]
    async fn pushed_event_is_persisted_before_acceptance() {
        let queue = Arc::new(TestQueue::default());
        let ready = tokio::sync::Notify::new();
        let invocation = Invocation::new(
            "http-worker",
            CloudEvent::new("request-1", "urn:verglas:http", "org.verglas.http.request"),
            Utc::now(),
        );

        let response = persist_event(queue.as_ref(), &ready, &invocation).await;
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

    /// Runtime rejection details remain visible without allowing unbounded output.
    #[tokio::test]
    async fn runtime_rejection_preserves_a_bounded_response_body() {
        let oversized = format!("diagnostic:{}", "x".repeat(RUNTIME_ERROR_BODY_LIMIT + 100));
        let app = Router::new().route(
            "/reject",
            get(move || {
                let oversized = oversized.clone();
                async move { (StatusCode::UNPROCESSABLE_ENTITY, oversized) }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let endpoint = format!("http://{}/reject", listener.local_addr().expect("address"));
        tokio::spawn(async move { axum::serve(listener, app).await.expect("runtime server") });

        let response = reqwest::get(endpoint).await.expect("response");
        let error = runtime_response(response, "worker run")
            .await
            .expect_err("rejection");

        assert!(error.contains("422 Unprocessable Entity"));
        assert!(error.contains("diagnostic:"));
        assert!(error.len() < RUNTIME_ERROR_BODY_LIMIT + 100);
    }

    /// A claimed worker executes through the container runtime and becomes a completion.
    #[tokio::test]
    async fn claimed_worker_executes_as_one_bounded_run() {
        let code = serde_json::json!({
            "image": "verglas/worker-a:sha256-test",
            "entrypoint": ["python", "worker.py"]
        })
        .to_string();
        let worker = WorkerRecord {
            name: "worker-a".to_owned(),
            code,
            output: Some("app.output".to_owned()),
            triggers: "[]".to_owned(),
            state: "running".to_owned(),
            config: "{}".to_owned(),
            placement: "local".to_owned(),
            created_by: "test".to_owned(),
            created_at: Utc::now(),
            revision: 1,
            build_status: WorkerBuildStatus::Ready,
            image_digest: Some("sha256:test".to_owned()),
        };
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
            worker_endpoint: "http://127.0.0.1:8334".to_owned(),
            queue: "local".to_owned(),
            consumer: "consumer-1".to_owned(),
            lease_seconds: 300,
            listen: "127.0.0.1:0".parse().expect("listen"),
            control_token: "test-control".to_owned(),
            secret_encryption_key: "00".repeat(32),
            container_runtime_url: String::new(),
            container_runtime_token: "runtime-token".to_owned(),
        };

        let queue = TestQueue::default();
        let app = Router::new().route(
            "/v1/worker-runs/{run_id}",
            put(|| async {
                Json(WorkerRunResult {
                    rows_produced: 3,
                    logs: String::new(),
                })
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let endpoint = format!("http://{}", listener.local_addr().expect("address"));
        tokio::spawn(async move { axum::serve(listener, app).await.expect("runtime server") });
        let runtime = RuntimeClient::new(endpoint, "runtime-token");
        let config: WorkerConfig = serde_json::from_str(&worker.config).expect("config");
        let prepared = prepare_worker_config(&worker, config).expect("prepare worker");
        let (_, completion) =
            execute_claimed_worker(&args, &runtime, &worker, prepared, &queue, claimed)
                .await
                .expect("execute");
        assert_eq!(completion, Completion::Succeeded { rows_produced: 3 });
    }
}
