//! The Rust worker contract: the single deployment primitive, mirroring the
//! TypeScript SDK's `contracts.ts` `defineWorker`/`runWorker` model.
//!
//! A worker is code plus one or more triggers. It replaces the former
//! Source / Sink / MV trio: instead of three Job kinds each with its own
//! executor, there is one [`Worker`] whose [`TriggerSpec`]s say when it runs and
//! whose output table(s) are deployment config, injected at run time. This
//! module is only the contract — no executor or engine logic lives here (the
//! harness owns the run loop, exactly as it did for the old Jobs).
//!
//! # Mapping to `sdks/typescript/src/contracts.ts`
//!
//! The Rust shapes mirror the TS shapes field for field; where the languages
//! force a different spelling the wire name is pinned with `#[serde(rename)]` so
//! a worker authored in TypeScript and one authored in Rust register the same
//! trigger JSON.
//!
//! | TS (`contracts.ts`)                         | Rust (this module)                    |
//! |---------------------------------------------|---------------------------------------|
//! | `WorkerDefinition`                          | [`Worker`] trait + [`TriggerSpec`]    |
//! | `TriggerSpec` (`type` discriminant)         | [`TriggerSpec`] (`#[serde(tag="type")]`) |
//! | `CronTriggerSpec.catchup`                   | [`Catchup`]                           |
//! | `CloudEvent`                                | [`CloudEvent`]                        |
//! | cron CloudEvent `data`                     | [`CronInterval`]                      |
//! | `ChangeEvent`                               | [`ChangeEvent`]                       |
//! | `WorkerContext`                             | [`WorkerContext`]                     |
//! | `WorkerResult.rowsWritten`                  | [`WorkerResult::rows_written`]        |
//! | `EndpointRunResult` (`endpoint-run.ts`)     | [`RunResult`]                         |
//!
//! # Subprocess workers are env-in / result-file-out
//!
//! A worker in another language runs as a subprocess exactly the way the TS
//! `endpoint-run` harness runs one: the parent sets the run's environment
//! ([`ENV_CLOUD_EVENT`], [`ENV_DEPLOYMENT`], [`ENV_TARGET`], plus the endpoint
//! and token), spawns the child, and reads a small result JSON ([`RunResult`])
//! back from [`ENV_RESULT_PATH`]. There is no framed stdio protocol on the
//! worker side — progress is the trigger's logical time, not a durable cursor
//! the child owns.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::grant::{LocalGrantHost, MemoryGrantHost, MemoryGrantRequest};
use crate::job::{JobError, Logger, Row};

/// The complete serialized CloudEvents 1.0 envelope delivered to a subprocess.
pub const ENV_CLOUD_EVENT: &str = "VERGLAS_CLOUD_EVENT";
/// The deployment name bound into the worker subprocess environment.
pub const ENV_DEPLOYMENT: &str = "DEPLOYMENT";
/// The deployment-configured output table (the first, when there are several).
pub const ENV_TARGET: &str = "TARGET";
/// The data-plane endpoint the worker's client connects to.
pub const ENV_ENDPOINT: &str = "VERGLAS_ENDPOINT";
/// The bearer token the worker's client authenticates with.
pub const ENV_TOKEN: &str = "VERGLAS_TOKEN";
/// The path the subprocess worker writes its [`RunResult`] JSON to.
pub const ENV_RESULT_PATH: &str = "RESULT_PATH";
/// The default result path when [`ENV_RESULT_PATH`] is unset — matches the TS
/// `endpoint-run` harness.
pub const DEFAULT_RESULT_PATH: &str = "/run/result.json";

/// How a cron worker with a `start_date` backlog runs the missed intervals.
/// Mirrors the TS `CronTriggerSpec.catchup`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Catchup {
    /// One interval at a time, oldest first (ordered backfill).
    Sequential,
    /// Intervals fanned out concurrently (fast, unordered).
    Parallel,
    /// Skip the backlog; start at the next live interval.
    None,
}

impl Default for Catchup {
    /// Absent catchup means skip the backlog — a fresh cron worker never fires a
    /// surprise backfill storm on first boot.
    fn default() -> Self {
        Catchup::None
    }
}

/// A trigger declaration — deployment config saying when a worker runs. The
/// `type` field is the discriminant, matching the TS `TriggerSpec` union.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TriggerSpec {
    /// Fire on a cron schedule; optionally backfill from `start_date`.
    Cron {
        /// A five-field cron expression, platform-parsed.
        schedule: String,
        /// Backfill anchor: catch up scheduled intervals from here until live.
        #[serde(default, rename = "startDate", skip_serializing_if = "Option::is_none")]
        start_date: Option<String>,
        /// How to run a `start_date` backlog.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        catchup: Option<Catchup>,
    },
    /// Fire when a request is routed to the worker's webhook path.
    Webhook {
        /// The path the webhook is mounted at (platform-scoped).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        path: Option<String>,
    },
    /// Fire when an accepted CloudEvent matches the declared attributes.
    Event {
        /// Exact CloudEvent `type` to accept.
        #[serde(rename = "eventType")]
        event_type: String,
        /// Optional exact CloudEvent `source` filter.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        source: Option<String>,
        /// Optional exact CloudEvent `subject` filter.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        subject: Option<String>,
    },
    /// Follow a local process or file continuously, appending each captured
    /// output line to the worker's target table as a row.
    ///
    /// This trigger runs the worker as a long-lived local process rather than a
    /// one-shot subprocess. When `file` is set the server tails that file; when
    /// it is absent the server runs the worker's own `exec` command and captures
    /// its stdout and stderr. Follow is local-only: it tails something on the
    /// machine the server runs on. Captured rows append through the server's
    /// catalog into the configured lakehouse.
    Follow {
        /// A file to tail. When absent, the server wraps the worker's `exec`
        /// command and captures its stdout and stderr instead.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        file: Option<String>,
    },
}

impl TriggerSpec {
    /// The wire discriminant (`cron`, `webhook`, `event`, `follow`).
    pub fn kind(&self) -> &'static str {
        match self {
            TriggerSpec::Cron { .. } => "cron",
            TriggerSpec::Webhook { .. } => "webhook",
            TriggerSpec::Event { .. } => "event",
            TriggerSpec::Follow { .. } => "follow",
        }
    }

    /// Returns whether this event subscription accepts a CloudEvent.
    pub fn matches(&self, event: &CloudEvent) -> bool {
        match self {
            TriggerSpec::Event {
                event_type,
                source,
                subject,
            } => {
                event.event_type == *event_type
                    && source.as_ref().is_none_or(|value| event.source == *value)
                    && subject
                        .as_ref()
                        .is_none_or(|value| event.subject.as_ref() == Some(value))
            }
            _ => false,
        }
    }
}

/// One CloudEvents 1.0 structured event delivered throughout the worker system.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CloudEvent {
    /// CloudEvents specification version. Only `1.0` is accepted.
    pub specversion: String,
    /// Producer-scoped stable event identity.
    pub id: String,
    /// Stable URI-reference identifying the producer.
    pub source: String,
    /// Event kind used by worker subscription filters.
    #[serde(rename = "type")]
    pub event_type: String,
    /// Optional resource within the source that the event concerns.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
    /// Optional event timestamp in RFC 3339 form.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time: Option<String>,
    /// Optional media type of `data`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub datacontenttype: Option<String>,
    /// Optional schema URI for `data`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dataschema: Option<String>,
    /// Event-specific structured payload.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
    /// Event-specific binary payload encoded as base64.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_base64: Option<String>,
    /// CloudEvents extension attributes not known to this SDK.
    #[serde(flatten)]
    pub extensions: BTreeMap<String, serde_json::Value>,
}

impl CloudEvent {
    /// Creates a CloudEvents 1.0 envelope with no optional attributes.
    pub fn new(
        id: impl Into<String>,
        source: impl Into<String>,
        event_type: impl Into<String>,
    ) -> CloudEvent {
        CloudEvent {
            specversion: "1.0".to_owned(),
            id: id.into(),
            source: source.into(),
            event_type: event_type.into(),
            subject: None,
            time: None,
            datacontenttype: None,
            dataschema: None,
            data: None,
            data_base64: None,
            extensions: BTreeMap::new(),
        }
    }

    /// Validates the required attributes and the supported CloudEvents version.
    pub fn validate(&self) -> Result<(), String> {
        if self.specversion != "1.0" {
            return Err(format!(
                "unsupported CloudEvents specversion `{}`",
                self.specversion
            ));
        }
        for (name, value) in [
            ("id", self.id.as_str()),
            ("source", self.source.as_str()),
            ("type", self.event_type.as_str()),
        ] {
            if value.is_empty() {
                return Err(format!("CloudEvent `{name}` is required"));
            }
        }
        if self.data.is_some() && self.data_base64.is_some() {
            return Err("CloudEvent cannot contain both `data` and `data_base64`".to_owned());
        }
        Ok(())
    }

    /// Reads and validates the single structured CloudEvent subprocess binding.
    pub fn from_env<F: Fn(&str) -> Option<String>>(getenv: F) -> Result<CloudEvent, String> {
        let body =
            getenv(ENV_CLOUD_EVENT).ok_or_else(|| format!("{ENV_CLOUD_EVENT} is required"))?;
        let event: CloudEvent = serde_json::from_str(&body)
            .map_err(|error| format!("invalid {ENV_CLOUD_EVENT}: {error}"))?;
        event.validate()?;
        Ok(event)
    }
}

/// The logical interval a cron run covers, half-open `[start, end)`. Mirrors the
/// Cron CloudEvent `data` fields `logicalDate` / `intervalStart` / `intervalEnd`. All
/// ISO 8601 strings; the worker reads them, it never parses a durable cursor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CronInterval {
    /// The nominal scheduled instant for this run.
    #[serde(rename = "logicalDate")]
    pub logical_date: String,
    /// Inclusive start of the logical interval this run covers.
    #[serde(rename = "intervalStart")]
    pub interval_start: String,
    /// Exclusive end of the logical interval this run covers.
    #[serde(rename = "intervalEnd")]
    pub interval_end: String,
}

/// A committed-table change from the catalog follow feed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangeEvent {
    /// The feed's monotonic sequence number for this change.
    pub seq: u64,
    /// The fully-qualified table that committed, `namespace.table`.
    pub table: String,
    /// The id of the snapshot the commit produced.
    #[serde(rename = "snapshotId", alias = "snapshot_id")]
    pub snapshot_id: String,
    /// When the commit landed, ISO 8601.
    #[serde(rename = "committedAt", alias = "committed_at")]
    pub committed_at: String,
}

/// One complete HTTP callback delivered as a worker event.
///
/// The scheduler owns no client connection. It stores these request bytes with
/// the run and hands them to the worker when the run is claimed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HttpCallback {
    /// Original HTTP method.
    pub method: String,
    /// Routed callback path, including its leading slash.
    pub path: String,
    /// Request headers after hop-by-hop headers have been removed.
    pub headers: BTreeMap<String, String>,
    /// Complete request body bytes.
    pub body: Vec<u8>,
}

/// What a [`Worker::run`] reports back. Mirrors the TS `WorkerResult`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerResult {
    /// Rows the run wrote, for the run summary. The harness also counts
    /// committed rows through the instrumented client, so this is advisory.
    #[serde(
        default,
        rename = "rowsWritten",
        skip_serializing_if = "Option::is_none"
    )]
    pub rows_written: Option<u64>,
}

/// The result JSON a subprocess worker writes to [`ENV_RESULT_PATH`], and the
/// server reads back. Mirrors the TS `EndpointRunResult`: `{"rows": n,
/// "error": null}` on success, `{"rows": 0, "error": "<message>"}` on failure.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunResult {
    /// Rows the run committed to its output table.
    pub rows: u64,
    /// `null` on success; the failure message otherwise.
    pub error: Option<String>,
}

impl RunResult {
    /// A success result reporting `rows` written.
    pub fn ok(rows: u64) -> RunResult {
        RunResult { rows, error: None }
    }

    /// A failure result carrying `message`.
    pub fn failed(message: impl Into<String>) -> RunResult {
        RunResult {
            rows: 0,
            error: Some(message.into()),
        }
    }
}

/// The runtime handed to an in-process [`Worker`]. Mirrors the TS
/// `WorkerContext`: the client, the invoking trigger, the deployment-configured
/// output table(s), the environment, a logger, and an abort flag. The client
/// type is generic (`C`) until the Rust data-plane client exists; each harness
/// pins it.
pub struct WorkerContext<C> {
    /// A connected client for the target endpoint (read/write via table verbs).
    pub client: C,
    /// The event that invoked this run.
    pub trigger: CloudEvent,
    /// The deployment-configured output table (the first configured output).
    pub output: String,
    /// Every deployment-configured output table (>= 1). `output` is
    /// `outputs[0]`.
    pub outputs: Vec<String>,
    /// The deployment environment: declared secret bindings and config values.
    pub env: serde_json::Map<String, serde_json::Value>,
    /// Structured log sink for worker-specific steps (stderr/tracing, not a
    /// platform `_LOGS` table). Never write secrets through it.
    pub log: Logger,
    /// Abort flag (true = stop long-running work). Mirrors the TS `AbortSignal`.
    pub signal: Option<Arc<AtomicBool>>,
    /// The memory grant host: request an initial allowance, grow it grow-only
    /// while running, release it when done. Every worker kind gets this, not
    /// just the query role — a worker with no opinion about its memory need
    /// simply never calls it. Defaults to [`LocalGrantHost`] (grants whatever
    /// is asked, enforces nothing) when the harness runs with no host agent
    /// attached; the harness swaps in the real enforcing host when one is.
    pub grant: Arc<dyn MemoryGrantHost>,
}

impl<C> WorkerContext<C> {
    /// The default grant host for a context the harness builds without an
    /// attached host agent — bookkeeping only, unconstrained. A standalone
    /// role binary not driven through `WorkerContext` at all (`verglas-query`
    /// today) holds a `MemoryGrantHost` directly instead, the same trait.
    pub fn default_grant_host() -> Arc<dyn MemoryGrantHost> {
        Arc::new(LocalGrantHost)
    }
}

/// A worker: code plus triggers, the single deployment primitive. Mirrors the TS
/// `WorkerDefinition`. `run` is invoked once per trigger event; the output table
/// is deployment config the harness passes in through [`WorkerContext`], never
/// hardcoded here.
#[async_trait]
pub trait Worker<C: Send + 'static>: Send {
    /// Deployment name. `None` means the harness falls back to the configured
    /// output table.
    fn name(&self) -> Option<&str> {
        None
    }

    /// The trigger(s) this worker expects. Informational in the contract — the
    /// deploy path is what actually registers them into `verglas_sys.workers`.
    fn triggers(&self) -> Vec<TriggerSpec> {
        Vec::new()
    }

    /// The memory grant this worker wants requested before it's launched, if
    /// it can say up front. The default is `None`: the executor launches the
    /// worker with no pre-request and the worker sizes itself reactively
    /// (its first `ctx.grant.request(...)`/`.grow(...)` call inside `run`). A
    /// worker that can estimate ahead of time overrides this — the query role
    /// does, deriving it from a plan-based estimate over the query it's about
    /// to run — so the executor can request the grant as part of launch
    /// rather than after the worker is already running.
    fn initial_grant_request(&self) -> Option<MemoryGrantRequest> {
        None
    }

    /// Runs once for the given trigger event and reports what it wrote.
    async fn run(&mut self, ctx: &mut WorkerContext<C>) -> Result<WorkerResult, JobError>;
}

/// Convenience: the `meta` [`Row`] a worker logs — a JSON object. Re-exported so
/// worker code names it without reaching into [`crate::job`].
pub type WorkerMeta = Row;

#[cfg(test)]
mod tests {
    use super::*;

    /// A cron trigger spec round-trips through the TS wire shape: `type`,
    /// `schedule`, `startDate`, and the snake_case `catchup` values.
    #[test]
    fn cron_spec_matches_ts_wire() {
        let json = r#"{"type":"cron","schedule":"0 0 * * *","startDate":"2026-01-01T00:00:00Z","catchup":"sequential"}"#;
        let spec: TriggerSpec = serde_json::from_str(json).expect("parse");
        assert_eq!(
            spec,
            TriggerSpec::Cron {
                schedule: "0 0 * * *".to_owned(),
                start_date: Some("2026-01-01T00:00:00Z".to_owned()),
                catchup: Some(Catchup::Sequential),
            }
        );
        assert_eq!(spec.kind(), "cron");
        // Re-serializes to the same camelCase key the TS side reads.
        assert!(
            serde_json::to_string(&spec)
                .expect("ser")
                .contains("startDate")
        );
    }

    /// Event subscriptions match CloudEvents by type and optional source and subject.
    #[test]
    fn event_trigger_matches_cloud_event() {
        let spec: TriggerSpec = serde_json::from_str(
            r#"{"type":"event","eventType":"org.apache.iceberg.snapshot.committed","source":"urn:verglas:catalog:local","subject":"agent_memory.memories"}"#,
        )
        .expect("event trigger");
        assert_eq!(
            spec,
            TriggerSpec::Event {
                event_type: "org.apache.iceberg.snapshot.committed".to_owned(),
                source: Some("urn:verglas:catalog:local".to_owned()),
                subject: Some("agent_memory.memories".to_owned()),
            }
        );
        let event: CloudEvent = serde_json::from_str(
            r#"{"specversion":"1.0","id":"42","source":"urn:verglas:catalog:local","type":"org.apache.iceberg.snapshot.committed","subject":"agent_memory.memories","data":{"snapshotId":"99"}}"#,
        )
        .expect("CloudEvent");
        assert!(spec.matches(&event));
    }

    /// Worker events accept exactly CloudEvents 1.0, not the former trigger union.
    #[test]
    fn cloud_event_rejects_other_spec_versions() {
        let event: CloudEvent = serde_json::from_str(
            r#"{"specversion":"0.3","id":"42","source":"urn:test","type":"test"}"#,
        )
        .expect("wire shape");
        assert_eq!(
            event.validate().expect_err("unsupported spec version"),
            "unsupported CloudEvents specversion `0.3`"
        );
    }

    /// The subprocess contract reads one complete CloudEvent envelope.
    #[test]
    fn cloud_event_from_env() {
        let mut expected = CloudEvent::new("run-1", "urn:verglas:scheduler", "org.verglas.cron");
        expected.data = Some(serde_json::json!({
            "logicalDate": "2026-08-01T00:00:00Z",
            "intervalStart": "2026-07-31T00:00:00Z",
            "intervalEnd": "2026-08-01T00:00:00Z"
        }));
        let body = serde_json::to_string(&expected).expect("CloudEvent JSON");
        assert_eq!(
            CloudEvent::from_env(|key| (key == ENV_CLOUD_EVENT).then(|| body.clone()))
                .expect("CloudEvent"),
            expected
        );
    }

    /// The harness must receive a CloudEvent for every worker run.
    #[test]
    fn missing_cloud_event_is_rejected() {
        let error = CloudEvent::from_env(|_| None).expect_err("missing event");
        assert_eq!(error, "VERGLAS_CLOUD_EVENT is required");
    }

    /// Websocket is not a worker scheduling trigger.
    #[test]
    fn websocket_trigger_spec_is_rejected() {
        assert!(
            serde_json::from_str::<TriggerSpec>(r#"{"type":"websocket","path":"/ws"}"#).is_err()
        );
    }

    /// The subprocess result file round-trips the success and failure shapes the
    /// TS harness writes.
    #[test]
    fn run_result_wire_shapes() {
        assert_eq!(
            serde_json::to_string(&RunResult::ok(7)).expect("ok"),
            r#"{"rows":7,"error":null}"#
        );
        let failed: RunResult =
            serde_json::from_str(r#"{"rows":0,"error":"boom"}"#).expect("failed");
        assert_eq!(failed, RunResult::failed("boom"));
    }

    /// A `ChangeEvent` reads either the camelCase feed spelling or the snake_case
    /// alias, so a Rust-emitted and a TS-emitted change parse the same.
    #[test]
    fn change_event_accepts_both_spellings() {
        let camel: ChangeEvent = serde_json::from_str(
            r#"{"seq":3,"table":"a.b","snapshotId":"99","committedAt":"2026-08-01T00:00:00Z"}"#,
        )
        .expect("camel");
        let snake: ChangeEvent = serde_json::from_str(
            r#"{"seq":3,"table":"a.b","snapshot_id":"99","committed_at":"2026-08-01T00:00:00Z"}"#,
        )
        .expect("snake");
        assert_eq!(camel, snake);
    }
}
