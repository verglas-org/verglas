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
//! | `TriggerEvent`                              | [`TriggerEvent`]                      |
//! | `CronTriggerEvent.{logicalDate,interval*}`  | [`CronInterval`]                      |
//! | `ChangeEvent`                               | [`ChangeEvent`]                       |
//! | `WorkerContext`                             | [`WorkerContext`]                     |
//! | `WorkerResult.rowsWritten`                  | [`WorkerResult::rows_written`]        |
//! | `EndpointRunResult` (`endpoint-run.ts`)     | [`RunResult`]                         |
//!
//! # Subprocess workers are env-in / result-file-out
//!
//! A worker in another language runs as a subprocess exactly the way the TS
//! `endpoint-run` harness runs one: the parent sets the run's environment
//! ([`ENV_TRIGGER`], [`ENV_LOGICAL_DATE`], [`ENV_INTERVAL_START`],
//! [`ENV_INTERVAL_END`], [`ENV_DEPLOYMENT`], [`ENV_TARGET`], plus the endpoint
//! and token), spawns the child, and reads a small result JSON ([`RunResult`])
//! back from [`ENV_RESULT_PATH`]. There is no framed stdio protocol on the
//! worker side — progress is the trigger's logical time, not a durable cursor
//! the child owns.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::grant::{LocalGrantHost, MemoryGrantHost, MemoryGrantRequest};
use crate::job::{JobError, Logger, Row};

/// The environment variable naming the trigger kind for a subprocess worker
/// (`cron`, `webhook`, `websocket`, `data_change`).
pub const ENV_TRIGGER: &str = "VERGLAS_TRIGGER";
/// The nominal scheduled instant of a cron run (ISO 8601).
pub const ENV_LOGICAL_DATE: &str = "VERGLAS_LOGICAL_DATE";
/// The inclusive start of a cron run's logical interval (ISO 8601).
pub const ENV_INTERVAL_START: &str = "VERGLAS_INTERVAL_START";
/// The exclusive end of a cron run's logical interval (ISO 8601).
pub const ENV_INTERVAL_END: &str = "VERGLAS_INTERVAL_END";
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

/// The table(s) a `data_change` trigger follows: the TS `string | string[]`.
/// Deserializes from either a bare string or an array; always presents as a
/// list.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum TableRef {
    /// A single dotted `namespace.table`.
    One(String),
    /// Several dotted tables.
    Many(Vec<String>),
}

impl TableRef {
    /// The tables as a flat list, whether declared as one or many.
    pub fn tables(&self) -> Vec<String> {
        match self {
            TableRef::One(t) => vec![t.clone()],
            TableRef::Many(ts) => ts.clone(),
        }
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
    /// Fire on each message of a websocket the worker follows.
    Websocket {
        /// The path the websocket is mounted at (platform-scoped).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        path: Option<String>,
    },
    /// Fire when a commit lands on any of the named tables.
    DataChange {
        /// The table(s) whose commits invoke the worker.
        table: TableRef,
    },
    /// Follow a local process or file continuously, appending each captured
    /// output line to the worker's target table as a row.
    ///
    /// This trigger runs the worker as a long-lived local process rather than a
    /// one-shot subprocess. When `file` is set the server tails that file; when
    /// it is absent the server runs the worker's own `exec` command and captures
    /// its stdout and stderr. It is LOCAL ONLY: a follow worker tails something
    /// on the machine the server runs on, so the cloud rejects it for fleet
    /// placement. When the server is logged in, the rows still stream into the
    /// tenant's cloud lakehouse, because the server's catalog points there.
    Follow {
        /// A file to tail. When absent, the server wraps the worker's `exec`
        /// command and captures its stdout and stderr instead.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        file: Option<String>,
    },
}

impl TriggerSpec {
    /// The wire discriminant (`cron`, `webhook`, `websocket`, `data_change`),
    /// for logging and the [`ENV_TRIGGER`] binding.
    pub fn kind(&self) -> &'static str {
        match self {
            TriggerSpec::Cron { .. } => "cron",
            TriggerSpec::Webhook { .. } => "webhook",
            TriggerSpec::Websocket { .. } => "websocket",
            TriggerSpec::DataChange { .. } => "data_change",
            TriggerSpec::Follow { .. } => "follow",
        }
    }
}

/// The logical interval a cron run covers, half-open `[start, end)`. Mirrors the
/// TS `CronTriggerEvent`'s `logicalDate` / `intervalStart` / `intervalEnd`. All
/// ISO 8601 strings; the worker reads them, it never parses a durable cursor.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct CronInterval {
    /// The nominal scheduled instant for this run.
    #[serde(
        default,
        rename = "logicalDate",
        skip_serializing_if = "Option::is_none"
    )]
    pub logical_date: Option<String>,
    /// Inclusive start of the logical interval this run covers.
    #[serde(
        default,
        rename = "intervalStart",
        skip_serializing_if = "Option::is_none"
    )]
    pub interval_start: Option<String>,
    /// Exclusive end of the logical interval this run covers.
    #[serde(
        default,
        rename = "intervalEnd",
        skip_serializing_if = "Option::is_none"
    )]
    pub interval_end: Option<String>,
}

/// A committed-table change, the payload of a `data_change` run. Mirrors the TS
/// `ChangeEvent`; the field spellings match the catalog feed's wire JSON.
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

/// The event that invoked one worker run. Mirrors the TS `TriggerEvent`; the
/// `type` field is the discriminant. Webhook/websocket bodies reach a
/// subprocess worker through its own client, so the Rust event carries only what
/// the server knows.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TriggerEvent {
    /// A scheduled run over a logical interval.
    Cron(CronInterval),
    /// A routed webhook run.
    Webhook,
    /// One websocket message.
    Websocket {
        /// The message body, as text.
        message: String,
    },
    /// A run fired by a table commit.
    DataChange {
        /// The change that fired the run.
        change: ChangeEvent,
    },
}

impl TriggerEvent {
    /// The wire discriminant, for logging and the [`ENV_TRIGGER`] binding.
    pub fn kind(&self) -> &'static str {
        match self {
            TriggerEvent::Cron(_) => "cron",
            TriggerEvent::Webhook => "webhook",
            TriggerEvent::Websocket { .. } => "websocket",
            TriggerEvent::DataChange { .. } => "data_change",
        }
    }

    /// Builds the trigger event for a subprocess run from its environment,
    /// mirroring the TS `endpoint-run` harness's `cronTrigger`. An unset or
    /// `cron` [`ENV_TRIGGER`] yields a [`TriggerEvent::Cron`] reading the
    /// interval env vars; any other value maps to that kind with an empty body.
    pub fn from_env<F: Fn(&str) -> Option<String>>(getenv: F) -> TriggerEvent {
        match getenv(ENV_TRIGGER).as_deref() {
            Some("webhook") => TriggerEvent::Webhook,
            Some("websocket") => TriggerEvent::Websocket {
                message: String::new(),
            },
            Some("data_change") => TriggerEvent::DataChange {
                change: ChangeEvent {
                    seq: 0,
                    table: getenv(ENV_TARGET).unwrap_or_default(),
                    snapshot_id: String::new(),
                    committed_at: String::new(),
                },
            },
            _ => TriggerEvent::Cron(CronInterval {
                logical_date: getenv(ENV_LOGICAL_DATE),
                interval_start: getenv(ENV_INTERVAL_START),
                interval_end: getenv(ENV_INTERVAL_END),
            }),
        }
    }
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
    pub trigger: TriggerEvent,
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

    /// A `data_change` trigger accepts both a single table and a list, matching
    /// the TS `string | string[]`.
    #[test]
    fn data_change_accepts_one_or_many_tables() {
        let one: TriggerSpec =
            serde_json::from_str(r#"{"type":"data_change","table":"agent_memory.memories"}"#)
                .expect("one");
        assert_eq!(
            one,
            TriggerSpec::DataChange {
                table: TableRef::One("agent_memory.memories".to_owned())
            }
        );
        let many: TriggerSpec =
            serde_json::from_str(r#"{"type":"data_change","table":["a.b","c.d"]}"#).expect("many");
        match many {
            TriggerSpec::DataChange { table } => {
                assert_eq!(table.tables(), vec!["a.b".to_owned(), "c.d".to_owned()])
            }
            other => panic!("expected data_change, got {other:?}"),
        }
    }

    /// The env → cron event mapping mirrors the TS `endpoint-run` harness: the
    /// interval env vars land on the cron trigger event.
    #[test]
    fn cron_event_from_env() {
        let vars = |k: &str| -> Option<String> {
            match k {
                ENV_TRIGGER => Some("cron".to_owned()),
                ENV_LOGICAL_DATE => Some("2026-08-01T00:00:00Z".to_owned()),
                ENV_INTERVAL_START => Some("2026-07-31T00:00:00Z".to_owned()),
                ENV_INTERVAL_END => Some("2026-08-01T00:00:00Z".to_owned()),
                _ => None,
            }
        };
        let event = TriggerEvent::from_env(vars);
        assert_eq!(
            event,
            TriggerEvent::Cron(CronInterval {
                logical_date: Some("2026-08-01T00:00:00Z".to_owned()),
                interval_start: Some("2026-07-31T00:00:00Z".to_owned()),
                interval_end: Some("2026-08-01T00:00:00Z".to_owned()),
            })
        );
        assert_eq!(event.kind(), "cron");
    }

    /// A webhook `VERGLAS_TRIGGER` maps to the webhook event, not cron.
    #[test]
    fn webhook_event_from_env() {
        let vars = |k: &str| -> Option<String> { (k == ENV_TRIGGER).then(|| "webhook".to_owned()) };
        assert_eq!(TriggerEvent::from_env(vars), TriggerEvent::Webhook);
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
