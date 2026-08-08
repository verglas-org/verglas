//! The worker executor: run one worker as a subprocess, env-in / result-file-out.
//!
//! This is the run loop the server drives for every deployment now that Source /
//! Sink / MV are gone. It mirrors the TypeScript SDK's `endpoint-run` harness:
//! the parent sets the run's environment ([`verglas_sdk::worker`] `VERGLAS_*`
//! bindings, `DEPLOYMENT`, `TARGET`, the endpoint and token), spawns the child,
//! waits for it, and reads the child's [`RunResult`] JSON back from the path it
//! named in `RESULT_PATH`. There is no framed stdio protocol on the worker side.
//!
//! Run observability is not owned here — catalog-side lakekeeping records
//! pipeline telemetry; this executor only runs the subprocess and returns its
//! outcome.

use std::collections::BTreeMap;
use uuid::Uuid;

use verglas_sdk::worker::{
    CloudEvent, ENV_CLOUD_EVENT, ENV_DEPLOYMENT, ENV_ENDPOINT, ENV_RESULT_PATH, ENV_TARGET,
    ENV_TOKEN, RunResult,
};

use crate::commit::HarnessError;

/// How a worker deployment is launched: the interpreter or binary plus its args
/// and working directory. Parsed from the worker row's `code`/`config` JSON.
#[derive(Debug, Clone)]
pub struct WorkerExec {
    /// The command to run (an interpreter like `bun`, or a binary).
    pub command: String,
    /// The command's arguments (the shim and module for a TS worker).
    pub args: Vec<String>,
    /// The working directory, if the worker needs one.
    pub cwd: Option<String>,
    /// Plain deployment environment bindings injected before reserved runtime
    /// bindings such as the endpoint and CloudEvent.
    pub env: BTreeMap<String, String>,
}

impl WorkerExec {
    /// Parses a worker exec spec from its `code` JSON:
    /// `{"exec":["bun","shim.ts"],"cwd":"/tmp"}`. Element 0 is the command, the
    /// rest are its arguments. An empty or missing `exec` is an error — the
    /// server can only run subprocess workers.
    pub fn from_config(name: &str, config: &str) -> Result<WorkerExec, HarnessError> {
        let value: serde_json::Value = serde_json::from_str(config)
            .map_err(|e| HarnessError::Job(format!("worker {name} config is not JSON: {e}")))?;
        let cwd = value.get("cwd").and_then(|v| v.as_str()).map(str::to_owned);
        let exec = value
            .get("exec")
            .and_then(|v| v.as_array())
            .ok_or_else(|| {
                HarnessError::Job(format!(
                    "worker {name} has no `exec` array in its config; the server can only run \
                 subprocess workers"
                ))
            })?;
        let mut parts = exec.iter().filter_map(|v| v.as_str().map(str::to_owned));
        let command = parts.next().ok_or_else(|| {
            HarnessError::Job(format!(
                "worker {name} has an empty `exec` array; the server can only run \
                 subprocess workers"
            ))
        })?;
        Ok(WorkerExec {
            command,
            args: parts.collect(),
            cwd,
            env: BTreeMap::new(),
        })
    }
}

/// One worker run to execute.
pub struct WorkerRun<'a> {
    /// The deployment name — used for the guard window and error messages.
    pub deployment: &'a str,
    /// The deployment-configured output table (bound as `TARGET`).
    pub output: &'a str,
    /// The data-plane endpoint the worker's client connects to.
    pub endpoint: &'a str,
    /// The bearer token the worker authenticates with (empty locally).
    pub token: &'a str,
}

/// What one worker run reported.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerOutcome {
    /// Rows the worker committed to its output table this run.
    pub rows_produced: u64,
}

/// Runs one worker subprocess for `event`, resolving the trigger into the run's
/// environment and reading the child's [`RunResult`] back.
///
/// The child writes `{"rows": n, "error": null}` on success and exits 0, or
/// `{"rows": 0, "error": "<message>"}` and exits 1 on failure; either an
/// error result or a non-zero exit is a [`HarnessError::Job`].
pub async fn run_worker(
    run: &WorkerRun<'_>,
    exec: &WorkerExec,
    event: &CloudEvent,
) -> Result<WorkerOutcome, HarnessError> {
    drive(run, exec, event).await
}

/// Spawns the child with the resolved environment, waits for it, and reads its
/// result file. The result path is a fresh temp file the parent owns and removes.
async fn drive(
    run: &WorkerRun<'_>,
    exec: &WorkerExec,
    event: &CloudEvent,
) -> Result<WorkerOutcome, HarnessError> {
    let result_path = std::env::temp_dir().join(format!("verglas-worker-{}.json", Uuid::new_v4()));

    let mut cmd = tokio::process::Command::new(&exec.command);
    cmd.args(&exec.args);
    if let Some(cwd) = &exec.cwd {
        cmd.current_dir(cwd);
    }
    cmd.envs(&exec.env);
    for (key, value) in run_env(run, event, &result_path)? {
        cmd.env(key, value);
    }

    let status = cmd
        .status()
        .await
        .map_err(|e| HarnessError::Job(format!("spawn worker {}: {e}", run.deployment)))?;

    let outcome = read_result(&result_path, run.deployment, status);
    let _ = std::fs::remove_file(&result_path);
    outcome
}

/// The run's environment: the trigger binding plus the deployment, target,
/// endpoint, token, and result path. Mirrors the TS `endpoint-run` env contract.
fn run_env(
    run: &WorkerRun<'_>,
    event: &CloudEvent,
    result_path: &std::path::Path,
) -> Result<Vec<(String, String)>, HarnessError> {
    let event_json = serde_json::to_string(event)
        .map_err(|error| HarnessError::Job(format!("serialize worker event: {error}")))?;
    event
        .validate()
        .map_err(|error| HarnessError::Job(format!("invalid worker CloudEvent: {error}")))?;
    let env = vec![
        (ENV_DEPLOYMENT.to_owned(), run.deployment.to_owned()),
        (ENV_TARGET.to_owned(), run.output.to_owned()),
        (ENV_ENDPOINT.to_owned(), run.endpoint.to_owned()),
        (ENV_TOKEN.to_owned(), run.token.to_owned()),
        (
            ENV_RESULT_PATH.to_owned(),
            result_path.to_string_lossy().into_owned(),
        ),
        (ENV_CLOUD_EVENT.to_owned(), event_json),
    ];
    Ok(env)
}

/// Interprets the child's result file and exit status. A result-file error, a
/// missing file, or a non-zero exit is a run failure; otherwise the reported row
/// count is the outcome.
fn read_result(
    result_path: &std::path::Path,
    deployment: &str,
    status: std::process::ExitStatus,
) -> Result<WorkerOutcome, HarnessError> {
    let parsed = std::fs::read_to_string(result_path)
        .ok()
        .and_then(|body| serde_json::from_str::<RunResult>(&body).ok());
    match parsed {
        Some(RunResult {
            error: Some(message),
            ..
        }) => Err(HarnessError::Job(format!("worker {deployment}: {message}"))),
        Some(RunResult { rows, error: None }) if status.success() => Ok(WorkerOutcome {
            rows_produced: rows,
        }),
        Some(RunResult { .. }) => Err(HarnessError::Job(format!(
            "worker {deployment} exited {status} despite an ok result"
        ))),
        None if status.success() => Ok(WorkerOutcome { rows_produced: 0 }),
        None => Err(HarnessError::Job(format!(
            "worker {deployment} exited {status} and wrote no result"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// A worker with no `exec` is rejected — the server only runs subprocess
    /// workers.
    #[test]
    fn exec_requires_exec_array() {
        assert!(WorkerExec::from_config("w", "{}").is_err());
        assert!(
            WorkerExec::from_config("w", r#"{"command":"bun","args":["shim.ts"]}"#).is_err(),
            "legacy command/args shape is refused"
        );
    }

    /// The unified `exec` array parses element 0 as the command and the rest as
    /// its args, so one portable spec round-trips with the worker launch contract.
    #[test]
    fn exec_parses_the_unified_exec_array() {
        let exec = WorkerExec::from_config(
            "w",
            r#"{"exec":["python3","collect.py","--once"],"cwd":"/app"}"#,
        )
        .expect("parse");
        assert_eq!(exec.command, "python3");
        assert_eq!(
            exec.args,
            vec!["collect.py".to_owned(), "--once".to_owned()]
        );
        assert_eq!(exec.cwd.as_deref(), Some("/app"));
    }

    /// An empty `exec` array is rejected — there is no command to run.
    #[test]
    fn exec_rejects_an_empty_exec_array() {
        assert!(WorkerExec::from_config("w", r#"{"exec":[]}"#).is_err());
    }

    /// Every run crosses the subprocess boundary as one structured CloudEvent.
    #[test]
    fn env_carries_one_cloud_event() {
        let run = WorkerRun {
            deployment: "d",
            output: "a.b",
            endpoint: "http://127.0.0.1:8334",
            token: "",
        };
        let mut event = CloudEvent::new("tick-1", "urn:verglas:scheduler", "org.verglas.cron");
        event.data = Some(serde_json::json!({
            "logicalDate": "2026-08-01T00:00:00Z",
            "intervalStart": "2026-07-31T00:00:00Z",
            "intervalEnd": "2026-08-01T00:00:00Z"
        }));
        let env: HashMap<String, String> =
            run_env(&run, &event, std::path::Path::new("/tmp/r.json"))
                .expect("run env")
                .into_iter()
                .collect();
        assert_eq!(env.get(ENV_TARGET).map(String::as_str), Some("a.b"));
        let serialized = env.get(ENV_CLOUD_EVENT).expect("serialized event");
        assert_eq!(
            serde_json::from_str::<CloudEvent>(serialized).expect("event"),
            event
        );
    }
}
