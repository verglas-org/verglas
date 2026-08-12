//! `verglas workers` — local worker registry against the server admin API.
//!
//! Every verb targets `--server-endpoint` / `VERGLAS_ENDPOINT` at `/v1/workers`.
//! Create registers a portable spec; list/get read the registry; run dispatches
//! one manual invocation; follow streams a local process or file into a table;
//! delete archives the worker. There is no update route on the local registry
//! beyond re-register (create again) or a state transition (delete archives).

use std::error::Error;
use std::fmt::Write as _;
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::Value;

use crate::cli::{WorkerCreateArgs, WorkerFollowArgs, WorkerRefArgs, WorkersCommand};
use crate::worker_spec::WorkerManifest;

/// The placeholder a missing/empty field renders as.
const DASH: &str = "-";

/// Dispatches `verglas workers` against the local server at `endpoint`.
pub async fn run(
    command: WorkersCommand,
    endpoint: &str,
    token: Option<&str>,
    json: bool,
) -> Result<(), Box<dyn Error>> {
    match command {
        WorkersCommand::List => {
            let server = crate::backend::server(endpoint, token)?;
            let rows: Value = server.get("/v1/workers").await?;
            emit_list(&rows, &["name", "state", "output", "created_by"], json)
        }
        WorkersCommand::Get(WorkerRefArgs { worker }) => {
            let server = crate::backend::server(endpoint, token)?;
            let detail: Value = server.get(&format!("/v1/workers/{worker}")).await?;
            emit_object(&detail, json)
        }
        WorkersCommand::Create(args) => run_create(endpoint, token, args, json).await,
        WorkersCommand::Delete(WorkerRefArgs { worker }) => {
            let server = crate::backend::server(endpoint, token)?;
            let response: Value = server
                .put_json(
                    &format!("/v1/workers/{worker}/state"),
                    &serde_json::json!({ "state": "archived" }),
                )
                .await?;
            if json {
                println!("{}", serde_json::to_string_pretty(&response)?);
            } else {
                println!("archived worker {worker}");
            }
            Ok(())
        }
        WorkersCommand::Run(WorkerRefArgs { worker }) => {
            run_now(endpoint, token, &worker, json).await
        }
        WorkersCommand::Follow(args) => run_follow(endpoint, token, args).await,
    }
}

/// Registers a worker from a portable spec file (`POST /v1/workers`).
async fn run_create(
    endpoint: &str,
    token: Option<&str>,
    args: WorkerCreateArgs,
    json: bool,
) -> Result<(), Box<dyn Error>> {
    let mut manifest = WorkerManifest::from_file(&args.file)?;
    apply_create_overrides(&mut manifest, args.name, args.schedule);
    manifest.validate()?;
    let server = crate::backend::server(endpoint, token)?;
    let row: Value = server
        .post_json("/v1/workers", &manifest.to_local_worker())
        .await?;
    emit_object(&row, json)
}

/// Applies the `--name`/`--schedule` overrides to a portable spec.
fn apply_create_overrides(
    manifest: &mut WorkerManifest,
    name: Option<String>,
    schedule: Option<String>,
) {
    if let Some(name) = name {
        manifest.name = name;
    }
    if let Some(schedule) = schedule {
        if let Some(crate::worker_spec::Trigger::Cron { cron, .. }) = manifest
            .triggers
            .iter_mut()
            .find(|trigger| matches!(trigger, crate::worker_spec::Trigger::Cron { .. }))
        {
            *cron = schedule;
        } else {
            manifest.triggers.push(crate::worker_spec::Trigger::Cron {
                cron: schedule,
                start_date: None,
                catchup: None,
            });
        }
    }
}

/// Dispatches a manual run (`POST /v1/workers/{name}/run`) with a fresh
/// Idempotency-Key so the server accepts the request.
async fn run_now(
    endpoint: &str,
    token: Option<&str>,
    worker: &str,
    json: bool,
) -> Result<(), Box<dyn Error>> {
    let base = endpoint.trim_end_matches('/');
    let url = format!("{base}/v1/workers/{worker}/run");
    let key = format!("cli-{}", short_id());
    let mut request = reqwest::Client::new()
        .post(&url)
        .header("Idempotency-Key", &key);
    if let Some(token) = token {
        request = request.bearer_auth(token);
    }
    let response = request
        .send()
        .await
        .map_err(|e| format!("could not reach server at {base}: {e}"))?;
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    if !status.is_success() {
        return Err(format!("server: {body} (HTTP {})", status.as_u16()).into());
    }
    let value: Value = if body.trim().is_empty() {
        Value::Null
    } else {
        serde_json::from_str(&body).map_err(|e| format!("failed to decode run response: {e}"))?
    };
    emit_object(&value, json)
}

/// Registers a throwaway follow worker and streams until Ctrl-C (or the wrapped
/// command finishes). Torn down on exit unless `--keep` is set.
async fn run_follow(
    endpoint: &str,
    token: Option<&str>,
    args: WorkerFollowArgs,
) -> Result<(), Box<dyn Error>> {
    let name = args
        .name
        .unwrap_or_else(|| format!("follow-{}", short_id()));
    let table = args
        .table
        .unwrap_or_else(|| format!("{}.{}", crate::worker_spec::FOLLOW_NAMESPACE, name));
    let trigger = match &args.file {
        Some(path) => crate::worker_spec::Trigger::Follow {
            file: Some(path.to_string_lossy().into_owned()),
        },
        None => {
            if args.command.is_empty() {
                return Err(
                    "give a command after `--`, or tail a file with `--file <path>`".into(),
                );
            }
            crate::worker_spec::Trigger::Follow { file: None }
        }
    };
    let manifest = WorkerManifest {
        spec_version: crate::worker_spec::SPEC_VERSION,
        name: name.clone(),
        runtime: crate::worker_spec::WorkerRuntime::Container,
        entrypoint: args.command,
        files: std::collections::BTreeMap::from([(
            "Dockerfile".to_owned(),
            "FROM alpine:3.22\nWORKDIR /app\n".to_owned(),
        )]),
        env: Default::default(),
        triggers: vec![trigger],
        target_tables: vec![table.clone()],
        scratch_target: None,
        resources: Default::default(),
    };
    manifest.validate()?;

    let server = crate::backend::server(endpoint, token)?;
    let _row: Value = server
        .post_json("/v1/workers", &manifest.to_local_worker())
        .await?;
    println!("following -> {table} (worker {name}); press Ctrl-C to stop");

    tokio::select! {
        _ = tokio::signal::ctrl_c() => println!("stopping {name}"),
        _ = wait_until_finished(&server, &name) => println!("worker {name} finished"),
    }

    if args.keep {
        println!("worker {name} left registered (--keep)");
    } else {
        let _: Value = server
            .put_json(
                &format!("/v1/workers/{name}/state"),
                &serde_json::json!({ "state": "archived" }),
            )
            .await?;
        println!("torn down worker {name}");
    }
    Ok(())
}

/// Polls the local worker until it leaves `running`/`created`. Transient read
/// errors are ignored so a blip never ends the follow early.
async fn wait_until_finished(server: &verglas_sdk::server::ServerClient, name: &str) {
    loop {
        tokio::time::sleep(std::time::Duration::from_millis(750)).await;
        if let Ok(row) = server.get::<Value>(&format!("/v1/workers/{name}")).await {
            let state = row
                .get("state")
                .and_then(Value::as_str)
                .unwrap_or("running");
            if state != "running" && state != "created" {
                return;
            }
        }
    }
}

/// A short hex id for throwaway names and run idempotency keys.
fn short_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default();
    format!("{:x}", nanos & 0xffff_ffff)
}

/// Renders one resource object as FIELD/VALUE (human) or pretty JSON.
fn emit_object(value: &Value, json: bool) -> Result<(), Box<dyn Error>> {
    if json {
        println!("{}", serde_json::to_string_pretty(value)?);
        return Ok(());
    }
    let Some(object) = value.as_object() else {
        println!("{value}");
        return Ok(());
    };
    let rows: Vec<(String, String)> = object
        .iter()
        .map(|(k, v)| {
            let rendered = match v {
                Value::String(s) => s.clone(),
                Value::Null => DASH.to_owned(),
                other => other.to_string(),
            };
            (k.clone(), rendered)
        })
        .collect();
    let borrowed: Vec<(&str, String)> = rows.iter().map(|(k, v)| (k.as_str(), v.clone())).collect();
    crate::output::print_key_value_table(("FIELD", "VALUE"), &borrowed, false)?;
    Ok(())
}

/// Emits a list response: the raw JSON (`--json`) or a fixed-width table.
fn emit_list(response: &Value, columns: &[&str], json: bool) -> Result<(), Box<dyn Error>> {
    if json {
        println!("{}", serde_json::to_string_pretty(response)?);
        return Ok(());
    }
    let rows = rows_of(response);
    println!("{}", render_table(columns, &rows, "(no workers)"));
    Ok(())
}

/// Pulls the array of rows out of a list response (bare array or `{workers:[...]}`).
fn rows_of(response: &Value) -> Vec<Value> {
    if let Value::Array(items) = response {
        return items.clone();
    }
    match response.get("workers") {
        Some(Value::Array(items)) => items.clone(),
        _ => Vec::new(),
    }
}

/// A single field of a JSON row for a human table.
fn field(row: &Value, key: &str) -> String {
    match row.get(key) {
        None | Some(Value::Null) => DASH.to_owned(),
        Some(Value::String(s)) if s.is_empty() => DASH.to_owned(),
        Some(Value::String(s)) => s.clone(),
        Some(other) => other.to_string(),
    }
}

/// Renders a fixed-width table of `columns` over `rows`.
fn render_table(columns: &[&str], rows: &[Value], placeholder: &str) -> String {
    if rows.is_empty() {
        return placeholder.to_owned();
    }
    let cells: Vec<Vec<String>> = rows
        .iter()
        .map(|r| columns.iter().map(|c| field(r, c)).collect())
        .collect();
    let widths: Vec<usize> = columns
        .iter()
        .enumerate()
        .map(|(i, header)| {
            cells
                .iter()
                .map(|row| row[i].chars().count())
                .chain(std::iter::once(header.chars().count()))
                .max()
                .unwrap_or(0)
        })
        .collect();
    let mut out = String::new();
    for (i, header) in columns.iter().enumerate() {
        let _ = write!(out, "{:<width$}", header.to_uppercase(), width = widths[i]);
        if i + 1 < columns.len() {
            out.push_str("  ");
        }
    }
    for row in &cells {
        out.push('\n');
        for (i, cell) in row.iter().enumerate() {
            let _ = write!(out, "{:<width$}", cell, width = widths[i]);
            if i + 1 < row.len() {
                out.push_str("  ");
            }
        }
    }
    out
}
