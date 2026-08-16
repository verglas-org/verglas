//! `verglas workers` — Verglas Cloud worker registry.
//!
//! Every verb targets Verglas Cloud at `/v1/workers`. The OSS stack does not
//! host workers; pointing `VERGLAS_ENDPOINT` at a local server is an error.

use std::error::Error;
use std::fmt::Write as _;
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::Value;

use crate::cli::{WorkerCreateArgs, WorkerRefArgs, WorkersCommand};
use crate::worker_spec::WorkerManifest;

/// The placeholder a missing/empty field renders as.
const DASH: &str = "-";

/// Dispatches `verglas workers` against Verglas Cloud.
pub async fn run(
    command: WorkersCommand,
    endpoint: &str,
    token: Option<&str>,
    json: bool,
) -> Result<(), Box<dyn Error>> {
    if let WorkersCommand::Follow(_) = command {
        return Err(
            "verglas workers follow is not supported; create a Cloud worker with `verglas workers create --file`".into(),
        );
    }
    crate::backend::require_cloud_workers(endpoint)?;
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
        WorkersCommand::Follow(_) => unreachable!("follow rejected above"),
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
