//! `verglas workers | containers | tables | db | volumes | secrets` — the cloud
//! resource command groups.
//!
//! These are the CLI's control-plane surface: unlike the data-plane verbs (which
//! talk to a daemon) they target the multi-tenant control plane resolved from a
//! stored `verglas login`. Every group needs a login; with none stored the verbs
//! fail with the same "run `verglas login`" pointer the rest of the control-plane
//! surface uses, never a cryptic transport error.
//!
//! Contracts (control plane, some routes shipped by parallel efforts):
//!
//! - workers = the deployments API: GET/POST /v1/deployments, GET/PATCH/DELETE
//!   /v1/deployments/:id, POST /v1/deployments/:id/run.
//! - containers = /v1/containers CRUD + /:id/scale|stop|resume, the curated
//!   catalog (GET /v1/containers/catalog, POST /v1/containers/catalog/:id/deploy),
//!   and per-container config (GET/PUT /v1/containers/:id/config). The catalog verbs
//!   make the CLI the first-class way to deploy the curated apps: `catalog` lists
//!   them, `deploy` deploys one idempotently, and `config` shows/sets a curated
//!   container's tenant config (secrets write-only, never printed).
//! - tables = GET /v1/tables/:name/snapshot; `list` derives the table set from
//!   the deployments' target tables (no dedicated list route).
//! - db = GET/POST /v1/dbs, DELETE /v1/dbs/:name.
//! - secrets = GET/POST /v1/secrets, DELETE /v1/secrets/:name. GET returns names
//!   only; a stored value is never returned by the control plane nor printed here.
//!
//! A route an older control plane has not shipped yet answers 404/405/501; these
//! verbs turn that into a clear "the control plane does not support this yet"
//! error rather than a panic or a bare HTTP status. `workers logs` has no route
//! on the control plane, so it errors honestly.
//!
//! Output follows the CLI's convention: human tables by default, `--json` for the
//! server's raw JSON, and a non-zero exit carrying the server's own error message
//! on any API failure.

use std::error::Error;
use std::path::Path;

use serde_json::Value;

use crate::cli::{
    ContainerConfigArgs, ContainerCreateArgs, ContainerDeployArgs, ContainerPushArgs,
    ContainerScaleArgs, ContainerUpdateArgs, ContainersCommand, DbCommand, DbCreateArgs,
    DbNameArgs, SecretNameArgs, SecretSetArgs, SecretsCommand, VolumeCreateArgs, VolumeNameArgs,
    VolumeResizeArgs, VolumesCommand, WorkerCreateArgs, WorkerFollowArgs, WorkerPullArgs,
    WorkerPushArgs, WorkerRefArgs, WorkerUpdateArgs, WorkersCommand,
};
use crate::controlplane::{ControlPlaneClient, ControlPlaneError};
use crate::worker_spec::WorkerManifest;

/// The placeholder a missing/empty field renders as, matching the platform verbs.
const DASH: &str = "-";

/// Resolves the control-plane client from the stored login, surfacing the
/// "run `verglas login`" pointer when none is stored (these verbs REQUIRE a
/// login, so `NotLoggedIn` is an error here, not an empty result).
fn client() -> Result<ControlPlaneClient, Box<dyn Error>> {
    Ok(crate::backend::control_plane()?)
}

/// Turns a control-plane error into a clear "not supported yet" message when it
/// looks like the ROUTE is absent (a 404/405/501 from a server older than this
/// CLI), so a resource group whose route a parallel effort has not shipped fails
/// honestly. Every other error surfaces verbatim (carrying the server's message).
fn map_unsupported(feature: &str, error: ControlPlaneError) -> Box<dyn Error> {
    if error.route_absent() {
        format!(
            "the control plane does not support {feature} yet — your server may be older than this CLI ({error})"
        )
        .into()
    } else {
        error.into()
    }
}

/// Reads a worker/container spec file and parses it into a JSON object. `.toml`
/// is parsed as TOML, everything else as JSON. A spec that is not an object is a
/// clear error (the request body is always an object).
fn read_spec(path: &Path) -> Result<Value, Box<dyn Error>> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| format!("could not read spec file {}: {e}", path.display()))?;
    let value: Value = if path.extension().and_then(|e| e.to_str()) == Some("toml") {
        let parsed: toml::Value = toml::from_str(&text)
            .map_err(|e| format!("{} is not valid TOML: {e}", path.display()))?;
        serde_json::to_value(parsed)
            .map_err(|e| format!("{} could not convert to JSON: {e}", path.display()))?
    } else {
        serde_json::from_str(&text)
            .map_err(|e| format!("{} is not valid JSON: {e}", path.display()))?
    };
    if !value.is_object() {
        return Err(format!("the spec in {} must be a JSON/TOML object", path.display()).into());
    }
    Ok(value)
}

/// A single field of a JSON row, rendered for a human table: a string verbatim,
/// an array of strings joined by commas, null/absent as a dash, and anything else
/// as compact JSON.
fn field(row: &Value, key: &str) -> String {
    match row.get(key) {
        None | Some(Value::Null) => DASH.to_owned(),
        Some(Value::String(s)) if s.is_empty() => DASH.to_owned(),
        Some(Value::String(s)) => s.clone(),
        Some(Value::Array(items)) => {
            let joined: Vec<String> = items
                .iter()
                .map(|i| match i {
                    Value::String(s) => s.clone(),
                    other => other.to_string(),
                })
                .collect();
            if joined.is_empty() {
                DASH.to_owned()
            } else {
                joined.join(",")
            }
        }
        Some(other) => other.to_string(),
    }
}

/// Renders a fixed-width table of `columns` over `rows`. `placeholder` is printed
/// (instead of a header row) when there are no rows, so an empty listing reads
/// clearly rather than as a bare header.
fn render_table(columns: &[&str], rows: &[Value], placeholder: &str) -> String {
    use std::fmt::Write as _;
    if rows.is_empty() {
        return placeholder.to_owned();
    }
    // Column widths fit the header and every cell.
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

/// Pulls the array of rows out of a list response: a top-level JSON array, or the
/// array under `key` (the control plane wraps lists, e.g. `{deployments:[...]}`),
/// or an empty list when neither is present.
fn rows_of(response: &Value, key: &str) -> Vec<Value> {
    if let Value::Array(items) = response {
        return items.clone();
    }
    match response.get(key) {
        Some(Value::Array(items)) => items.clone(),
        _ => Vec::new(),
    }
}

/// Renders one resource object as a two-column FIELD/VALUE table (human) or the
/// raw pretty JSON (`--json`). A scalar field prints verbatim; a nested object or
/// array prints as compact JSON so the whole record is still visible.
fn emit_object(value: &Value, json: bool) -> Result<(), Box<dyn Error>> {
    if json {
        println!("{}", serde_json::to_string_pretty(value)?);
        return Ok(());
    }
    let Some(object) = value.as_object() else {
        // A non-object success body (e.g. a bare string/number) prints as-is.
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

/// Emits a list response: the raw JSON (`--json`) or a fixed-width table over
/// `columns` (human), keyed by the wrapper `key`.
fn emit_list(
    response: &Value,
    key: &str,
    columns: &[&str],
    placeholder: &str,
    json: bool,
) -> Result<(), Box<dyn Error>> {
    if json {
        println!("{}", serde_json::to_string_pretty(response)?);
        return Ok(());
    }
    let rows = rows_of(response, key);
    println!("{}", render_table(columns, &rows, placeholder));
    Ok(())
}

// --- workers ---------------------------------------------------------------

/// Fetches one worker's detail, accepting either its control-plane id or its
/// name. Tries the id path first; on a 404 it resolves the name through the
/// deployments list and fetches by the resolved id.
async fn worker_detail(
    client: &ControlPlaneClient,
    reference: &str,
) -> Result<Value, Box<dyn Error>> {
    match client
        .get_value(&format!("/v1/deployments/{reference}"))
        .await
    {
        Ok(value) => Ok(value),
        Err(ControlPlaneError::NotFound | ControlPlaneError::Api { status: 404, .. }) => {
            let id = resolve_worker_name(client, reference).await?;
            Ok(client.get_value(&format!("/v1/deployments/{id}")).await?)
        }
        Err(other) => Err(other.into()),
    }
}

/// Resolves a worker id or name to its id: an id that the list contains is
/// returned as-is, otherwise the name is matched. A reference that is neither is
/// a clear error.
async fn resolve_worker_id(
    client: &ControlPlaneClient,
    reference: &str,
) -> Result<String, Box<dyn Error>> {
    let deployments = client.deployments().await?;
    if let Some(found) = deployments
        .iter()
        .find(|d| d.id == reference || d.name == reference)
    {
        Ok(found.id.clone())
    } else {
        Err(format!("no worker with id or name `{reference}`").into())
    }
}

/// Resolves a worker NAME to its id through the deployments list.
async fn resolve_worker_name(
    client: &ControlPlaneClient,
    name: &str,
) -> Result<String, Box<dyn Error>> {
    let deployments = client.deployments().await?;
    deployments
        .into_iter()
        .find(|d| d.name == name)
        .map(|d| d.id)
        .ok_or_else(|| format!("no worker with id or name `{name}`").into())
}

/// Dispatches `verglas workers`. Most verbs target the cloud control plane;
/// `create --local` and `follow` target the local daemon at `endpoint`, and
/// `push`/`pull` bridge the two.
pub async fn run_workers(
    command: WorkersCommand,
    endpoint: &str,
    json: bool,
) -> Result<(), Box<dyn Error>> {
    match command {
        WorkersCommand::List => {
            let client = client()?;
            let response = client.get_value("/v1/deployments").await?;
            emit_list(
                &response,
                "deployments",
                &[
                    "id",
                    "name",
                    "kind",
                    "trigger",
                    "placement",
                    "status",
                    "schedule",
                ],
                "(no workers)",
                json,
            )
        }
        WorkersCommand::Get(WorkerRefArgs { worker }) => {
            let client = client()?;
            let detail = worker_detail(&client, &worker).await?;
            emit_object(&detail, json)
        }
        WorkersCommand::Create(args) => run_worker_create(endpoint, args, json).await,
        WorkersCommand::Update(args) => {
            let client = client()?;
            run_worker_update(&client, args, json).await
        }
        WorkersCommand::Delete(WorkerRefArgs { worker }) => {
            let client = client()?;
            let id = resolve_worker_id(&client, &worker).await?;
            let response = client
                .delete_value(&format!("/v1/deployments/{id}"))
                .await?;
            if json {
                println!("{}", serde_json::to_string_pretty(&response)?);
            } else {
                println!("deleted worker {worker}");
            }
            Ok(())
        }
        WorkersCommand::Run(WorkerRefArgs { worker }) => {
            let client = client()?;
            let id = resolve_worker_id(&client, &worker).await?;
            let response = client
                .post_value(&format!("/v1/deployments/{id}/run"), &Value::Null)
                .await?;
            emit_object(&response, json)
        }
        WorkersCommand::Logs(WorkerRefArgs { worker }) => {
            // The control plane exposes no per-worker logs route yet: fail
            // honestly rather than pretending to tail nothing.
            let _ = worker;
            Err(
                "`verglas workers logs` is not available: the control plane exposes no \
                 per-worker logs route yet"
                    .into(),
            )
        }
        WorkersCommand::Follow(args) => run_worker_follow(endpoint, args).await,
        WorkersCommand::Push(args) => run_worker_push(endpoint, args, json).await,
        WorkersCommand::Pull(args) => run_worker_pull(args, json).await,
    }
}

/// `workers create`: register a worker from a unified portable spec file. With
/// `--local` the worker is registered on the local daemon (`POST /v1/workers`);
/// otherwise on the cloud (`POST /v1/deployments`). The same file drives both.
async fn run_worker_create(
    endpoint: &str,
    args: WorkerCreateArgs,
    json: bool,
) -> Result<(), Box<dyn Error>> {
    let mut manifest = WorkerManifest::from_file(&args.file)?;
    apply_create_overrides(&mut manifest, args.name, args.schedule);
    manifest.validate()?;

    if args.local {
        let daemon = crate::backend::daemon(endpoint)?;
        let row: Value = daemon
            .post_json("/v1/workers", &manifest.to_local_worker())
            .await?;
        return emit_object(&row, json);
    }

    if manifest.is_follow() {
        return Err(
            "a follow worker is local-only; create it with `--local`, or use \
             `verglas workers follow`"
                .into(),
        );
    }
    let client = client()?;
    let response = client
        .post_value("/v1/deployments", &manifest.to_cloud_deployment("cloud"))
        .await?;
    report_missing_secrets(&client, &manifest).await;
    emit_object(&response, json)
}

/// Applies the `--name`/`--schedule` overrides to a unified spec.
fn apply_create_overrides(
    manifest: &mut WorkerManifest,
    name: Option<String>,
    schedule: Option<String>,
) {
    if let Some(name) = name {
        manifest.name = name;
    }
    if let Some(schedule) = schedule {
        manifest.trigger = crate::worker_spec::Trigger::Cron { cron: schedule };
    }
}

/// `workers update`: build the PATCH body from the spec file (if any) plus the
/// `--schedule`/`--status` overrides, then PATCH the resolved deployment. An
/// update with neither a spec nor an override is a clear error rather than a
/// no-op request.
async fn run_worker_update(
    client: &ControlPlaneClient,
    args: WorkerUpdateArgs,
    json: bool,
) -> Result<(), Box<dyn Error>> {
    let mut body = match &args.file {
        Some(path) => read_spec(path)?,
        None => Value::Object(serde_json::Map::new()),
    };
    if let Some(schedule) = args.schedule {
        body["schedule"] = Value::String(schedule);
    }
    if let Some(status) = args.status {
        body["status"] = Value::String(status);
    }
    if body.as_object().is_none_or(|o| o.is_empty()) {
        return Err("nothing to update: pass --file, --schedule, and/or --status".into());
    }
    let id = resolve_worker_id(client, &args.worker).await?;
    let response = client
        .patch_value(&format!("/v1/deployments/{id}"), &body)
        .await?;
    emit_object(&response, json)
}

/// `workers follow`: register a throwaway follow worker on the local daemon and
/// stream a local process or file into a table until Ctrl-C (or, for a wrapped
/// command, until it exits). Torn down on exit unless `--keep` is set.
async fn run_worker_follow(endpoint: &str, args: WorkerFollowArgs) -> Result<(), Box<dyn Error>> {
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
    let cwd = std::env::current_dir()
        .ok()
        .map(|p| p.display().to_string());
    let manifest = WorkerManifest {
        spec_version: crate::worker_spec::SPEC_VERSION,
        name: name.clone(),
        exec: args.command,
        cwd,
        files: Default::default(),
        env: Default::default(),
        trigger,
        target_tables: vec![table.clone()],
        resources: Default::default(),
    };
    manifest.validate()?;

    let daemon = crate::backend::daemon(endpoint)?;
    let _row: Value = daemon
        .post_json("/v1/workers", &manifest.to_local_worker())
        .await?;
    println!("following -> {table} (worker {name}); press Ctrl-C to stop");

    // Wait for Ctrl-C, or for a wrapped command to finish on its own. A file tail
    // never finishes; only Ctrl-C ends it.
    tokio::select! {
        _ = tokio::signal::ctrl_c() => println!("stopping {name}"),
        _ = wait_until_finished(&daemon, &name) => println!("worker {name} finished"),
    }

    if args.keep {
        println!("worker {name} left registered (--keep)");
    } else {
        let _: Value = daemon
            .put_json(
                &format!("/v1/workers/{name}/state"),
                &serde_json::json!({ "state": "archived" }),
            )
            .await?;
        println!("torn down worker {name}");
    }
    Ok(())
}

/// Polls the local worker until it leaves the `running` state (a wrapped command
/// that exits is marked `completed` by the daemon). Errors are ignored so a
/// transient read never ends the follow early.
async fn wait_until_finished(daemon: &verglas_sdk::daemon::DaemonClient, name: &str) {
    loop {
        tokio::time::sleep(std::time::Duration::from_millis(750)).await;
        if let Ok(row) = daemon.get::<Value>(&format!("/v1/workers/{name}")).await {
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

/// `workers push`: read a locally-registered worker, translate it to a cloud
/// deployment, and register it. Secrets never ride along — a referenced secret
/// the cloud lacks is reported so it can be set there.
async fn run_worker_push(
    endpoint: &str,
    args: WorkerPushArgs,
    json: bool,
) -> Result<(), Box<dyn Error>> {
    let daemon = crate::backend::daemon(endpoint)?;
    let row: Value = daemon
        .get(&format!("/v1/workers/{}", args.worker))
        .await
        .map_err(|e| match e {
            verglas_sdk::daemon::DaemonError::Api { status: 404, .. } => {
                format!("no local worker named `{}`", args.worker).into()
            }
            other => Box::<dyn Error>::from(other),
        })?;
    let manifest = WorkerManifest::from_local_worker(&row)?;
    if manifest.is_follow() {
        return Err(
            "a follow worker is local-only and cannot be pushed to the cloud (it tails \
             something on this machine)"
                .into(),
        );
    }
    let placement = if args.fleet { "fleet" } else { "cloud" };
    let client = client()?;
    let response = client
        .post_value("/v1/deployments", &manifest.to_cloud_deployment(placement))
        .await?;
    report_missing_secrets(&client, &manifest).await;
    if !json {
        println!("pushed worker {} to the cloud ({placement})", manifest.name);
    }
    emit_object(&response, json)
}

/// `workers pull`: fetch a cloud worker's detail, translate it to a portable
/// spec, and write it to `--file` (TOML) or print it.
async fn run_worker_pull(args: WorkerPullArgs, json: bool) -> Result<(), Box<dyn Error>> {
    let client = client()?;
    let detail = worker_detail(&client, &args.worker).await?;
    let manifest = WorkerManifest::from_cloud_deployment(&detail)?;
    match &args.file {
        Some(path) => {
            std::fs::write(path, manifest.to_toml()?)
                .map_err(|e| format!("could not write {}: {e}", path.display()))?;
            if !json {
                println!("pulled worker {} to {}", manifest.name, path.display());
            }
            Ok(())
        }
        None => {
            if json {
                println!("{}", serde_json::to_string_pretty(&manifest)?);
            } else {
                print!("{}", manifest.to_toml()?);
            }
            Ok(())
        }
    }
}

/// Reports any `@secret:` references the cloud does not yet have set, so a pushed
/// worker's operator knows exactly which secrets to add. Best-effort: a failure
/// to list the cloud's secrets is silent (the push already succeeded). Secret
/// VALUES are never touched — only names.
async fn report_missing_secrets(client: &ControlPlaneClient, manifest: &WorkerManifest) {
    let referenced = manifest.secret_names();
    if referenced.is_empty() {
        return;
    }
    let present: Vec<String> = client
        .get_value("/v1/secrets")
        .await
        .ok()
        .and_then(|v| {
            v.get("secrets").and_then(Value::as_array).map(|a| {
                a.iter()
                    .filter_map(|n| n.as_str().map(str::to_owned))
                    .collect()
            })
        })
        .unwrap_or_default();
    let missing: Vec<&String> = referenced.iter().filter(|n| !present.contains(n)).collect();
    if !missing.is_empty() {
        let names: Vec<&str> = missing.iter().map(|s| s.as_str()).collect();
        // An advisory, not data — to stderr so a `--json` stdout stays pure JSON.
        eprintln!(
            "note: the cloud has no value for these secrets yet — set them with `verglas secrets set <NAME>`: {}",
            names.join(", ")
        );
    }
}

/// A short random id for a throwaway worker name.
fn short_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default();
    format!("{:x}", nanos & 0xffff_ffff)
}

// --- containers ------------------------------------------------------------

/// Dispatches `verglas containers`. Every route is mapped through
/// [`map_unsupported`] so a control plane that has not shipped the containers API
/// yet fails with a clear message rather than a bare 404.
pub async fn run_containers(command: ContainersCommand, json: bool) -> Result<(), Box<dyn Error>> {
    let client = client()?;
    match command {
        ContainersCommand::List => {
            let response = client
                .get_value("/v1/containers")
                .await
                .map_err(|e| map_unsupported("containers", e))?;
            emit_list(
                &response,
                "containers",
                &["id", "name", "image", "mode", "status", "instances"],
                "(no containers)",
                json,
            )
        }
        ContainersCommand::Get(args) => {
            let response = client
                .get_value(&format!("/v1/containers/{}", args.container))
                .await
                .map_err(|e| map_unsupported("containers", e))?;
            emit_object(&response, json)
        }
        ContainersCommand::Create(args) => run_container_create(&client, args, json).await,
        ContainersCommand::Update(ContainerUpdateArgs { container, file }) => {
            let spec = read_spec(&file)?;
            let response = client
                .patch_value(&format!("/v1/containers/{container}"), &spec)
                .await
                .map_err(|e| map_unsupported("containers", e))?;
            emit_object(&response, json)
        }
        ContainersCommand::Delete(args) => {
            let response = client
                .delete_value(&format!("/v1/containers/{}", args.container))
                .await
                .map_err(|e| map_unsupported("containers", e))?;
            if json {
                println!("{}", serde_json::to_string_pretty(&response)?);
            } else {
                println!("deleted container {}", args.container);
            }
            Ok(())
        }
        ContainersCommand::Scale(ContainerScaleArgs {
            container,
            instances,
        }) => {
            let body = serde_json::json!({ "instances": instances });
            let response = client
                .post_value(&format!("/v1/containers/{container}/scale"), &body)
                .await
                .map_err(|e| map_unsupported("containers", e))?;
            emit_object(&response, json)
        }
        ContainersCommand::Stop(args) => {
            let response = client
                .post_value(
                    &format!("/v1/containers/{}/stop", args.container),
                    &Value::Null,
                )
                .await
                .map_err(|e| map_unsupported("containers", e))?;
            emit_object(&response, json)
        }
        ContainersCommand::Resume(args) => {
            let response = client
                .post_value(
                    &format!("/v1/containers/{}/resume", args.container),
                    &Value::Null,
                )
                .await
                .map_err(|e| map_unsupported("containers", e))?;
            emit_object(&response, json)
        }
        ContainersCommand::Catalog => run_catalog_list(&client, json).await,
        ContainersCommand::Deploy(args) => run_catalog_deploy(&client, args, json).await,
        ContainersCommand::Config(args) => run_container_config(&client, args, json).await,
        ContainersCommand::Push(args) => run_container_push(&client, args, json).await,
    }
}

/// `containers push`: register a bring-your-own-image into the tenant registry
/// over the existing custom-image path. The cloud pulls the reference and
/// converts it to a bootable rootfs; local container execution is out of scope.
async fn run_container_push(
    client: &ControlPlaneClient,
    args: ContainerPushArgs,
    json: bool,
) -> Result<(), Box<dyn Error>> {
    let mut body = serde_json::json!({ "image_ref": args.image });
    let name = args.name.unwrap_or_else(|| image_repo_name(&args.image));
    body["name"] = Value::String(name.clone());
    if let Some(tag) = args.tag.or_else(|| image_tag(&args.image)) {
        body["tag"] = Value::String(tag);
    }
    let response = client
        .post_value("/v1/containers", &body)
        .await
        .map_err(|e| map_unsupported("container push", e))?;
    if !json {
        println!(
            "pushed image {} into the tenant registry as `{name}`",
            args.image
        );
    }
    emit_object(&response, json)
}

/// The DNS-label repository name from an image reference, e.g.
/// `docker://ghcr.io/acme/app:1.2` -> `app`.
fn image_repo_name(image: &str) -> String {
    let no_scheme = image.split_once("://").map(|(_, r)| r).unwrap_or(image);
    let path = no_scheme
        .split_once('@')
        .map(|(l, _)| l)
        .unwrap_or(no_scheme);
    let repo = path.rsplit('/').next().unwrap_or(path);
    let name = repo.split_once(':').map(|(l, _)| l).unwrap_or(repo);
    if name.is_empty() {
        "image".to_owned()
    } else {
        name.to_owned()
    }
}

/// The tag from an image reference, if it carries one (`…/app:1.2` -> `1.2`).
fn image_tag(image: &str) -> Option<String> {
    let no_scheme = image.split_once("://").map(|(_, r)| r).unwrap_or(image);
    let repo = no_scheme.rsplit('/').next().unwrap_or(no_scheme);
    repo.split_once(':').map(|(_, tag)| tag.to_owned())
}

/// `containers catalog`: list the curated apps. Human output is a table with the
/// boolean surfaces rendered as yes/no; `--json` is the server's raw response.
async fn run_catalog_list(client: &ControlPlaneClient, json: bool) -> Result<(), Box<dyn Error>> {
    let response = client
        .get_value("/v1/containers/catalog")
        .await
        .map_err(|e| map_unsupported("the container catalog", e))?;
    if json {
        println!("{}", serde_json::to_string_pretty(&response)?);
        return Ok(());
    }
    // Render the booleans as yes/no so the table reads plainly.
    let rows: Vec<Value> = rows_of(&response, "catalog")
        .into_iter()
        .map(|entry| {
            serde_json::json!({
                "id": entry.get("id").cloned().unwrap_or(Value::Null),
                "description": entry.get("description").cloned().unwrap_or(Value::Null),
                "ui": yes_no(&entry, "has_ui"),
                "mcp": yes_no(&entry, "has_mcp"),
                "default": yes_no(&entry, "is_default"),
            })
        })
        .collect();
    println!(
        "{}",
        render_table(
            &["id", "description", "ui", "mcp", "default"],
            &rows,
            "(no catalog apps)"
        )
    );
    Ok(())
}

/// Renders a boolean field as `yes`/`no` (a missing or non-boolean field is `no`).
fn yes_no(row: &Value, key: &str) -> Value {
    Value::String(
        if row.get(key) == Some(&Value::Bool(true)) {
            "yes"
        } else {
            "no"
        }
        .to_owned(),
    )
}

/// `containers deploy <catalog-id>`: deploy a curated app as one of this tenant's
/// containers. Idempotent — an app already deployed returns 200 with `created:false`
/// and is reported as already deployed (still exit 0). Prints the container id, the
/// UI hostname (when the app has a UI), and the MCP endpoint (when declared).
async fn run_catalog_deploy(
    client: &ControlPlaneClient,
    args: ContainerDeployArgs,
    json: bool,
) -> Result<(), Box<dyn Error>> {
    let path = format!("/v1/containers/catalog/{}/deploy", args.catalog_id);
    let response = client
        .post_value(&path, &Value::Null)
        .await
        .map_err(|e| map_unsupported("the container catalog", e))?;
    if json {
        println!("{}", serde_json::to_string_pretty(&response)?);
        return Ok(());
    }
    let created = response.get("created") == Some(&Value::Bool(true));
    let id = response
        .get("id")
        .and_then(Value::as_str)
        .unwrap_or(&args.catalog_id);
    if created {
        println!("deployed {} as container {id}", args.catalog_id);
    } else {
        println!("{} is already deployed as container {id}", args.catalog_id);
    }
    if let Some(hostname) = response.get("hostname").and_then(Value::as_str) {
        println!("UI:  https://{hostname}/");
    }
    if let Some(mcp) = response.get("mcp_endpoint").and_then(Value::as_str) {
        println!("MCP: {mcp}");
    }
    Ok(())
}

/// `containers config <id> [--set KEY=VALUE ...] [--mode M]`: show or set a curated
/// container's config. With no `--set`/`--mode`, GET the schema + current values.
/// Otherwise resolve the values (reading `KEY=-` from stdin so secrets stay out of
/// shell history), pick the mode (explicit `--mode`, else the current one), and PUT.
/// A secret value is never echoed; the read-back never carries secret values.
async fn run_container_config(
    client: &ControlPlaneClient,
    args: ContainerConfigArgs,
    json: bool,
) -> Result<(), Box<dyn Error>> {
    let path = format!("/v1/containers/{}/config", args.container);
    // No writes requested: show the current config.
    if args.set.is_empty() && args.mode.is_none() {
        let response = client
            .get_value(&path)
            .await
            .map_err(|e| map_unsupported("container config", e))?;
        emit_object(&response, json)?;
        return Ok(());
    }

    // Determine which fields are secret so we can warn on --set of a secret and keep
    // the values out of any output. Fetch the schema (also gives the current mode).
    let current = client
        .get_value(&path)
        .await
        .map_err(|e| map_unsupported("container config", e))?;
    if current.get("configurable") == Some(&Value::Bool(false)) {
        return Err(format!("container {} takes no configuration", args.container).into());
    }
    let secret_keys = secret_field_keys(&current);
    let values = resolve_config_values(&args.set, &secret_keys)?;
    let mode = match args.mode {
        Some(mode) => mode,
        None => current
            .get("config")
            .and_then(|c| c.get("mode"))
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or("--mode is required: the container has no current mode to keep")?,
    };

    let body = serde_json::json!({ "mode": mode, "values": values });
    let response = client
        .put_value(&path, &body)
        .await
        .map_err(|e| map_unsupported("container config", e))?;
    emit_object(&response, json)
}

/// The declared secret field keys across every mode of a config-schema response, so
/// the CLI can warn when a secret is passed with `--set` and confirm none are echoed.
fn secret_field_keys(config: &Value) -> std::collections::HashSet<String> {
    let mut keys = std::collections::HashSet::new();
    if let Some(modes) = config.pointer("/schema/modes").and_then(Value::as_array) {
        for mode in modes {
            if let Some(fields) = mode.get("fields").and_then(Value::as_array) {
                for f in fields {
                    if f.get("secret") == Some(&Value::Bool(true))
                        && let Some(k) = f.get("key").and_then(Value::as_str)
                    {
                        keys.insert(k.to_owned());
                    }
                }
            }
        }
    }
    keys
}

/// Resolves `--set KEY=VALUE` pairs into a values map. `KEY=-` reads its value from
/// stdin (one line per `-`, in the order given) so a secret never lands in the shell
/// history; a secret passed inline warns and steers to stdin. Values are never
/// printed. A malformed pair (no `=`) or an empty resolved value is a clear error.
fn resolve_config_values(
    set: &[String],
    secret_keys: &std::collections::HashSet<String>,
) -> Result<serde_json::Map<String, Value>, Box<dyn Error>> {
    use std::io::Read as _;
    // Split each pair; remember which want stdin, preserving order.
    let mut parsed: Vec<(String, Option<String>)> = Vec::new();
    let mut stdin_count = 0usize;
    for pair in set {
        let (key, val) = pair
            .split_once('=')
            .ok_or_else(|| format!("--set expects KEY=VALUE, got `{pair}`"))?;
        if key.is_empty() {
            return Err(format!("--set has an empty key: `{pair}`").into());
        }
        if val == "-" {
            stdin_count += 1;
            parsed.push((key.to_owned(), None));
        } else {
            if secret_keys.contains(key) {
                eprintln!(
                    "warning: passing the secret `{key}` with --set may leave it in your shell \
                     history; pipe it instead with `--set {key}=-`"
                );
            }
            parsed.push((key.to_owned(), Some(val.to_owned())));
        }
    }
    // Read stdin once and hand out a line per `KEY=-`, in order.
    let mut stdin_lines: std::vec::IntoIter<String> = Vec::new().into_iter();
    if stdin_count > 0 {
        let mut buf = String::new();
        std::io::stdin().read_to_string(&mut buf)?;
        let lines: Vec<String> = buf.lines().map(str::to_owned).collect();
        if lines.len() < stdin_count {
            return Err(format!(
                "expected {stdin_count} stdin line(s) for the `KEY=-` field(s), got {}",
                lines.len()
            )
            .into());
        }
        stdin_lines = lines.into_iter();
    }
    let mut out = serde_json::Map::new();
    for (key, val) in parsed {
        let value = match val {
            Some(v) => v,
            None => stdin_lines.next().unwrap_or_default(),
        };
        if value.is_empty() {
            return Err(format!("the value for `{key}` is empty").into());
        }
        out.insert(key, Value::String(value));
    }
    Ok(out)
}

/// `containers create`: read the spec, apply the `--name` override, POST it.
async fn run_container_create(
    client: &ControlPlaneClient,
    args: ContainerCreateArgs,
    json: bool,
) -> Result<(), Box<dyn Error>> {
    let mut spec = read_spec(&args.file)?;
    if let Some(name) = args.name {
        spec["name"] = Value::String(name);
    }
    let response = client
        .post_value("/v1/containers", &spec)
        .await
        .map_err(|e| map_unsupported("containers", e))?;
    emit_object(&response, json)
}

// --- db --------------------------------------------------------------------

/// Dispatches `verglas db`. Routes are mapped through [`map_unsupported`] so a
/// control plane without the dbs API yet fails clearly.
pub async fn run_db(command: DbCommand, json: bool) -> Result<(), Box<dyn Error>> {
    let client = client()?;
    match command {
        DbCommand::List => {
            let response = client
                .get_value("/v1/dbs")
                .await
                .map_err(|e| map_unsupported("databases", e))?;
            emit_list(
                &response,
                "dbs",
                &["name", "type", "state", "compute", "created_at"],
                "(no databases)",
                json,
            )
        }
        DbCommand::Create(DbCreateArgs { name, db_type }) => {
            // The engine is passed through as `type`; the control plane validates it
            // (a bad type is a 400 surfaced verbatim) and returns the engine's
            // connection endpoint. `postgres` is the default.
            let body = serde_json::json!({ "name": name, "type": db_type });
            let response = client
                .post_value("/v1/dbs", &body)
                .await
                .map_err(|e| map_unsupported("databases", e))?;
            // The response carries the ONE-TIME connection credentials. Print them
            // (that is the whole point of create) with a warning that the password
            // is shown once, and never log them anywhere else. In --json the raw
            // JSON goes to stdout and the warning to stderr, so stdout stays pure.
            let warning = format!(
                "WARNING: the connection credentials for `{name}` are shown once and are not \
                 stored by the CLI — copy them now."
            );
            if json {
                eprintln!("{warning}");
                println!("{}", serde_json::to_string_pretty(&response)?);
            } else {
                println!("{warning}\n");
                emit_object(&response, false)?;
            }
            Ok(())
        }
        DbCommand::Delete(DbNameArgs { name }) => {
            let response = client
                .delete_value(&format!("/v1/dbs/{name}"))
                .await
                .map_err(|e| map_unsupported("databases", e))?;
            if json {
                println!("{}", serde_json::to_string_pretty(&response)?);
            } else {
                println!("deleted database {name}");
            }
            Ok(())
        }
    }
}

// --- volumes ---------------------------------------------------------------

/// Parses a human size into a byte count: a bare integer is bytes; a suffix scales
/// it. Binary suffixes (`KiB`/`MiB`/`GiB`/`TiB`) are powers of 1024; decimal
/// (`KB`/`MB`/`GB`/`TB`, or bare `K`/`M`/`G`/`T`) are powers of 1000. Case-
/// insensitive. A malformed or non-positive size is a clear error.
fn parse_size(input: &str) -> Result<u64, Box<dyn Error>> {
    let s = input.trim();
    if s.is_empty() {
        return Err("size is empty".into());
    }
    // Split the leading numeric part from the unit suffix.
    let split = s
        .find(|c: char| !c.is_ascii_digit() && c != '.')
        .unwrap_or(s.len());
    let (num, unit) = s.split_at(split);
    let value: f64 = num
        .parse()
        .map_err(|_| format!("`{input}` is not a valid size"))?;
    let factor: f64 = match unit.trim().to_ascii_lowercase().as_str() {
        "" | "b" => 1.0,
        "k" | "kb" => 1e3,
        "m" | "mb" => 1e6,
        "g" | "gb" => 1e9,
        "t" | "tb" => 1e12,
        "kib" => 1024.0,
        "mib" => 1024f64.powi(2),
        "gib" => 1024f64.powi(3),
        "tib" => 1024f64.powi(4),
        other => return Err(format!("`{other}` is not a known size unit").into()),
    };
    let bytes = (value * factor).round();
    if !(bytes.is_finite() && bytes >= 1.0) {
        return Err(format!("`{input}` must be a positive size").into());
    }
    Ok(bytes as u64)
}

/// Renders a byte count as a human-readable binary size (e.g. `10 GiB`), so the
/// volume table reads plainly. Exact powers of 1024 render without a fraction.
fn human_size(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else if (value.fract()).abs() < 0.05 {
        format!("{} {}", value.round() as u64, UNITS[unit])
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

/// Maps a raw volume row from the control plane into the human table shape: a
/// readable size and a plain `attached` yes/no derived from the attachment field.
fn volume_row(entry: &Value) -> Value {
    let size = entry
        .get("size_bytes")
        .and_then(Value::as_u64)
        .map(human_size)
        .unwrap_or_else(|| DASH.to_owned());
    let attached = if entry
        .get("attached_deployment_id")
        .is_some_and(|v| !v.is_null())
    {
        "yes"
    } else {
        "no"
    };
    serde_json::json!({
        "name": entry.get("name").cloned().unwrap_or(Value::Null),
        "size": size,
        "state": entry.get("state").cloned().unwrap_or(Value::Null),
        "attached": attached,
        "device": entry.get("device_id").cloned().unwrap_or(Value::Null),
    })
}

/// Dispatches `verglas volumes`. Routes are mapped through [`map_unsupported`] so a
/// control plane without the volumes API yet fails clearly.
pub async fn run_volumes(command: VolumesCommand, json: bool) -> Result<(), Box<dyn Error>> {
    let client = client()?;
    match command {
        VolumesCommand::List => {
            let response = client
                .get_value("/v1/volumes")
                .await
                .map_err(|e| map_unsupported("block volumes", e))?;
            if json {
                println!("{}", serde_json::to_string_pretty(&response)?);
                return Ok(());
            }
            let rows: Vec<Value> = rows_of(&response, "volumes")
                .iter()
                .map(volume_row)
                .collect();
            println!(
                "{}",
                render_table(
                    &["name", "size", "state", "attached", "device"],
                    &rows,
                    "(no volumes)"
                )
            );
            Ok(())
        }
        VolumesCommand::Get(VolumeNameArgs { name }) => {
            let response = client
                .get_value(&format!("/v1/volumes/{name}"))
                .await
                .map_err(|e| map_unsupported("block volumes", e))?;
            emit_object(&response, json)
        }
        VolumesCommand::Create(VolumeCreateArgs { name, size }) => {
            let size_bytes = parse_size(&size)?;
            let body = serde_json::json!({ "name": name, "size_bytes": size_bytes });
            let response = client
                .post_value("/v1/volumes", &body)
                .await
                .map_err(|e| map_unsupported("block volumes", e))?;
            emit_object(&response, json)
        }
        VolumesCommand::Resize(VolumeResizeArgs { name, size }) => {
            let size_bytes = parse_size(&size)?;
            let body = serde_json::json!({ "size_bytes": size_bytes });
            let response = client
                .patch_value(&format!("/v1/volumes/{name}"), &body)
                .await
                .map_err(|e| map_unsupported("block volumes", e))?;
            emit_object(&response, json)
        }
        VolumesCommand::Delete(VolumeNameArgs { name }) => {
            let response = client
                .delete_value(&format!("/v1/volumes/{name}"))
                .await
                .map_err(|e| map_unsupported("block volumes", e))?;
            if json {
                println!("{}", serde_json::to_string_pretty(&response)?);
            } else {
                println!("deleted volume {name}");
            }
            Ok(())
        }
    }
}

// --- secrets ---------------------------------------------------------------

/// Dispatches `verglas secrets`. A stored value is never returned by the control
/// plane and never printed here: `list` shows names only, and `set` reports only
/// that the secret was stored.
pub async fn run_secrets(command: SecretsCommand, json: bool) -> Result<(), Box<dyn Error>> {
    let client = client()?;
    match command {
        SecretsCommand::List => {
            // The list endpoint returns `{secrets:[name,...]}` — an array of bare
            // names, not objects. Render each as a one-column row.
            let response = client.get_value("/v1/secrets").await?;
            if json {
                println!("{}", serde_json::to_string_pretty(&response)?);
            } else {
                let names = response
                    .get("secrets")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default();
                let rows: Vec<Value> = names
                    .iter()
                    .map(|name| match name {
                        Value::String(s) => serde_json::json!({ "name": s }),
                        other => serde_json::json!({ "name": other.to_string() }),
                    })
                    .collect();
                println!("{}", render_table(&["name"], &rows, "(no secrets)"));
            }
            Ok(())
        }
        SecretsCommand::Set(args) => {
            // Resolve the value (from --value, --file, or stdin) and POST it. The
            // value travels only in the request body — it is never printed, and
            // the success line names the secret alone.
            let value = resolve_secret_value(&args)?;
            let body = serde_json::json!({ "name": args.name, "value": value });
            let response = client.post_value("/v1/secrets", &body).await?;
            if json {
                println!("{}", serde_json::to_string_pretty(&response)?);
            } else {
                println!("set secret {}", args.name);
            }
            Ok(())
        }
        SecretsCommand::Delete(SecretNameArgs { name }) => {
            let response = client.delete_value(&format!("/v1/secrets/{name}")).await?;
            if json {
                println!("{}", serde_json::to_string_pretty(&response)?);
            } else {
                println!("deleted secret {name}");
            }
            Ok(())
        }
    }
}

/// Resolves the secret value for `secrets set`: `--value` verbatim, else the
/// trimmed contents of `--file`, else one line read from stdin (so the value is
/// never recorded in the shell history). An empty value is refused before any
/// request is made, and the value is never echoed. When stdin is a terminal a
/// short prompt is written to stderr so an interactive run is not a silent wait.
fn resolve_secret_value(args: &SecretSetArgs) -> Result<String, Box<dyn Error>> {
    if let Some(value) = &args.value {
        if value.is_empty() {
            return Err("the secret value is empty".into());
        }
        return Ok(value.clone());
    }
    if let Some(path) = &args.file {
        let text = std::fs::read_to_string(path)
            .map_err(|e| format!("could not read secret file {}: {e}", path.display()))?;
        let value = text.trim_end_matches(['\n', '\r']);
        if value.is_empty() {
            return Err(format!("the secret file {} is empty", path.display()).into());
        }
        return Ok(value.to_owned());
    }
    use std::io::{BufRead as _, IsTerminal as _, Write as _};
    if std::io::stdin().is_terminal() {
        eprint!("Enter value for secret `{}`: ", args.name);
        let _ = std::io::stderr().flush();
    }
    let mut line = String::new();
    std::io::stdin().lock().read_line(&mut line)?;
    let value = line.trim_end_matches(['\n', '\r']);
    if value.is_empty() {
        return Err(
            "the secret value is empty — provide it on stdin, or with --value or --file".into(),
        );
    }
    Ok(value.to_owned())
}

#[cfg(test)]
mod tests {
    //! Pure-function tests for spec parsing and the table/object renderers. The
    //! HTTP request construction and response rendering are exercised end to end
    //! against a mock control plane in `tests/cloud.rs`.

    use super::*;

    #[test]
    fn read_spec_parses_json_and_toml_to_the_same_object() {
        let dir = tempfile::tempdir().expect("tempdir");
        let json_path = dir.path().join("spec.json");
        let toml_path = dir.path().join("spec.toml");
        std::fs::write(
            &json_path,
            r#"{"name":"orders","kind":"source","min_instances":1}"#,
        )
        .expect("write json");
        std::fs::write(
            &toml_path,
            "name = \"orders\"\nkind = \"source\"\nmin_instances = 1\n",
        )
        .expect("write toml");

        let from_json = read_spec(&json_path).expect("json spec");
        let from_toml = read_spec(&toml_path).expect("toml spec");
        assert_eq!(from_json["name"], "orders");
        assert_eq!(from_toml["name"], "orders");
        assert_eq!(from_json["kind"], from_toml["kind"]);
        assert_eq!(from_json["min_instances"], from_toml["min_instances"]);
    }

    #[test]
    fn read_spec_rejects_a_non_object() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("spec.json");
        std::fs::write(&path, "[1,2,3]").expect("write");
        let err = read_spec(&path).expect_err("an array is not a valid spec");
        assert!(
            err.to_string().contains("must be a JSON/TOML object"),
            "the error must name the object requirement: {err}"
        );
    }

    #[test]
    fn render_table_lays_out_headers_and_dashes_missing_fields() {
        let rows = vec![
            serde_json::json!({"id":"d1","name":"orders","kind":"source"}),
            serde_json::json!({"id":"d2","name":"rollup"}),
        ];
        let table = render_table(&["id", "name", "kind"], &rows, "(none)");
        assert!(table.contains("ID") && table.contains("NAME") && table.contains("KIND"));
        assert!(table.contains("orders") && table.contains("rollup"));
        // The missing `kind` on the second row renders as a dash, not empty.
        let last_line = table.lines().last().expect("a row");
        assert!(
            last_line.contains(DASH),
            "a missing field must render as a dash: {last_line}"
        );
    }

    #[test]
    fn render_table_shows_a_placeholder_when_empty() {
        let table = render_table(&["name"], &[], "(no tables)");
        assert_eq!(table, "(no tables)");
    }

    #[test]
    fn field_joins_string_arrays() {
        let row = serde_json::json!({"target_tables":["a.orders","a.rollup"]});
        assert_eq!(field(&row, "target_tables"), "a.orders,a.rollup");
    }

    #[test]
    fn parse_size_reads_bytes_and_binary_and_decimal_suffixes() {
        assert_eq!(parse_size("1024").expect("bytes"), 1024);
        assert_eq!(parse_size("10GiB").expect("gib"), 10 * 1024 * 1024 * 1024);
        assert_eq!(parse_size("1kib").expect("kib"), 1024);
        assert_eq!(parse_size("1MB").expect("mb"), 1_000_000);
        assert_eq!(parse_size("2G").expect("bare g"), 2_000_000_000);
        assert!(parse_size("0").is_err(), "zero is not a positive size");
        assert!(parse_size("10ZB").is_err(), "unknown unit is an error");
        assert!(parse_size("abc").is_err(), "non-numeric is an error");
    }

    #[test]
    fn human_size_renders_readable_binary_sizes() {
        assert_eq!(human_size(10 * 1024 * 1024 * 1024), "10 GiB");
        assert_eq!(human_size(512), "512 B");
        assert_eq!(human_size(1536), "1.5 KiB");
    }

    #[test]
    fn volume_row_derives_size_and_attached_flag() {
        let attached = serde_json::json!({
            "name": "data", "size_bytes": 10u64 * 1024 * 1024 * 1024,
            "state": "available", "attached_deployment_id": "dep-1", "device_id": "blk-t-data"
        });
        let row = volume_row(&attached);
        assert_eq!(row["size"], "10 GiB");
        assert_eq!(row["attached"], "yes");
        let standalone = serde_json::json!({
            "name": "d", "size_bytes": 1u64, "state": "available",
            "attached_deployment_id": Value::Null, "device_id": "blk-t-d"
        });
        assert_eq!(volume_row(&standalone)["attached"], "no");
    }

    #[test]
    fn rows_of_reads_a_wrapped_or_bare_array() {
        let wrapped = serde_json::json!({"deployments":[{"id":"d1"}]});
        assert_eq!(rows_of(&wrapped, "deployments").len(), 1);
        let bare = serde_json::json!([{"id":"d1"},{"id":"d2"}]);
        assert_eq!(rows_of(&bare, "deployments").len(), 2);
        let empty = serde_json::json!({"other":1});
        assert!(rows_of(&empty, "deployments").is_empty());
    }
}
