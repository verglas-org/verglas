//! `verglas table` — create, append, list, show, and history (issue #287).
//!
//! Every verb calls the local daemon's HTTP API (#323): create and append
//! stream the source file's bytes to the ingest route, list/show/history read
//! the inspect routes. The daemon is the only local write authority — the CLI
//! embeds no engine and has no daemon-less path; with no daemon listening each
//! verb fails with a clear error naming the endpoint. Results render as
//! `--output json` (the stable shape the skill parses) or a human summary.

use std::error::Error;

use verglas_sdk::CompactionReport;
use verglas_sdk::report::{
    AppendReport, CreateReport, FieldInfo, HistoryReport, ListReport, ShowReport,
};
use verglas_sdk::vector::{
    DeclareIndexRequest, IndexListResponse, IndexParams, IndexReport, RegistryIndexListResponse,
    SearchRequest, SearchResponse,
};

use crate::cli::{IndexCommand, TableCommand, TableDeleteArgs};

/// Dispatches a `verglas table` subcommand.
pub async fn run(
    command: TableCommand,
    daemon_endpoint: &str,
    json: bool,
) -> Result<(), Box<dyn Error>> {
    // `delete` speaks the Iceberg REST catalog directly and `metrics` builds its
    // own daemon client, so neither needs — nor should fail on — the data-plane
    // client the other verbs open.
    match command {
        TableCommand::Delete(args) => return run_delete(args, json).await,
        TableCommand::Metrics => {
            return crate::commands::table_metrics::run(daemon_endpoint, json).await;
        }
        _ => {}
    }
    let client = crate::backend::daemon(daemon_endpoint)?;
    match command {
        TableCommand::Create(args) => {
            let report: CreateReport = client
                .ingest(
                    &args.table,
                    &args.source,
                    "create",
                    args.partition_by.as_deref(),
                )
                .await?;
            emit(&report, json, render_create)
        }
        TableCommand::Append(args) => {
            let report: AppendReport = client
                .ingest(&args.table, &args.source, "append", None)
                .await?;
            emit(&report, json, render_append)
        }
        TableCommand::List(args) => {
            let path = match &args.namespace {
                Some(ns) => format!("/v1/tables?namespace={ns}"),
                None => "/v1/tables".to_owned(),
            };
            let report: ListReport = client.get(&path).await?;
            emit(&report, json, render_list)
        }
        TableCommand::Show(args) => {
            let report: ShowReport = client
                .get(&format!("/v1/tables/{}/describe", args.table))
                .await?;
            emit(&report, json, render_show)
        }
        TableCommand::History(args) => {
            let report: HistoryReport = client
                .get(&format!("/v1/tables/{}/history", args.table))
                .await?;
            emit(&report, json, render_history)
        }
        TableCommand::Compact => {
            let report: CompactionReport = client
                .post_json("/admin/compact", &serde_json::json!({}))
                .await?;
            emit(&report, json, render_compact)
        }
        // Handled above, before the data-plane client was built.
        TableCommand::Delete(_) | TableCommand::Metrics => {
            unreachable!("delete and metrics are dispatched before the daemon client")
        }
    }
}

/// Drops a table via the tenant's Iceberg REST catalog. This is the one table
/// verb that does not go through the daemon: dropping a table is a catalog
/// control-plane operation, so it reads the `[catalog]` uri and bearer from
/// `~/.verglas/config.toml` and issues the REST drop-table call. Requires
/// `--yes` or an interactive `y` confirmation; prints exactly what will be
/// dropped before doing it.
async fn run_delete(args: TableDeleteArgs, json: bool) -> Result<(), Box<dyn Error>> {
    let catalog = crate::commands::catalog::CatalogClient::from_agent_config()?;
    let (namespace, table) = crate::commands::catalog::split_ident(&args.table)?;

    if !args.yes {
        eprintln!(
            "About to DROP table `{}` from the catalog at {}.",
            args.table,
            catalog.base()
        );
        eprintln!("This removes the table's catalog entry and cannot be undone.");
        eprint!("Proceed? [y/N] ");
        use std::io::Write;
        std::io::stderr().flush().ok();
        let mut answer = String::new();
        std::io::stdin().read_line(&mut answer)?;
        let answer = answer.trim().to_ascii_lowercase();
        if answer != "y" && answer != "yes" {
            return Err("aborted: pass --yes or answer `y` to confirm the drop".into());
        }
    }

    catalog.drop_table(&namespace, &table).await?;
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(
                &serde_json::json!({ "table": args.table, "dropped": true })
            )?
        );
    } else {
        println!("dropped table {}", args.table);
    }
    Ok(())
}

/// Dispatches a `verglas index` subcommand: the one command group for vector
/// indexes. `list` reads the durable registry (or one table's indexes); `add`
/// and `search` are the table-scoped index operations. Every verb calls the
/// daemon's data-plane API.
pub async fn run_index_registry(
    command: IndexCommand,
    daemon_endpoint: &str,
    json: bool,
) -> Result<(), Box<dyn Error>> {
    let client = crate::backend::daemon(daemon_endpoint)?;
    match command {
        IndexCommand::List(args) => match args.table {
            // No table: the durable registry across tables, graphs, and clusters.
            None => {
                let response: RegistryIndexListResponse = client.get("/v1/indexes").await?;
                emit(&response, json, render_registry_index_list)
            }
            // A table: just that table's declared indexes.
            Some(table) => {
                let response: IndexListResponse =
                    client.get(&format!("/v1/tables/{table}/indexes")).await?;
                emit(&response, json, render_index_list)
            }
        },
        IndexCommand::Add(args) => {
            let request = DeclareIndexRequest {
                field: args.field.clone(),
                metric: args.metric.clone(),
                id_field: args.id_field.clone(),
                params: (args.r.is_some() || args.l.is_some() || args.alpha.is_some()).then_some(
                    IndexParams {
                        r: args.r,
                        l: args.l,
                        alpha: args.alpha,
                    },
                ),
            };
            let report: IndexReport = client
                .post_json(
                    &format!("/v1/tables/{}/indexes", args.table),
                    &serde_json::to_value(&request)?,
                )
                .await?;
            emit(&report, json, render_index)
        }
        IndexCommand::Search(args) => {
            let request = SearchRequest {
                vector: parse_vector(&args.vector)?,
                k: args.k,
                l: args.l,
            };
            let response: SearchResponse = client
                .post_json(
                    &format!("/v1/tables/{}/indexes/{}/search", args.table, args.field),
                    &serde_json::to_value(&request)?,
                )
                .await?;
            emit(&response, json, render_search)
        }
    }
}

/// Human summary for `index list` (the durable registry).
fn render_registry_index_list(response: &RegistryIndexListResponse) {
    if response.indexes.is_empty() {
        println!("(no indexes)");
        return;
    }
    for index in &response.indexes {
        let snapshot = index
            .reflected_snapshot
            .map(|s| s.to_string())
            .unwrap_or_else(|| "-".to_owned());
        println!(
            "{}.{} [{}]  cluster {}  {}  snapshot {}",
            index.target, index.field, index.metric, index.cluster_id, index.state, snapshot
        );
    }
}

/// Parses a comma-separated float list into a query vector.
fn parse_vector(s: &str) -> Result<Vec<f32>, Box<dyn Error>> {
    s.split(',')
        .map(str::trim)
        .filter(|t| !t.is_empty())
        .map(|t| {
            t.parse::<f32>()
                .map_err(|e| format!("bad vector element '{t}': {e}").into())
        })
        .collect()
}

/// Serializes `report` as pretty JSON when `json` is set, else calls `render`.
fn emit<T, F>(report: &T, json: bool, render: F) -> Result<(), Box<dyn Error>>
where
    T: serde::Serialize,
    F: FnOnce(&T),
{
    if json {
        println!("{}", serde_json::to_string_pretty(report)?);
    } else {
        render(report);
    }
    Ok(())
}

/// Renders a one-line schema column list.
fn render_schema(schema: &[FieldInfo]) {
    for field in schema {
        let required = if field.required {
            "required"
        } else {
            "optional"
        };
        println!("  {:<20} {:<16} {required}", field.name, field.r#type);
    }
}

/// Human summary for `create`.
fn render_create(report: &CreateReport) {
    println!("created {} ({} columns)", report.table, report.schema.len());
    render_schema(&report.schema);
    if !report.partition_by.is_empty() {
        println!("  partitioned by: {}", report.partition_by.join(", "));
    }
    println!(
        "  wrote {} row(s) in {} file(s); snapshot {}",
        report.records_added,
        report.data_files_added,
        report
            .snapshot_id
            .map(|s| s.to_string())
            .unwrap_or_else(|| "-".to_owned())
    );
}

/// Human summary for `append`.
fn render_append(report: &AppendReport) {
    println!(
        "appended {} row(s) to {} in {} file(s); snapshot {}",
        report.records_added,
        report.table,
        report.data_files_added,
        report
            .snapshot_id
            .map(|s| s.to_string())
            .unwrap_or_else(|| "-".to_owned())
    );
}

/// Human summary for `list`.
fn render_list(report: &ListReport) {
    if report.tables.is_empty() {
        println!("(no tables)");
        return;
    }
    for table in &report.tables {
        println!("{}.{}", table.namespace, table.name);
    }
}

/// Human summary for `show`.
fn render_show(report: &ShowReport) {
    println!("{}", report.table);
    render_schema(&report.schema);
    if !report.partition_by.is_empty() {
        println!("  partitioned by: {}", report.partition_by.join(", "));
    }
    let n = |v: Option<u64>| v.map(|x| x.to_string()).unwrap_or_else(|| "-".to_owned());
    println!(
        "  rows: {}   files: {}   size: {} bytes",
        n(report.row_count),
        n(report.file_count),
        n(report.size_bytes)
    );
    println!(
        "  current snapshot: {}",
        report
            .current_snapshot_id
            .map(|s| s.to_string())
            .unwrap_or_else(|| "(empty)".to_owned())
    );
}

/// Human summary for `index add`.
fn render_index(report: &IndexReport) {
    let snap = report
        .reflected_snapshot
        .map(|s| s.to_string())
        .unwrap_or_else(|| "(no data yet)".to_owned());
    println!(
        "index {}.{} [{}] — {} build reflecting snapshot {}",
        report.target,
        report.field,
        report.metric,
        if report.full_build {
            "full"
        } else {
            "incremental"
        },
        snap
    );
    println!(
        "  live {}   inserts {}   deletes {}   tombstones {}{}",
        report.live_count,
        report.inserts,
        report.deletes,
        report.tombstones,
        if report.consolidated {
            "   (consolidated)"
        } else {
            ""
        }
    );
    if let Some(loc) = &report.blob_location {
        println!("  blob: {loc} ({} bytes)", report.blob_bytes);
    }
}

/// Human summary for `index search`.
fn render_search(response: &SearchResponse) {
    let via = if response.source == "index" {
        "index"
    } else {
        "brute force (no index)"
    };
    println!("{} neighbor(s) via {via}:", response.neighbors.len());
    for hit in &response.neighbors {
        println!("  {:<12} {:.6}", hit.id, hit.distance);
    }
}

/// Human summary for `index list`.
fn render_index_list(response: &IndexListResponse) {
    if response.indexes.is_empty() {
        println!("(no indexes)");
        return;
    }
    for index in &response.indexes {
        let live = index
            .live_count
            .map(|c| c.to_string())
            .unwrap_or_else(|| "-".to_owned());
        println!(
            "{}.{} [{}]  live {}",
            index.target, index.field, index.metric, live
        );
    }
}

/// Human summary for `compact`.
fn render_compact(report: &CompactionReport) {
    let budget = if report.budget_bounded {
        "; budget-bounded, run again to continue"
    } else {
        ""
    };
    let history = if report.snapshots_expired > 0 || report.orphan_files_deleted > 0 {
        format!(
            "; expired {} old snapshot(s), reclaimed {} file(s)",
            report.snapshots_expired, report.orphan_files_deleted
        )
    } else {
        String::new()
    };
    println!(
        "scanned {} table(s), committed {} group(s), {} undersized file(s) remain{budget}{history}",
        report.tables_scanned, report.groups_committed, report.undersized_remaining
    );
    for table in &report.compacted {
        let snapshot = table
            .snapshot_id
            .map(|s| format!("; snapshot {s}"))
            .unwrap_or_default();
        let table_history = if table.snapshots_expired > 0 || table.orphan_files_deleted > 0 {
            format!(
                "; expired {} snapshot(s), reclaimed {} file(s)",
                table.snapshots_expired, table.orphan_files_deleted
            )
        } else {
            String::new()
        };
        println!(
            "  {}: {} group(s), {} file(s) -> {} file(s){snapshot}{table_history}",
            table.table, table.groups_committed, table.input_data_files, table.output_data_files
        );
    }
    for (table, message) in &report.failures {
        println!("  {table}: failed — {message}");
    }
    if report.compacted.is_empty() && report.failures.is_empty() {
        println!("  (nothing to compact)");
    }
}

/// Human summary for `history`.
fn render_history(report: &HistoryReport) {
    println!("{} — {} snapshot(s)", report.table, report.snapshots.len());
    for snapshot in &report.snapshots {
        println!(
            "  {}  {}  {}  {}",
            snapshot.timestamp,
            snapshot.snapshot_id,
            snapshot.operation,
            {
                let mut keys: Vec<String> = snapshot
                    .summary
                    .iter()
                    .filter(|(k, _)| k.starts_with("added-") || k.starts_with("total-"))
                    .map(|(k, v)| format!("{k}={v}"))
                    .collect();
                keys.sort();
                keys.join(" ")
            }
        );
    }
}
