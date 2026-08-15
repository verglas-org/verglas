//! `verglas table` — create, append, list, show, history, and delete.
//!
//! Every verb speaks the tenant's Iceberg REST catalog directly: create/append go through `verglas_sdk::ingest` (#145), which
//! writes Parquet data files straight to object storage with catalog-vended
//! credentials, and list/show/history/delete speak Iceberg REST directly. The
//! CLI embeds no query or write engine of its own — the SDK owns the whole
//! write path. Results render as `--output json` (the stable shape the skill
//! parses) or a human summary.

use std::error::Error;

use crate::cli::{TableCommand, TableDeleteArgs};
use verglas_sdk::report::{
    AppendReport, CreateReport, FieldInfo, HistoryReport, ListReport, ShowReport,
};

/// Dispatches a `verglas table` subcommand.
pub async fn run(
    command: TableCommand,
    token: Option<&str>,
    json: bool,
) -> Result<(), Box<dyn Error>> {
    match command {
        TableCommand::Delete(args) => return run_delete(args, token, json).await,
        TableCommand::List(args) => {
            let report = crate::commands::catalog::CatalogClient::from_agent_config(token)?
                .list_tables(args.namespace.as_deref())
                .await?;
            emit(&report, json, render_list)
        }
        TableCommand::Show(args) => {
            let report = crate::commands::catalog::CatalogClient::from_agent_config(token)?
                .show_table(&args.table)
                .await?;
            emit(&report, json, render_show)
        }
        TableCommand::History(args) => {
            let report = crate::commands::catalog::CatalogClient::from_agent_config(token)?
                .table_history(&args.table)
                .await?;
            emit(&report, json, render_history)
        }
        TableCommand::Create(args) => {
            let catalog = crate::commands::catalog::CatalogClient::from_agent_config(token)?;
            let report: CreateReport = verglas_sdk::ingest::create_table(
                &catalog.ingest_config(),
                &args.table,
                &args.source,
                args.partition_by.as_deref(),
            )
            .await?;
            emit(&report, json, render_create)
        }
        TableCommand::Append(args) => {
            let catalog = crate::commands::catalog::CatalogClient::from_agent_config(token)?;
            let report: AppendReport =
                verglas_sdk::ingest::append(&catalog.ingest_config(), &args.table, &args.source)
                    .await?;
            emit(&report, json, render_append)
        }
    }
}

/// Drops a table via the tenant's Iceberg REST catalog. This is the one table
/// verb that does not go through the server: dropping a table is a catalog
/// operation, so it reads the `[catalog]` uri and bearer from
/// `~/.verglas/config.toml` and issues the REST drop-table call. Requires
/// `--yes` or an interactive `y` confirmation; prints exactly what will be
/// dropped before doing it.
async fn run_delete(
    args: TableDeleteArgs,
    token: Option<&str>,
    json: bool,
) -> Result<(), Box<dyn Error>> {
    let catalog = crate::commands::catalog::CatalogClient::from_agent_config(token)?;
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
