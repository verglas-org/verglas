//! Read-only table inspection: `list`, `show`, and `history`.
//!
//! These verbs never touch data; they read catalog listings and table metadata.
//! `show` reports the schema, partition columns, and the row/file/size counters
//! from the current snapshot summary; `history` reports every snapshot with its
//! id, time, operation, and summary — the provenance trail an agent walks to
//! answer "where did this number come from".

use std::collections::HashSet;

use chrono::TimeZone;
use futures::TryStreamExt;
use iceberg::{Catalog, NamespaceIdent, TableIdent};

use crate::error::Result;
use crate::ident::ident_to_dotted;
use crate::report::{HistoryReport, ListReport, ShowReport, SnapshotInfo, TableRef, field_info};

/// Lists tables. With `namespace` set, lists just that namespace; otherwise
/// walks every top-level namespace. Results are sorted for stable output.
pub async fn list(catalog: &dyn Catalog, namespace: Option<&str>) -> Result<ListReport> {
    let namespaces = match namespace {
        Some(dotted) => vec![NamespaceIdent::from_vec(
            dotted.split('.').map(str::to_owned).collect(),
        )?],
        None => catalog.list_namespaces(None).await?,
    };

    let mut tables = Vec::new();
    for ns in namespaces {
        for ident in catalog.list_tables(&ns).await? {
            tables.push(TableRef {
                namespace: ident.namespace().as_ref().join("."),
                name: ident.name().to_owned(),
            });
        }
    }
    tables.sort_by(|a, b| (&a.namespace, &a.name).cmp(&(&b.namespace, &b.name)));
    Ok(ListReport { tables })
}

/// Shows a table's schema, partition columns, current-snapshot counters, and
/// current snapshot id.
pub async fn show(catalog: &dyn Catalog, ident: &TableIdent) -> Result<ShowReport> {
    let table = catalog.load_table(ident).await?;
    let metadata = table.metadata();

    let schema = metadata
        .current_schema()
        .as_struct()
        .fields()
        .iter()
        .map(field_info)
        .collect();

    let partition_by = metadata
        .default_partition_spec()
        .fields()
        .iter()
        .map(|field| field.name.clone())
        .collect();

    // Counts come from the current snapshot's data files, not the snapshot
    // summary: iceberg-rust's fast append records only its own contribution in
    // `total-records`, so the summary is not the cumulative table total. The
    // scan plan lists every live data file, which is authoritative.
    let (row_count, file_count, size_bytes) = if metadata.current_snapshot().is_some() {
        let scan = table.scan().build()?;
        let tasks: Vec<_> = scan.plan_files().await?.try_collect().await?;
        let mut rows = 0u64;
        let mut size = 0u64;
        let mut seen: HashSet<String> = HashSet::new();
        for task in &tasks {
            // A file split into multiple tasks must be counted once.
            if seen.insert(task.data_file_path.clone()) {
                rows += task.record_count.unwrap_or(0);
                size += task.file_size_in_bytes;
            }
        }
        (Some(rows), Some(seen.len() as u64), Some(size))
    } else {
        (None, None, None)
    };

    Ok(ShowReport {
        table: ident_to_dotted(ident),
        schema,
        partition_by,
        row_count,
        file_count,
        size_bytes,
        current_snapshot_id: metadata.current_snapshot_id(),
    })
}

/// Reports every snapshot oldest-first: id, parent, time, operation, summary.
pub async fn history(catalog: &dyn Catalog, ident: &TableIdent) -> Result<HistoryReport> {
    let table = catalog.load_table(ident).await?;
    let metadata = table.metadata();

    let mut snapshots: Vec<SnapshotInfo> = metadata
        .snapshots()
        .map(|snapshot| {
            let summary = snapshot.summary();
            SnapshotInfo {
                snapshot_id: snapshot.snapshot_id(),
                parent_snapshot_id: snapshot.parent_snapshot_id(),
                timestamp_ms: snapshot.timestamp_ms(),
                timestamp: rfc3339(snapshot.timestamp_ms()),
                operation: summary.operation.as_str().to_owned(),
                summary: summary
                    .additional_properties
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
            }
        })
        .collect();
    snapshots.sort_by_key(|s| s.timestamp_ms);
    Ok(HistoryReport {
        table: ident_to_dotted(ident),
        snapshots,
    })
}

/// Formats an epoch-millis timestamp as an RFC 3339 UTC string, or the raw
/// millis wrapped in a marker if it is somehow out of range.
fn rfc3339(timestamp_ms: i64) -> String {
    match chrono::Utc.timestamp_millis_opt(timestamp_ms) {
        chrono::LocalResult::Single(dt) => dt.to_rfc3339(),
        _ => format!("epoch-ms:{timestamp_ms}"),
    }
}
