//! Reading the edge table as of a snapshot, and computing the live edge set.
//!
//! This is the source-of-truth path: it reads the plain Iceberg edge table
//! through the engine's own query path (time-travelled to a snapshot) and
//! applies the append-only supersede rule. Both the index builder and the
//! traversal fallback go through here, so "the graph as of snapshot S" has one
//! definition. Because it reads only the tables, it is exactly what answers when
//! no Puffin index is present — the turn-off path.

use std::collections::HashSet;
use std::sync::Arc;

use iceberg::table::Table;
use iceberg::{Catalog, TableIdent};
use serde_json::{Map, Value};
use verglas_iceberg::TimeTravel;
use verglas_iceberg::query::query;

use crate::error::{GraphError, Result};
use crate::model::Edge;
use crate::schema::{
    COL_AGENT_ID, COL_CONFIDENCE, COL_DST_ID, COL_EDGE_ID, COL_NAMESPACE, COL_PREDICATE,
    COL_PROPERTIES, COL_PROVENANCE, COL_SRC_ID, COL_SUPERSEDES, COL_VALID_FROM,
};

/// The dotted `namespace.name` form of a table identifier.
pub fn dotted(ident: &TableIdent) -> String {
    let namespace = ident.namespace().as_ref().join(".");
    if namespace.is_empty() {
        ident.name().to_owned()
    } else {
        format!("{namespace}.{}", ident.name())
    }
}

/// Loads the edges of `edges_ident` as of a snapshot and returns them with the
/// snapshot they reflect. When `at` is `None` the table's current snapshot is
/// used; a table with no snapshot yet yields an empty set and `None`.
///
/// The returned edges are the raw rows (before the supersede filter); callers
/// that want the traversable set call [`live_edges`].
pub async fn load_edges(
    catalog: Arc<dyn Catalog>,
    edges_ident: &TableIdent,
    at: Option<i64>,
) -> Result<(Vec<Edge>, Option<i64>)> {
    let table = catalog.load_table(edges_ident).await?;
    let snapshot = match resolve_snapshot(&table, at) {
        Some(id) => id,
        None => return Ok((Vec::new(), None)),
    };

    let table_name = dotted(edges_ident);
    let report = query(
        catalog,
        "SELECT * FROM edges",
        Some(TimeTravel {
            reference: snapshot.to_string(),
            table: table_name.clone(),
        }),
    )
    .await?;

    let mut edges = Vec::with_capacity(report.rows.len());
    for row in &report.rows {
        edges.push(edge_from_row(row, &table_name)?);
    }
    Ok((edges, Some(snapshot)))
}

/// Resolves the snapshot to read: the requested one, or the table's current
/// snapshot, or `None` when the table has no data yet.
fn resolve_snapshot(table: &Table, at: Option<i64>) -> Option<i64> {
    at.or_else(|| table.metadata().current_snapshot_id())
}

/// Filters raw edges to the live set: every edge whose id is not named by a
/// visible `supersedes` pointer. Belief revision is append-only, so a retracted
/// edge is one another (later, still-visible) row supersedes.
pub fn live_edges(edges: Vec<Edge>) -> Vec<Edge> {
    let superseded: HashSet<&str> = edges
        .iter()
        .filter_map(|e| e.supersedes.as_deref())
        .collect();
    if superseded.is_empty() {
        return edges;
    }
    let superseded: HashSet<String> = superseded.into_iter().map(str::to_owned).collect();
    edges
        .into_iter()
        .filter(|e| !superseded.contains(&e.edge_id))
        .collect()
}

/// Parses one query result row into an [`Edge`], naming any missing required
/// column so a malformed table surfaces clearly rather than silently dropping
/// edges.
fn edge_from_row(row: &Map<String, Value>, table: &str) -> Result<Edge> {
    Ok(Edge {
        edge_id: required_str(row, COL_EDGE_ID, table)?,
        src_id: required_str(row, COL_SRC_ID, table)?,
        predicate: required_str(row, COL_PREDICATE, table)?,
        dst_id: required_str(row, COL_DST_ID, table)?,
        confidence: required_f64(row, COL_CONFIDENCE, table)?,
        provenance: required_str(row, COL_PROVENANCE, table)?,
        supersedes: optional_str(row, COL_SUPERSEDES),
        valid_from: optional_i64(row, COL_VALID_FROM),
        agent_id: optional_str(row, COL_AGENT_ID),
        namespace: optional_str(row, COL_NAMESPACE),
        properties: parse_properties(row.get(COL_PROPERTIES)),
    })
}

/// Reads a required string column, erroring by name when it is absent or not a
/// string.
fn required_str(row: &Map<String, Value>, column: &str, table: &str) -> Result<String> {
    row.get(column)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| GraphError::MalformedRow {
            table: table.to_owned(),
            detail: format!("missing required string column `{column}`"),
        })
}

/// Reads a required floating-point column, erroring by name when it is absent or
/// not a number.
fn required_f64(row: &Map<String, Value>, column: &str, table: &str) -> Result<f64> {
    row.get(column)
        .and_then(Value::as_f64)
        .ok_or_else(|| GraphError::MalformedRow {
            table: table.to_owned(),
            detail: format!("missing required numeric column `{column}`"),
        })
}

/// Reads an optional string column, treating null/absent as `None`.
fn optional_str(row: &Map<String, Value>, column: &str) -> Option<String> {
    row.get(column).and_then(Value::as_str).map(str::to_owned)
}

/// Reads an optional integer column, treating null/absent as `None`.
fn optional_i64(row: &Map<String, Value>, column: &str) -> Option<i64> {
    row.get(column).and_then(Value::as_i64)
}

/// Parses the JSON-string `properties` column back into an object, defaulting to
/// empty when absent or unparseable (properties are best-effort payload, not a
/// correctness surface).
fn parse_properties(value: Option<&Value>) -> Map<String, Value> {
    match value.and_then(Value::as_str) {
        Some(text) => match serde_json::from_str::<Value>(text) {
            Ok(Value::Object(map)) => map,
            _ => Map::new(),
        },
        None => Map::new(),
    }
}
