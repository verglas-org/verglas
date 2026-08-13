//! The canonical Iceberg table layout for the property graph, and the
//! conversion between the model types and table rows.
//!
//! Two tables: a nodes table and an edges (triplet) table. Both are
//! append-only. The schemas here are the one documented layout the whole engine
//! writes and reads; column names are constants so the scan path and the write
//! path never drift.
//!
//! Belief revision is append-only: the edge table has no mutable
//! `superseded_by` column (that would require rewriting the retracted row).
//! Instead a revising row carries `supersedes` = the retracted `edge_id` (a
//! new→old pointer). #83's forward `superseded_by` view is derivable from it.
//! This preserves time-travel: an older snapshot simply does not contain the
//! revising row, so the old belief is still live there.

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use serde_json::{Map, Value, json};

use crate::model::{Edge, Node};

/// The nodes-table name within a graph's namespace.
pub const NODES_TABLE: &str = "nodes";
/// The edges-table name within a graph's namespace.
pub const EDGES_TABLE: &str = "edges";

/// Nodes-table column: the entity id.
pub const COL_NODE_ID: &str = "node_id";
/// Nodes-table column: JSON array of labels.
pub const COL_LABELS: &str = "labels";

/// Edges-table column: the assertion id.
pub const COL_EDGE_ID: &str = "edge_id";
/// Edges-table column: the subject entity id.
pub const COL_SRC_ID: &str = "src_id";
/// Edges-table column: the predicate.
pub const COL_PREDICATE: &str = "predicate";
/// Edges-table column: the object entity id.
pub const COL_DST_ID: &str = "dst_id";
/// Edges-table column: confidence in [0, 1].
pub const COL_CONFIDENCE: &str = "confidence";
/// Edges-table column: the asserting memory id.
pub const COL_PROVENANCE: &str = "provenance";
/// Edges-table column: the `edge_id` this row supersedes, if any.
pub const COL_SUPERSEDES: &str = "supersedes";
/// Edges-table column: caller-supplied logical valid-from time.
pub const COL_VALID_FROM: &str = "valid_from";

/// Column shared by both tables: the owning agent.
pub const COL_AGENT_ID: &str = "agent_id";
/// Column shared by both tables: the namespace scope.
pub const COL_NAMESPACE: &str = "namespace";
/// Column shared by both tables: the JSON property bag.
pub const COL_PROPERTIES: &str = "properties";

/// The Arrow schema of the nodes table. Object columns (labels, properties) are
/// stored as JSON strings so any shape round-trips without a schema change —
/// the graph model is the source of truth, the columns are its serialization.
pub fn nodes_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(COL_NODE_ID, DataType::Utf8, false),
        Field::new(COL_LABELS, DataType::Utf8, true),
        Field::new(COL_PROPERTIES, DataType::Utf8, true),
        Field::new(COL_AGENT_ID, DataType::Utf8, true),
        Field::new(COL_NAMESPACE, DataType::Utf8, true),
    ]))
}

/// The Arrow schema of the edges (triplet) table.
pub fn edges_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(COL_EDGE_ID, DataType::Utf8, false),
        Field::new(COL_SRC_ID, DataType::Utf8, false),
        Field::new(COL_PREDICATE, DataType::Utf8, false),
        Field::new(COL_DST_ID, DataType::Utf8, false),
        Field::new(COL_CONFIDENCE, DataType::Float64, false),
        Field::new(COL_PROVENANCE, DataType::Utf8, false),
        Field::new(COL_SUPERSEDES, DataType::Utf8, true),
        Field::new(COL_VALID_FROM, DataType::Int64, true),
        Field::new(COL_AGENT_ID, DataType::Utf8, true),
        Field::new(COL_NAMESPACE, DataType::Utf8, true),
        Field::new(COL_PROPERTIES, DataType::Utf8, true),
    ]))
}

/// Serializes a JSON object to a compact string, or null when it is empty.
fn object_string(map: &Map<String, Value>) -> Value {
    if map.is_empty() {
        Value::Null
    } else {
        Value::String(Value::Object(map.clone()).to_string())
    }
}

/// Serializes a string list to a JSON-array string, or null when empty.
fn list_string(items: &[String]) -> Value {
    if items.is_empty() {
        Value::Null
    } else {
        Value::String(json!(items).to_string())
    }
}

/// Renders an optional string as a JSON string or null.
fn opt_string(value: &Option<String>) -> Value {
    match value {
        Some(v) => Value::String(v.clone()),
        None => Value::Null,
    }
}

/// Converts a node to the JSON row the commit path appends.
pub fn node_to_row(node: &Node) -> Value {
    json!({
        COL_NODE_ID: node.id,
        COL_LABELS: list_string(&node.labels),
        COL_PROPERTIES: object_string(&node.properties),
        COL_AGENT_ID: opt_string(&node.agent_id),
        COL_NAMESPACE: opt_string(&node.namespace),
    })
}

/// Converts an edge to the JSON row the commit path appends.
pub fn edge_to_row(edge: &Edge) -> Value {
    json!({
        COL_EDGE_ID: edge.edge_id,
        COL_SRC_ID: edge.src_id,
        COL_PREDICATE: edge.predicate,
        COL_DST_ID: edge.dst_id,
        COL_CONFIDENCE: edge.confidence,
        COL_PROVENANCE: edge.provenance,
        COL_SUPERSEDES: opt_string(&edge.supersedes),
        COL_VALID_FROM: edge.valid_from,
        COL_AGENT_ID: opt_string(&edge.agent_id),
        COL_NAMESPACE: opt_string(&edge.namespace),
        COL_PROPERTIES: object_string(&edge.properties),
    })
}
