//! The `indexes` verb-family wire types: the request bodies and reports the
//! server's `/v1/tables/{t}/indexes...` and `/v1/graphs/{ns}/indexes...` routes
//! serve, and the CLI and TypeScript SDK speak.
//!
//! A vector index is a streaming Vamana (DiskANN/FreshDiskANN) ANN index over an
//! embedding field, maintained by an MV and attached to the reflected Iceberg
//! snapshot as a Puffin statistics file. These types are the transport contract
//! only; the server owns the conversion to the engine types.
//!
//! Every field is camelCase on the wire, matching the other SDK shapes.

use serde::{Deserialize, Serialize};

/// Optional Vamana construction parameters. Omitted fields take the engine
/// defaults (`R = 64`, `L = 100`, `alpha = 1.2`).
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IndexParams {
    /// Maximum out-degree `R`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub r: Option<usize>,
    /// Build/insert candidate-list size `L`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub l: Option<usize>,
    /// Pruning parameter `alpha` (`>= 1`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alpha: Option<f32>,
}

/// `POST /v1/tables/{t}/indexes` / `POST /v1/graphs/{ns}/indexes` body: declare
/// an index on a field and kick the initial build.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeclareIndexRequest {
    /// The embedding column to index.
    pub field: String,
    /// The distance metric: `"l2"` or `"cosine"`. Defaults to `"cosine"`.
    #[serde(default = "default_metric")]
    pub metric: String,
    /// The identity column keying vectors. Defaults to `"id"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id_field: Option<String>,
    /// Optional Vamana parameters.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<IndexParams>,
}

fn default_metric() -> String {
    "cosine".to_owned()
}

/// The report from declaring/refreshing an index.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IndexReport {
    /// The logical target (`tbl:ns.table` or `graph:ns`).
    pub target: String,
    /// The indexed field.
    pub field: String,
    /// The metric the index was built under.
    pub metric: String,
    /// The source snapshot the index now reflects, or absent when the source had
    /// no rows to index yet.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reflected_snapshot: Option<i64>,
    /// Whether this run was a full build (vs an incremental update).
    pub full_build: bool,
    /// Vectors inserted/updated this run.
    pub inserts: usize,
    /// Vectors deleted this run.
    pub deletes: usize,
    /// Whether a delete-consolidation ran.
    pub consolidated: bool,
    /// Live vectors after the run.
    pub live_count: usize,
    /// Tombstones remaining in the persisted blob.
    pub tombstones: usize,
    /// The customer-table Puffin statistics-file location.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blob_location: Option<String>,
    /// The persisted Puffin file size in bytes.
    pub blob_bytes: usize,
}

/// One nearest neighbor: id and distance.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SearchHit {
    /// The neighbor's id (the source row's identity value).
    pub id: i64,
    /// The distance under the index metric; smaller is closer.
    pub distance: f32,
}

/// `POST /v1/tables/{t}/indexes/{field}/search` body.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SearchRequest {
    /// The query vector.
    pub vector: Vec<f32>,
    /// The number of neighbors to return.
    pub k: usize,
    /// The candidate-list size `L` (defaults to `max(k, R)` when omitted).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub l: Option<usize>,
}

/// The indexed ANN search response.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SearchResponse {
    /// Always `"index"`; a missing exact-snapshot attachment is an error.
    pub source: String,
    /// Nearest-first neighbors.
    pub neighbors: Vec<SearchHit>,
}

/// One declared index, as listed by `GET /v1/tables/{t}/indexes`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IndexInfo {
    /// The logical target.
    pub target: String,
    /// The indexed field.
    pub field: String,
    /// The metric.
    pub metric: String,
    /// The reflected source snapshot, if built.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reflected_snapshot: Option<i64>,
    /// Live vector count, if built.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub live_count: Option<usize>,
}

/// The `GET .../indexes` response.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IndexListResponse {
    /// The declared indexes.
    pub indexes: Vec<IndexInfo>,
}
