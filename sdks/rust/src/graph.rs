//! The `graph` verb-family wire types: the request bodies and result reports the
//! server's `/v1/graphs/...` routes serve and the CLI and TypeScript SDK speak.
//!
//! A graph is a namespace holding two plain Iceberg tables (`<ns>.nodes` and
//! `<ns>.edges`) plus a snapshot-bound Puffin adjacency index. These types are
//! the transport contract only — they mirror the `verglas-graph` engine's model
//! (`Node`, `Edge`, `Neighbor`, `Reached`, `Path`, `Subgraph`) but stay
//! independent of it so the CLI (which never opens a catalog) and external
//! callers depend on the SDK, not the engine. The server owns the conversion
//! between these shapes and the engine types.
//!
//! Every field is camelCase on the wire, matching the `tables_api` SDK shapes,
//! so the TypeScript `Graph` handle and the Rust CLI read the same JSON.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// The default confidence for an edge whose body omits it: full confidence,
/// mirroring the engine's `Edge::new`.
fn default_confidence() -> f64 {
    1.0
}

/// The traversal direction relative to a node, as it travels on the wire.
/// Mirrors the engine's `Direction`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum GraphDirection {
    /// Follow edges where the node is the subject (`srcId`), toward objects.
    #[default]
    Out,
    /// Follow edges where the node is the object (`dstId`), toward subjects.
    In,
    /// Follow edges in either direction.
    Both,
}

/// A node to insert, as the `nodes` route receives it. `id` is required; the
/// rest default to empty/absent so a minimal `{ "id": "n1" }` is valid.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeInput {
    /// The stable entity id.
    pub id: String,
    /// Zero or more labels (the entity's kind tags).
    #[serde(default)]
    pub labels: Vec<String>,
    /// Arbitrary typed properties.
    #[serde(default)]
    pub properties: Map<String, Value>,
    /// The owning agent, when agent-scoped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    /// The namespace scope, when set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

/// An edge (triplet) to insert, as the `edges` route receives it. `srcId`,
/// `predicate`, `dstId`, and `provenance` are required; `edgeId` is generated
/// by the engine when absent and `confidence` defaults to 1.0.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EdgeInput {
    /// The assertion id. Absent means the engine assigns a fresh one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub edge_id: Option<String>,
    /// The subject entity id.
    pub src_id: String,
    /// The predicate (a namespaced verb).
    pub predicate: String,
    /// The object entity id.
    pub dst_id: String,
    /// Confidence in [0, 1]; defaults to 1.0 when omitted.
    #[serde(default = "default_confidence")]
    pub confidence: f64,
    /// The asserting memory (or source) id — required, so every edge traces to
    /// a provenance receipt.
    pub provenance: String,
    /// The `edgeId` this row supersedes, if it revises an earlier belief.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supersedes: Option<String>,
    /// A caller-supplied logical valid-from time, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub valid_from: Option<i64>,
    /// The owning agent, when agent-scoped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    /// The namespace scope, when set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    /// Arbitrary edge properties.
    #[serde(default)]
    pub properties: Map<String, Value>,
}

/// The body of `POST /v1/graphs/{ns}/nodes`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct InsertNodesRequest {
    /// The nodes to append.
    pub nodes: Vec<NodeInput>,
}

/// The body of `POST /v1/graphs/{ns}/edges`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct InsertEdgesRequest {
    /// The edges to append.
    pub edges: Vec<EdgeInput>,
}

/// The body of `POST /v1/graphs/{ns}/index`: build/refresh the adjacency index,
/// optionally as of a specific edge snapshot (the current one when absent).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BuildIndexRequest {
    /// The edge-table snapshot to index as of; `None` = the current tip.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub at: Option<i64>,
}

/// The traversal operation a graph query runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum GraphOp {
    /// Direct neighbors of a node (`GraphReader::get_neighbors`).
    Neighbors,
    /// Every node reached within `k` hops (`GraphReader::k_hop`).
    KHop,
    /// The bounded neighborhood: reached nodes and traversed edges
    /// (`GraphReader::neighborhood`).
    Neighborhood,
    /// The shortest path between two nodes (`GraphReader::paths`).
    Paths,
}

/// The predicate/confidence filter applied during a traversal. Mirrors the
/// engine's `TraversalFilter`; an empty filter follows every edge.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GraphFilter {
    /// Only follow edges with this predicate, when set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub predicate: Option<String>,
    /// Only follow edges whose confidence is at least this, when set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_confidence: Option<f64>,
}

/// The body of `POST /v1/graphs/{ns}/query`: a traversal request. Which of `k`,
/// `maxHops`, and `dst` are read depends on `op`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GraphQueryRequest {
    /// Which traversal to run.
    pub op: GraphOp,
    /// The anchor node (the `src` for `paths`).
    pub start: String,
    /// The destination node for `paths`; ignored by the other ops.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dst: Option<String>,
    /// The direction to follow edges; defaults to `out`.
    #[serde(default)]
    pub direction: GraphDirection,
    /// The hop bound for `kHop` and `neighborhood`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub k: Option<u32>,
    /// The hop bound for `paths`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_hops: Option<u32>,
    /// The predicate/confidence filter.
    #[serde(default)]
    pub filter: GraphFilter,
    /// Read the graph as of this edge snapshot (time travel); the current tip
    /// when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub as_of: Option<i64>,
}

/// One neighbor in a `neighbors` result. Mirrors the engine's `Neighbor`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NeighborView {
    /// The adjacent node's id.
    pub node_id: String,
    /// The predicate of the connecting edge.
    pub predicate: String,
    /// The confidence of the connecting edge.
    pub confidence: f64,
    /// The connecting edge's id.
    pub edge_id: String,
    /// The connecting edge's provenance.
    pub provenance: String,
    /// Whether the edge was followed out of, or into, the queried node.
    pub direction: GraphDirection,
}

/// One reached node in a `kHop` result. Mirrors the engine's `Reached`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReachedView {
    /// The reached node's id.
    pub node_id: String,
    /// The number of hops from the start (the start itself is 0).
    pub hops: u32,
    /// The product of edge confidences along the discovering path.
    pub path_confidence: f64,
}

/// A triplet with its receipt, as returned inside a subgraph or a path. Mirrors
/// the engine's `TripletReceipt`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TripletView {
    /// The subject entity id.
    pub src_id: String,
    /// The predicate.
    pub predicate: String,
    /// The object entity id.
    pub dst_id: String,
    /// The edge confidence.
    pub confidence: f64,
    /// The edge id.
    pub edge_id: String,
    /// The provenance (asserting memory) of the edge.
    pub provenance: String,
}

/// A bounded neighborhood in a `neighborhood` result. Mirrors the engine's
/// `Subgraph`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubgraphView {
    /// The reached nodes, with hop distance and path confidence.
    pub nodes: Vec<ReachedView>,
    /// The traversed edges, each a triplet receipt.
    pub edges: Vec<TripletView>,
}

/// A path in a `paths` result. Mirrors the engine's `Path`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PathView {
    /// The node ids from source to destination, in order.
    pub nodes: Vec<String>,
    /// The edges taken between consecutive nodes.
    pub edges: Vec<TripletView>,
    /// The product of edge confidences along the path.
    pub confidence: f64,
}

/// Which path served a query — the bound Puffin index, or a table scan. The
/// turn-off assertion checks the two agree.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum GraphBackend {
    /// Served from the snapshot-bound adjacency index.
    Index,
    /// Served from a scan of the edge table (no usable index).
    Scan,
}

/// The result of `POST /v1/graphs/{ns}/query`. The field matching `op` is
/// populated and the rest are absent, so the caller reads by `op`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GraphQueryResponse {
    /// The operation that ran.
    pub op: GraphOp,
    /// Which path served it (index or scan).
    pub backend: GraphBackend,
    /// The edge snapshot the read reflected.
    pub snapshot_id: i64,
    /// The `neighbors` result, when `op == neighbors`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub neighbors: Option<Vec<NeighborView>>,
    /// The `kHop` result, when `op == kHop`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reached: Option<Vec<ReachedView>>,
    /// The `neighborhood` result, when `op == neighborhood`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subgraph: Option<SubgraphView>,
    /// The `paths` result, when `op == paths`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub paths: Option<Vec<PathView>>,
}

/// The result of `POST /v1/graphs/{ns}` (create): the namespace and the two
/// plain Iceberg tables that back it.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GraphCreateReport {
    /// The graph's namespace.
    pub namespace: String,
    /// The nodes table, as `namespace.nodes`.
    pub nodes_table: String,
    /// The edges table, as `namespace.edges`.
    pub edges_table: String,
}

/// The result of a node or edge insert: the new snapshot id (absent when no
/// rows were given) and how many rows were appended.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InsertReport {
    /// The new snapshot id of the written table, or `null` for an empty insert.
    pub snapshot_id: Option<i64>,
    /// The number of rows appended.
    pub count: usize,
}

/// The result of `POST /v1/graphs/{ns}/index`: what the build bound to, or
/// `built: false` when the edge table has no data to index.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IndexReport {
    /// Whether an index was built (false when the edge table is empty).
    pub built: bool,
    /// The edge snapshot the index bound to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_id: Option<i64>,
    /// The number of distinct nodes indexed.
    pub node_count: usize,
    /// The number of live edges indexed.
    pub edge_count: usize,
    /// The Puffin blob path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blob_path: Option<String>,
    /// The Puffin file size in bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blob_bytes: Option<u64>,
    /// How the index was built (`full` today).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
}

/// The result of `GET /v1/graphs/{ns}` (show): the backing tables, their live
/// counts, and whether an index is bound to the current edge snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GraphShowReport {
    /// The graph's namespace.
    pub namespace: String,
    /// The nodes table, as `namespace.nodes`.
    pub nodes_table: String,
    /// The edges table, as `namespace.edges`.
    pub edges_table: String,
    /// The live node-table row count, when available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_count: Option<u64>,
    /// The live edge-table row count, when available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub edge_count: Option<u64>,
    /// Whether an adjacency index is bound to the current edge snapshot.
    pub indexed: bool,
    /// The current edge-table snapshot id, or `null` when the graph is empty.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_id: Option<i64>,
}
