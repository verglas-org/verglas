//! The domain model of the property graph: nodes, edges, and the shapes
//! traversal returns.
//!
//! A node is an entity with labels and a JSON property bag. An edge is a
//! directed, labeled, property-carrying assertion (a triplet: subject
//! `src_id`, predicate, object `dst_id`) with a confidence and a required
//! provenance — the memory that asserted it — plus an optional `supersedes`
//! pointer that retracts an earlier assertion (belief revision, append-only).
//! These types are the engine's public vocabulary; the CLI/SDK and the memory
//! pipeline build and read them.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// The direction of traversal relative to a node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Direction {
    /// Follow edges where the node is the subject (`src_id`), toward objects.
    Out,
    /// Follow edges where the node is the object (`dst_id`), toward subjects.
    In,
    /// Follow edges in either direction.
    Both,
}

/// A node (entity) of the property graph.
#[derive(Debug, Clone, PartialEq)]
pub struct Node {
    /// The stable entity id (the subject/object reference edges point at).
    pub id: String,
    /// Zero or more labels (the entity's kind / type tags).
    pub labels: Vec<String>,
    /// Arbitrary typed properties.
    pub properties: Map<String, Value>,
    /// The owning agent, when the graph is agent-scoped. Stored, not filtered on
    /// by the engine.
    pub agent_id: Option<String>,
    /// The namespace scope, when set. Stored, not filtered on by the engine.
    pub namespace: Option<String>,
}

impl Node {
    /// A node with just an id and no labels or properties.
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            labels: Vec::new(),
            properties: Map::new(),
            agent_id: None,
            namespace: None,
        }
    }

    /// Adds a label, returning the node for chaining.
    pub fn with_label(mut self, label: impl Into<String>) -> Self {
        self.labels.push(label.into());
        self
    }
}

/// An edge (triplet): a directed labeled assertion from `src_id` to `dst_id`.
///
/// Append-only: an edge is never mutated. `supersedes`, when set, names the
/// `edge_id` of an earlier assertion this row retracts — belief revision is a
/// new row, so time-travel is preserved (an older snapshot still sees the old
/// belief). See `schema.rs` for why this is the inverse of #83's
/// `superseded_by` pointer.
#[derive(Debug, Clone, PartialEq)]
pub struct Edge {
    /// The unique id of this assertion.
    pub edge_id: String,
    /// The subject entity id.
    pub src_id: String,
    /// The predicate (a namespaced verb, e.g. `experiment/produced`).
    pub predicate: String,
    /// The object entity id.
    pub dst_id: String,
    /// Confidence in the assertion, in [0, 1].
    pub confidence: f64,
    /// The id of the memory (or source) that asserted this edge — required, so
    /// every edge traces to a provenance receipt.
    pub provenance: String,
    /// The `edge_id` this assertion supersedes, if it revises an earlier belief.
    pub supersedes: Option<String>,
    /// A caller-supplied logical valid-from time, if any.
    pub valid_from: Option<i64>,
    /// The owning agent, when agent-scoped. Stored, not filtered on by the engine.
    pub agent_id: Option<String>,
    /// The namespace scope, when set. Stored, not filtered on by the engine.
    pub namespace: Option<String>,
    /// Arbitrary edge properties.
    pub properties: Map<String, Value>,
}

impl Edge {
    /// A minimal edge with a fresh random `edge_id`, confidence 1.0, and the
    /// given provenance. The caller fills the rest as needed.
    pub fn new(
        src_id: impl Into<String>,
        predicate: impl Into<String>,
        dst_id: impl Into<String>,
        provenance: impl Into<String>,
    ) -> Self {
        Self {
            edge_id: uuid::Uuid::new_v4().to_string(),
            src_id: src_id.into(),
            predicate: predicate.into(),
            dst_id: dst_id.into(),
            confidence: 1.0,
            provenance: provenance.into(),
            supersedes: None,
            valid_from: None,
            agent_id: None,
            namespace: None,
            properties: Map::new(),
        }
    }

    /// Sets the confidence, returning the edge for chaining.
    pub fn with_confidence(mut self, confidence: f64) -> Self {
        self.confidence = confidence;
        self
    }

    /// Marks this edge as superseding `edge_id`, returning it for chaining.
    pub fn superseding(mut self, edge_id: impl Into<String>) -> Self {
        self.supersedes = Some(edge_id.into());
        self
    }
}

/// One neighbor reached by `get_neighbors`: the adjacent node and the edge that
/// connects it, carrying the receipt (edge id + provenance) so the result is
/// explainable.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Neighbor {
    /// The adjacent node's id.
    pub node_id: String,
    /// The predicate of the connecting edge.
    pub predicate: String,
    /// The confidence of the connecting edge.
    pub confidence: f64,
    /// The connecting edge's id.
    pub edge_id: String,
    /// The connecting edge's provenance (the memory that asserted it).
    pub provenance: String,
    /// Whether the edge was followed out of, or into, the queried node.
    pub direction: Direction,
}

/// A node reached by `k_hop`, with its hop distance from the start and the
/// product of edge confidences along the path that reached it. The per-hop
/// decay factor (γ) is the caller's policy — the engine returns the raw product
/// and the hop count.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Reached {
    /// The reached node's id.
    pub node_id: String,
    /// The number of hops from the start node (the start itself is 0).
    pub hops: u32,
    /// The product of edge confidences along the discovering path.
    pub path_confidence: f64,
}

/// A triplet with its receipt — the edge form returned inside a subgraph or a
/// path.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TripletReceipt {
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

/// A bounded neighborhood: the set of nodes reached within k hops and the edges
/// among them that were traversed.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Subgraph {
    /// The reached nodes, with hop distance and path confidence.
    pub nodes: Vec<Reached>,
    /// The traversed edges, each a triplet receipt.
    pub edges: Vec<TripletReceipt>,
}

/// A path between two nodes: the node id sequence, the edges taken, and the
/// product of edge confidences.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Path {
    /// The node ids from source to destination, in order.
    pub nodes: Vec<String>,
    /// The edges taken between consecutive nodes.
    pub edges: Vec<TripletReceipt>,
    /// The product of edge confidences along the path.
    pub confidence: f64,
}
