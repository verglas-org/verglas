//! Traversal primitives over an [`AdjacencyIndex`]: neighbors, k-hop expansion,
//! bounded neighborhood extraction, and a bounded shortest path.
//!
//! Every primitive runs over the one `AdjacencyIndex` structure, so it does not
//! matter whether that index was decoded from a Puffin blob or built in memory
//! from a table scan — the results are identical. k-hop is a breadth-first
//! search; path confidence is the product of edge confidences (the caller
//! applies any per-hop decay).

use std::collections::HashMap;
use std::collections::VecDeque;

use crate::csr::{AdjEntry, AdjacencyIndex};
use crate::model::{Direction, Neighbor, Path, Reached, Subgraph, TripletReceipt};

/// A filter applied while traversing. The default filters nothing.
#[derive(Debug, Clone, Default)]
pub struct TraversalFilter {
    /// Only follow edges with this predicate, when set.
    pub predicate: Option<String>,
    /// Only follow edges whose confidence is at least this, when set.
    pub min_confidence: Option<f64>,
}

impl TraversalFilter {
    /// A filter that follows only edges with the given predicate.
    pub fn predicate(name: impl Into<String>) -> Self {
        Self {
            predicate: Some(name.into()),
            min_confidence: None,
        }
    }
}

/// One adjacent step: the neighbor's dense index, the connecting edge's index,
/// and the direction the edge was followed.
struct Step {
    /// The adjacent node's dense index.
    neighbor: u32,
    /// The connecting edge's meta index.
    edge: u32,
    /// Whether the edge was followed out of, or into, the current node.
    direction: Direction,
}

/// Returns the direct neighbors of `node_id` in the given direction, filtered by
/// predicate, each carrying its edge receipt. An unknown node (no incident
/// edges) yields an empty list.
pub fn get_neighbors(
    index: &AdjacencyIndex,
    node_id: &str,
    direction: Direction,
    filter: &TraversalFilter,
) -> Vec<Neighbor> {
    let Some(node) = index.node_index(node_id) else {
        return Vec::new();
    };
    steps(index, node, direction, filter)
        .into_iter()
        .filter_map(|step| neighbor_of(index, &step))
        .collect()
}

/// Expands k hops from `start` in the given direction, returning every node
/// reached at 1..=`k` hops with its hop distance and path confidence (the
/// product of edge confidences on the discovering shortest path). The start
/// node is the anchor and is not included in the result.
pub fn k_hop(
    index: &AdjacencyIndex,
    start: &str,
    k: u32,
    direction: Direction,
    filter: &TraversalFilter,
) -> Vec<Reached> {
    let mut reached = bfs(index, start, k, direction, filter).reached;
    // Drop the anchor and return in a deterministic (hops, node) order.
    if let Some(start_idx) = index.node_index(start) {
        reached.remove(&start_idx);
    }
    let mut out: Vec<Reached> = reached
        .into_iter()
        .filter_map(|(node, state)| {
            index.node_id(node).map(|id| Reached {
                node_id: id.to_owned(),
                hops: state.hops,
                path_confidence: state.confidence,
            })
        })
        .collect();
    out.sort_by(|a, b| (a.hops, a.node_id.as_str()).cmp(&(b.hops, b.node_id.as_str())));
    out
}

/// Extracts the bounded neighborhood within `k` hops of `start`: the reached
/// nodes (as in [`k_hop`], plus the anchor at hop 0) and the edges traversed to
/// reach them, each an explainable triplet receipt.
pub fn neighborhood(
    index: &AdjacencyIndex,
    start: &str,
    k: u32,
    direction: Direction,
    filter: &TraversalFilter,
) -> Subgraph {
    let search = bfs(index, start, k, direction, filter);
    let mut nodes: Vec<Reached> = search
        .reached
        .iter()
        .filter_map(|(node, state)| {
            index.node_id(*node).map(|id| Reached {
                node_id: id.to_owned(),
                hops: state.hops,
                path_confidence: state.confidence,
            })
        })
        .collect();
    nodes.sort_by(|a, b| (a.hops, a.node_id.as_str()).cmp(&(b.hops, b.node_id.as_str())));

    let mut edges: Vec<TripletReceipt> = search
        .edges
        .iter()
        .filter_map(|edge| receipt(index, *edge))
        .collect();
    edges.sort_by(|a, b| a.edge_id.cmp(&b.edge_id));
    edges.dedup_by(|a, b| a.edge_id == b.edge_id);

    Subgraph { nodes, edges }
}

/// Finds the shortest path from `src` to `dst` within `max_hops`, if one exists,
/// returning it as a single-element vector (empty when unreachable). Following
/// the CSR's sorted neighbor order makes the chosen shortest path deterministic.
pub fn paths(
    index: &AdjacencyIndex,
    src: &str,
    dst: &str,
    max_hops: u32,
    direction: Direction,
    filter: &TraversalFilter,
) -> Vec<Path> {
    let (Some(src_idx), Some(dst_idx)) = (index.node_index(src), index.node_index(dst)) else {
        return Vec::new();
    };
    if src_idx == dst_idx {
        return vec![Path {
            nodes: vec![src.to_owned()],
            edges: Vec::new(),
            confidence: 1.0,
        }];
    }

    // BFS with predecessor tracking: each node remembers the node and edge it
    // was first (shortest) reached from.
    let mut pred: HashMap<u32, (u32, u32)> = HashMap::new();
    let mut visited: HashMap<u32, u32> = HashMap::new();
    visited.insert(src_idx, 0);
    let mut queue: VecDeque<u32> = VecDeque::new();
    queue.push_back(src_idx);

    while let Some(node) = queue.pop_front() {
        let hops = visited.get(&node).copied().unwrap_or(0);
        if hops >= max_hops {
            continue;
        }
        for step in steps(index, node, direction, filter) {
            if visited.contains_key(&step.neighbor) {
                continue;
            }
            visited.insert(step.neighbor, hops + 1);
            pred.insert(step.neighbor, (node, step.edge));
            if step.neighbor == dst_idx {
                return reconstruct(index, src_idx, dst_idx, &pred)
                    .into_iter()
                    .collect();
            }
            queue.push_back(step.neighbor);
        }
    }
    Vec::new()
}

/// Rebuilds the path from `src` to `dst` by walking the predecessor map back,
/// producing the node id sequence, the edge receipts, and the confidence
/// product.
fn reconstruct(
    index: &AdjacencyIndex,
    src: u32,
    dst: u32,
    pred: &HashMap<u32, (u32, u32)>,
) -> Option<Path> {
    let mut node_seq = vec![dst];
    let mut edge_seq = Vec::new();
    let mut current = dst;
    while current != src {
        let (prev, edge) = pred.get(&current).copied()?;
        edge_seq.push(edge);
        node_seq.push(prev);
        current = prev;
    }
    node_seq.reverse();
    edge_seq.reverse();

    let nodes: Vec<String> = node_seq
        .into_iter()
        .filter_map(|n| index.node_id(n).map(str::to_owned))
        .collect();
    let mut confidence = 1.0;
    let mut edges = Vec::with_capacity(edge_seq.len());
    for edge in edge_seq {
        let r = receipt(index, edge)?;
        confidence *= r.confidence;
        edges.push(r);
    }
    Some(Path {
        nodes,
        edges,
        confidence,
    })
}

/// The BFS state carried per reached node.
struct NodeState {
    /// Hops from the start (0 for the start itself).
    hops: u32,
    /// The product of edge confidences on the discovering path.
    confidence: f64,
}

/// The result of a bounded BFS: the reached nodes and the edges traversed.
struct Search {
    /// Reached node index → its shortest-hop state.
    reached: HashMap<u32, NodeState>,
    /// The edge indices traversed during expansion.
    edges: Vec<u32>,
}

/// A bounded breadth-first search from `start` up to `k` hops. Records each
/// node at its minimum hop distance with the best (largest) confidence product
/// at that distance, and every edge followed from a node inside the bound.
fn bfs(
    index: &AdjacencyIndex,
    start: &str,
    k: u32,
    direction: Direction,
    filter: &TraversalFilter,
) -> Search {
    let mut reached: HashMap<u32, NodeState> = HashMap::new();
    let mut edges: Vec<u32> = Vec::new();
    let Some(start_idx) = index.node_index(start) else {
        return Search { reached, edges };
    };
    reached.insert(
        start_idx,
        NodeState {
            hops: 0,
            confidence: 1.0,
        },
    );
    // Layered BFS so a node is fixed at its minimum hop distance.
    let mut frontier = vec![start_idx];
    for hop in 1..=k {
        let mut next: Vec<u32> = Vec::new();
        for node in frontier.drain(..) {
            let parent_conf = reached.get(&node).map(|s| s.confidence).unwrap_or(1.0);
            for step in steps(index, node, direction, filter) {
                let conf = parent_conf * edge_confidence(index, step.edge);
                edges.push(step.edge);
                match reached.get_mut(&step.neighbor) {
                    Some(existing) if existing.hops == hop => {
                        // Same layer: keep the higher-confidence discovery.
                        if conf > existing.confidence {
                            existing.confidence = conf;
                        }
                    }
                    Some(_) => {}
                    None => {
                        reached.insert(
                            step.neighbor,
                            NodeState {
                                hops: hop,
                                confidence: conf,
                            },
                        );
                        next.push(step.neighbor);
                    }
                }
            }
        }
        frontier = next;
        if frontier.is_empty() {
            break;
        }
    }
    Search { reached, edges }
}

/// The adjacency steps out of a node in the given direction, honoring the
/// predicate and confidence filters. For [`Direction::Both`], out-edges precede
/// in-edges.
fn steps(
    index: &AdjacencyIndex,
    node: u32,
    direction: Direction,
    filter: &TraversalFilter,
) -> Vec<Step> {
    // Resolve a predicate filter to its dense index once; a predicate the graph
    // does not use matches nothing.
    let predicate = match filter.predicate.as_deref() {
        Some(name) => match index.predicate_index(name) {
            Some(p) => Some(p),
            None => return Vec::new(),
        },
        None => None,
    };
    let floor = filter.min_confidence;
    let mut out = Vec::new();
    let mut push = |entries: &[AdjEntry], dir: Direction| {
        for entry in entries {
            let Some(meta) = index.edge_meta(entry.edge) else {
                continue;
            };
            if let Some(p) = predicate
                && meta.predicate != p
            {
                continue;
            }
            if let Some(min) = floor
                && meta.confidence < min
            {
                continue;
            }
            out.push(Step {
                neighbor: entry.neighbor,
                edge: entry.edge,
                direction: dir,
            });
        }
    };
    match direction {
        Direction::Out => push(index.out_entries(node), Direction::Out),
        Direction::In => push(index.in_entries(node), Direction::In),
        Direction::Both => {
            push(index.out_entries(node), Direction::Out);
            push(index.in_entries(node), Direction::In);
        }
    }
    out
}

/// The confidence of an edge index, defaulting to 1.0 for a missing meta (never
/// happens for a well-formed index; keeps the math total).
fn edge_confidence(index: &AdjacencyIndex, edge: u32) -> f64 {
    index.edge_meta(edge).map(|m| m.confidence).unwrap_or(1.0)
}

/// Builds a [`Neighbor`] from an adjacency step.
fn neighbor_of(index: &AdjacencyIndex, step: &Step) -> Option<Neighbor> {
    let meta = index.edge_meta(step.edge)?;
    let node_id = index.node_id(step.neighbor)?.to_owned();
    let predicate = index.predicate_name(meta.predicate)?.to_owned();
    Some(Neighbor {
        node_id,
        predicate,
        confidence: meta.confidence,
        edge_id: meta.edge_id.clone(),
        provenance: meta.provenance.clone(),
        direction: step.direction,
    })
}

/// Builds a full triplet receipt from an edge index, resolving the subject and
/// object ids the edge meta carries.
fn receipt(index: &AdjacencyIndex, edge: u32) -> Option<TripletReceipt> {
    let meta = index.edge_meta(edge)?;
    let predicate = index.predicate_name(meta.predicate)?.to_owned();
    let src_id = index.node_id(meta.src)?.to_owned();
    let dst_id = index.node_id(meta.dst)?.to_owned();
    Some(TripletReceipt {
        src_id,
        predicate,
        dst_id,
        confidence: meta.confidence,
        edge_id: meta.edge_id.clone(),
        provenance: meta.provenance.clone(),
    })
}
