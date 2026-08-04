//! Hermetic acceptance tests for the graph-over-Iceberg engine.
//!
//! All in-process against an Iceberg `MemoryCatalog` over a temp warehouse (the
//! same fixture the verglas-iceberg tests use) — no server, no MinIO, no
//! network. They prove the load-bearing properties from #258:
//!
//! - index-vs-scan equivalence: k-hop / neighbors from the Puffin index match a
//!   brute-force edge-scan reference exactly (the core correctness test);
//! - time-travel: the graph AS OF an older snapshot excludes later edges;
//! - turn-off: with no index bound, queries still return correct results via the
//!   scan, just slower;
//! - belief revision: a `supersedes` row retracts an edge, and time-travel still
//!   sees the old belief.

use std::collections::BTreeSet;
use std::collections::HashMap;
use std::sync::Arc;

use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::{Catalog, CatalogBuilder};
use verglas_graph::{Direction, Edge, Graph, Node, ReaderBackend, TraversalFilter};

/// An in-process memory catalog over a fresh temp warehouse. The tempdir guard
/// is leaked because the test process is short-lived.
async fn memory_catalog() -> Arc<dyn Catalog> {
    let warehouse = tempfile::tempdir().expect("warehouse tempdir");
    let catalog = MemoryCatalogBuilder::default()
        .load(
            "memory",
            HashMap::from([(
                MEMORY_CATALOG_WAREHOUSE.to_string(),
                warehouse.path().to_str().expect("utf8 path").to_string(),
            )]),
        )
        .await
        .expect("memory catalog");
    std::mem::forget(warehouse);
    Arc::new(catalog)
}

/// An edge with a fixed id and confidence so tests can assert receipts exactly.
fn edge(id: &str, src: &str, pred: &str, dst: &str, conf: f64) -> Edge {
    let mut e = Edge::new(src, pred, dst, format!("mem-{id}"));
    e.edge_id = id.to_owned();
    e.confidence = conf;
    e
}

/// A small fixture graph: a diamond plus a typed side edge.
///
/// a -knows-> b -knows-> d ; a -knows-> c -knows-> d ; a -likes-> c
fn fixture_edges() -> Vec<Edge> {
    vec![
        edge("e1", "a", "knows", "b", 0.9),
        edge("e2", "a", "knows", "c", 0.8),
        edge("e3", "b", "knows", "d", 0.7),
        edge("e4", "c", "knows", "d", 0.6),
        edge("e5", "a", "likes", "c", 0.5),
    ]
}

/// The set of (node, hops) pairs a reader reaches from `start`, for comparison.
fn reach_set(reader: &verglas_graph::GraphReader, start: &str, k: u32) -> BTreeSet<(String, u32)> {
    reader
        .k_hop(start, k, Direction::Out, &TraversalFilter::default())
        .into_iter()
        .map(|r| (r.node_id, r.hops))
        .collect()
}

/// Builds a graph, inserts the fixture edges, and returns the graph plus the
/// edges-table snapshot.
async fn fixture_graph() -> (Graph, i64) {
    let catalog = memory_catalog().await;
    let graph = Graph::open(catalog, "kg").expect("open");
    graph.ensure_tables().await.expect("ensure tables");
    graph
        .insert_nodes(&[
            Node::new("a"),
            Node::new("b"),
            Node::new("c"),
            Node::new("d"),
        ])
        .await
        .expect("insert nodes");
    let snapshot = graph
        .insert_edges(&fixture_edges())
        .await
        .expect("insert edges")
        .expect("snapshot");
    (graph, snapshot)
}

/// Core correctness: the Puffin index and the brute-force scan return identical
/// k-hop results, and the index path is actually used.
#[tokio::test]
async fn index_matches_scan_equivalence() {
    let (graph, snapshot) = fixture_graph().await;
    let report = graph
        .build_index(Some(snapshot))
        .await
        .expect("build index")
        .expect("indexed");
    assert_eq!(report.node_count, 4);
    assert_eq!(report.edge_count, 5);
    assert_eq!(report.snapshot_id, snapshot);

    let indexed = graph.reader(Some(snapshot)).await.expect("index reader");
    assert_eq!(indexed.backend(), ReaderBackend::Index);

    let scanned = graph
        .reader_scan_only(Some(snapshot))
        .await
        .expect("scan reader");
    assert_eq!(scanned.backend(), ReaderBackend::Scan);

    // 2-hop out from `a` reaches b,c at hop 1 and d at hop 2 — identical both ways.
    let from_index = reach_set(&indexed, "a", 2);
    let from_scan = reach_set(&scanned, "a", 2);
    assert_eq!(from_index, from_scan);
    let expected: BTreeSet<(String, u32)> = [
        ("b".to_owned(), 1),
        ("c".to_owned(), 1),
        ("d".to_owned(), 2),
    ]
    .into_iter()
    .collect();
    assert_eq!(from_index, expected);

    // Neighbor receipts match exactly between index and scan.
    let ni = indexed.get_neighbors("a", Direction::Out, &TraversalFilter::default());
    let ns = scanned.get_neighbors("a", Direction::Out, &TraversalFilter::default());
    assert_eq!(ni, ns);
    // a has three out-edges: knows b, knows c, likes c.
    assert_eq!(ni.len(), 3);
    let ids: BTreeSet<&str> = ni.iter().map(|n| n.edge_id.as_str()).collect();
    assert_eq!(ids, ["e1", "e2", "e5"].into_iter().collect());
}

/// Predicate filter and path-confidence product agree across index and scan.
#[tokio::test]
async fn predicate_filter_and_confidence_match() {
    let (graph, snapshot) = fixture_graph().await;
    graph.build_index(Some(snapshot)).await.expect("build");

    let indexed = graph.reader(Some(snapshot)).await.expect("reader");
    let scanned = graph
        .reader_scan_only(Some(snapshot))
        .await
        .expect("scan reader");

    let filter = TraversalFilter::predicate("knows");
    let ki = indexed.k_hop("a", 2, Direction::Out, &filter);
    let ks = scanned.k_hop("a", 2, Direction::Out, &filter);
    assert_eq!(ki, ks);

    // Only `knows` edges: a->b (0.9), a->c (0.8), then ->d. `likes` is excluded.
    let d = ki.iter().find(|r| r.node_id == "d").expect("reaches d");
    assert_eq!(d.hops, 2);
    // Best path to d is a->b->d = 0.9 * 0.7 = 0.63 (beats a->c->d = 0.8*0.6=0.48).
    assert!((d.path_confidence - 0.63).abs() < 1e-9);

    // The bounded shortest path a..d carries full receipts and the confidence
    // product, identical between backends.
    let pi = indexed.paths("a", "d", 3, Direction::Out, &filter);
    let ps = scanned.paths("a", "d", 3, Direction::Out, &filter);
    assert_eq!(pi, ps);
    let path = pi.first().expect("a path exists");
    assert_eq!(path.nodes, vec!["a", "b", "d"]);
    assert!((path.confidence - 0.63).abs() < 1e-9);
    assert_eq!(path.edges.first().expect("first edge").src_id, "a");
    assert_eq!(path.edges.first().expect("first edge").dst_id, "b");
}

/// Time-travel: querying AS OF the older snapshot excludes edges added later.
#[tokio::test]
async fn time_travel_excludes_later_edges() {
    let (graph, snap1) = fixture_graph().await;
    // A second commit extends the graph: d -knows-> e.
    let snap2 = graph
        .insert_edges(&[edge("e6", "d", "knows", "e", 1.0)])
        .await
        .expect("insert")
        .expect("snapshot");
    assert_ne!(snap1, snap2);

    // Build an index at each snapshot so both the index and scan paths are tested.
    graph.build_index(Some(snap1)).await.expect("index snap1");
    graph.build_index(Some(snap2)).await.expect("index snap2");

    // As of snap1, e is unreachable (its edge does not exist yet).
    let old = graph.reader(Some(snap1)).await.expect("reader snap1");
    assert_eq!(old.backend(), ReaderBackend::Index);
    let old_reach: BTreeSet<String> = old
        .k_hop("a", 5, Direction::Out, &TraversalFilter::default())
        .into_iter()
        .map(|r| r.node_id)
        .collect();
    assert!(!old_reach.contains("e"));
    assert!(old_reach.contains("d"));

    // As of snap2, e is reachable at hop 3.
    let new = graph.reader(Some(snap2)).await.expect("reader snap2");
    let e = new
        .k_hop("a", 5, Direction::Out, &TraversalFilter::default())
        .into_iter()
        .find(|r| r.node_id == "e")
        .expect("e reachable at snap2");
    assert_eq!(e.hops, 3);

    // The old-snapshot scan path agrees with the old-snapshot index.
    let old_scan = graph.reader_scan_only(Some(snap1)).await.expect("scan");
    let old_scan_reach: BTreeSet<String> = old_scan
        .k_hop("a", 5, Direction::Out, &TraversalFilter::default())
        .into_iter()
        .map(|r| r.node_id)
        .collect();
    assert_eq!(old_reach, old_scan_reach);
}

/// Turn-off: with no index ever built, queries still answer correctly via scan.
#[tokio::test]
async fn turn_off_no_index_still_answers() {
    let (graph, snapshot) = fixture_graph().await;
    // No build_index call at all.
    let reader = graph.reader(Some(snapshot)).await.expect("reader");
    // With nothing bound, reader() falls back to the scan path.
    assert_eq!(reader.backend(), ReaderBackend::Scan);
    let reach = reach_set(&reader, "a", 2);
    let expected: BTreeSet<(String, u32)> = [
        ("b".to_owned(), 1),
        ("c".to_owned(), 1),
        ("d".to_owned(), 2),
    ]
    .into_iter()
    .collect();
    assert_eq!(reach, expected);
}

/// Belief revision: a `supersedes` row retracts an edge from the live set, and
/// time-travel still sees the old belief at the earlier snapshot.
#[tokio::test]
async fn supersede_retracts_and_time_travels() {
    let catalog = memory_catalog().await;
    let graph = Graph::open(catalog, "kg").expect("open");
    graph.ensure_tables().await.expect("ensure");

    // Assert a -role-> engineer.
    let snap1 = graph
        .insert_edges(&[edge("r1", "alice", "role", "engineer", 1.0)])
        .await
        .expect("insert")
        .expect("snap");

    // Supersede it: a -role-> manager, retracting r1.
    let mut revision = edge("r2", "alice", "role", "manager", 1.0);
    revision.supersedes = Some("r1".to_owned());
    let snap2 = graph
        .insert_edges(&[revision])
        .await
        .expect("insert")
        .expect("snap");

    // As of snap2 (current), only the manager edge is live.
    let now = graph.reader(Some(snap2)).await.expect("reader");
    let neighbors = now.get_neighbors("alice", Direction::Out, &TraversalFilter::default());
    assert_eq!(neighbors.len(), 1);
    assert_eq!(neighbors[0].node_id, "manager");

    // As of snap1, the engineer belief is still live (the retraction row is not
    // visible yet).
    let before = graph.reader(Some(snap1)).await.expect("reader");
    let old = before.get_neighbors("alice", Direction::Out, &TraversalFilter::default());
    assert_eq!(old.len(), 1);
    assert_eq!(old[0].node_id, "engineer");
}
