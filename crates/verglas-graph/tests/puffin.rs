//! In-memory Puffin framing for DO graph adjacency publication.

use verglas_graph::Edge;
use verglas_graph::csr::AdjacencyIndex;
use verglas_graph::puffin::{from_puffin_bytes, to_puffin_bytes};

#[tokio::test]
async fn adjacency_round_trips_through_a_real_puffin_container() {
    let edge = Edge::new("alice", "knows", "bob", "memory-1");
    let index = AdjacencyIndex::from_edges(&[edge], 17);

    let bytes = to_puffin_bytes(&index).await.expect("write Puffin");
    let decoded = from_puffin_bytes(&bytes).await.expect("read Puffin");
    assert_eq!(decoded, index);
    assert!(bytes.starts_with(b"PFA1"));
}
