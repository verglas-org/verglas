# verglas-graph worklog

- #258: New crate. The graph-over-Iceberg engine: property-graph node and edge
  (triplet) tables on Iceberg, a CSR adjacency index serialized into a
  snapshot-bound `verglas-graph-adjacency-v1` Puffin blob (reusing the existing
  Puffin writer and the statistics-file attach path), and traversal primitives
  (get_neighbors, k_hop, neighborhood, bounded paths) that read the index with a
  correctness fallback to a table scan when no index is bound (the turn-off
  path). Append-only edges with a `supersedes` pointer give belief revision and
  time-travel via Iceberg snapshots. Framework only — no CLI/SDK or memory
  pipeline wiring. Design rationale and citations in docs/design/graph-engine.md.
- chore: Remove docs/ cross-references after deleting the docs directory. Crate module docs are the reference now.
- #91: Removed stale shadow-store extension notes from the graph attachment
  implementation. Graph and Vamana indexes now share the same authoritative
  table-FileIO plus snapshot StatisticsFile model.
- #91: Updated graph route documentation to identify `verglas-server` as the
  serving process. Graph storage and traversal behavior are unchanged.
- #66: Removed cloud-committer embedding language from crate placement docs.
- #148: Added `precedent.rs`: a hand-rolled BM25 (k1=1.2, b=0.75) lexical
  ranker with its own tokenizer, unit-tested beside the implementation. Added
  `scan::load_nodes`/`node_from_row`/`parse_label_list` (the nodes-table
  analogue of the existing edge scan path) and `scan::latest_nodes_by_id`
  (last-write-wins reduction over append-only `PutNodes` rows), plus
  `Graph::load_nodes` exposing it. Both are consumed by verglas-s3's new
  `QueryPrecedents` operation (#148) to rank `Decision` nodes; neither existed
  before because no caller had needed to read node properties back out.
- Iceberg 0.10.1: `CompressionCodec::Zstd` became `Zstd(level)`; the
  adjacency Puffin write uses `CompressionCodec::zstd_default()`.
- #137: Updated graph engine contract comments to refer to the Graph API's AddNodes operation while preserving the append-only, last-write-wins node semantics.

