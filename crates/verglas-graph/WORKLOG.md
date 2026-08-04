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
