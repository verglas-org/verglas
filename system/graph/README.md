# Graph system project

This prebuilt Worker and Durable Object exposes a fixed named property graph
over embedded Turso tables and bidirectional B-tree adjacency indexes. Build it
from the repository root:

```sh
npx verglas-worker-build system/graph --out /tmp/verglas-graph-build
```

Tenant Workers declare a fixed binding:

```json
{
  "graphs": [
    { "binding": "GRAPH", "graph_name": "knowledge" }
  ]
}
```

The binding provides `upsertNodes`, `upsertEdges`, `getNodes`, `getEdges`,
`deleteNodes`, `deleteEdges`, `neighbors`, `shortestPath`, and `describe`.
Traversal is deterministic and bounded by depth, frontier, visited-node,
scanned-edge, request-byte, and response-byte ceilings. Pinned Turso does not
support recursive CTEs, so the product has one iterative indexed traversal path.

Operators manage filterable properties through the authenticated Worker HTTP
surface:

```text
POST /graph/property-index/create { "scope": "node", "propertyName": "rank", "indexType": "number" }
POST /graph/property-index/list   {}
POST /graph/property-index/delete { "scope": "node", "propertyName": "rank" }
```

Set `GRAPH_AUTH_TOKEN` to require a bearer token on that HTTP surface.
