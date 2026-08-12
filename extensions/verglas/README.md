# Verglas DuckDB extension

Query Verglas from DuckDB without copying data into the client. The extension
forwards SQL to the database-scoped query API and turns its Arrow IPC response
into DuckDB vectors. It also exposes the canonical graph traversals and vector
index search as table functions.

## Connect

Set connection details in the process environment. The token is never accepted
as a SQL argument or written to DuckDB configuration.

```sh
export VERGLAS_ENDPOINT=https://query.example.com
export VERGLAS_DATABASE=analytics
export VERGLAS_TOKEN=replace-with-a-short-lived-token
```

Build a development artifact with `make configure debug`, then load it:

```sql
LOAD 'build/debug/verglas.duckdb_extension';
SELECT * FROM verglas_query('SELECT * FROM events LIMIT 10');
SELECT * FROM verglas_graph_neighbors('knowledge', 'account:42', 'out');
SELECT * FROM verglas_graph_k_hop('knowledge', 'account:42', 2, 'out');
SELECT * FROM verglas_graph_paths('knowledge', 'account:42', 'company:7', 3, 'out');
SELECT * FROM verglas_vector_search('table', 'documents', 'embedding', [0.1::FLOAT, 0.9::FLOAT], 10);
SELECT * FROM verglas_vector_search('graph', 'knowledge', 'embedding', [0.1::FLOAT, 0.9::FLOAT], 10);
```

`verglas_query(sql)` receives a schema and Arrow batches from the Verglas query
endpoint. Graph results preserve their backend and snapshot identifiers;
`verglas_graph_paths` returns `nodes` as `VARCHAR[]` and preserves ordered edges
in `edges_json`. Vector search accepts a native `FLOAT[]`, requires an explicit
`table` or `graph` resource kind, and returns the index result's id, distance,
and source.

## Build and distribute

This is a Rust loadable extension using DuckDB's official extension CI tools.
`make configure debug test_debug` builds against DuckDB 1.5.5 and leaves a
platform-specific `build/debug/verglas.duckdb_extension` artifact. The GitHub
workflow builds release artifacts across DuckDB's supported client platforms.

Publish each release into a DuckDB custom-extension repository with the standard
`<duckdb-version>/<platform>/verglas.duckdb_extension.gz` layout. Clients can
then install it with:

```sql
INSTALL verglas FROM 'https://extensions.example.com';
LOAD verglas;
```

Custom builds are unsigned. Start the DuckDB CLI with `-unsigned` (or enable
`allow_unsigned_extensions` in an embedded client) until the extension is
accepted and signed through DuckDB Community Extensions. The package is not
published to either repository yet.

The extension deliberately has no local fallback, SQL rewriting, or alternate
endpoint. A failed remote query is a bounded, redacted DuckDB error.
