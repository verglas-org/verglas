---
name: lakehouse
description: Use the installed `verglas` CLI against the lakehouse runtime for Iceberg tables (`verglas table`), property graphs (`verglas graph`), and vector buckets/indexes (`verglas vector`). Use when creating, appending to, inspecting, or dropping agent-managed Iceberg tables; building or traversing a property graph whose edges carry provenance and confidence; or putting, listing, and querying vectors for nearest-neighbor search. SQL analysis goes through any Iceberg client (for example DuckDB) against the catalog and S3 endpoint. Prefer `--json` for machine-readable output. Do not use for editing source code or handling secrets.
---

# Lakehouse CLI

The installed `verglas` CLI operates one Iceberg lakehouse through three command groups: `table`, `graph`, and `vector`. Graph and vector verbs speak the runtime node's S3 semantic listener (`VERGLAS_S3_ENDPOINT`, default `http://127.0.0.1:8333`, SigV4 keys from `VERGLAS_S3_ACCESS_KEY_ID`/`VERGLAS_S3_SECRET_ACCESS_KEY`); table verbs speak the Iceberg REST catalog from `~/.verglas/config.toml` directly. Every command accepts `--json` for a stable, parseable shape; without it you get human-readable tables — use `--json` unless a person is reading the output directly. Workers talk to Verglas Cloud by default. Self-hosted OSS servers use `VERGLAS_ENDPOINT`; there is no `--server-endpoint` flag. Pass `--token` / `--credentials-file` from env or `~/.verglas` — do not pass credentials as bare arguments. Only the verbs and flags below exist — run `verglas <group> <verb> --help` before assuming an argument shape this skill does not show.

## Tables: `verglas table`

Agent-managed Iceberg tables, catalog-direct.

```bash
verglas --json table create <namespace.table> <file>   # CSV/Parquet/JSONL infer schema from the extension
verglas --json table append <namespace.table> <file>   # same formats as create; a schema mismatch names the column
verglas --json table list [namespace]                    # list tables, optionally within one namespace
verglas --json table show <namespace.table>               # schema, partitioning, current-snapshot counters
verglas --json table history <namespace.table>            # snapshot log: ids, timestamps, operations, summaries
verglas --json table delete <namespace.table> --yes        # drop the catalog entry; irreversible, requires --yes or an interactive confirmation
```

Every `create`/`append` is a new snapshot. Answer "where did this number come from" by re-running the query that produced it at the snapshot named in `table history`; answer "what changed" by diffing row counts across two snapshots.

## Property graphs: `verglas graph`

A graph is a namespace holding two plain Iceberg tables (nodes, edges) plus a snapshot-bound adjacency index — the verbs parallel `table`. Edges carry provenance and an optional confidence and are append-only: a contradiction is a new edge, never a mutation of an old one.

```bash
verglas --json graph create <namespace>                # idempotent: ensure the nodes/edges tables exist
verglas --json graph add-node <namespace> <file|->       # JSON array of {"id","labels"?,"properties"?}
verglas --json graph add-edge <namespace> <file|->       # JSON array of {"sourceId","predicate","targetId","provenance","confidence"?,"edgeId"?}
verglas --json graph neighbors <namespace> <node-id>      # a node's direct neighbors
verglas --json graph k-hop <namespace> <node-id>          # every node reached within K hops, with hop distance and path confidence
verglas --json graph paths <namespace> <src-id> <dst-id>  # shortest path between two nodes within a hop bound
verglas --json graph index <namespace>                     # build or refresh the adjacency index
verglas --json graph show <namespace>                      # graph name and current edges snapshot
verglas --json graph list                                   # every graph namespace
verglas --json graph delete <namespace>                     # drop the graph and its backing tables
```

`add-node`/`add-edge` take a JSON file path, or read stdin when the path is omitted or `-`. Check `verglas graph k-hop --help` and `verglas graph paths --help` for their hop-bound flag before guessing its name.

## Vector search: `verglas vector`

Vector buckets hold named ANN indexes; vectors are keyed float32 payloads with optional metadata.

```bash
verglas --json vector create-bucket <bucket>
verglas --json vector create-index <bucket> <index> --dimension <n> --metric cosine
verglas --json vector put <bucket> <index> <file|->        # JSON array of {"key","data":{"float32":[...]},"metadata"?}
verglas --json vector query <bucket> <index> --top-k <k> --query-vector '[...]'
verglas --json vector list <bucket> <index>
verglas --json vector get <bucket> <index> <key>...
verglas --json vector delete <bucket> <index> <key>...
verglas --json vector delete-index <bucket> <index>
verglas --json vector delete-bucket <bucket>
verglas --json vector list-buckets
verglas --json vector list-indexes <bucket>
```

## SQL analysis

The CLI carries no SQL verb. Query the same tables with any Iceberg client —
for example DuckDB attached to the catalog from `~/.verglas/config.toml`, with
reads served through the runtime's S3 endpoint. Time travel is an Iceberg
client feature: pin the snapshot id from `verglas table history`. Keep the
exact SQL you ran; it is the reproducible artifact.

## Conventions

- Pick one namespace for your project's agent-managed data; do not write into a namespace you did not create.
- Prefer one table per logical dataset, and `table append` rather than re-creating.
- `--json` on every command; do not parse the human-readable tables.
