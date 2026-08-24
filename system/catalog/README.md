# Catalog system project

This is the prebuilt Iceberg Catalog product: one ordinary Worker and one
Durable Object. Build it with the JavaScript SDK:

```sh
node sdks/worker-js/bin/build.mjs system/catalog --out /tmp/verglas-catalog-build
```

The component imports only the ordinary Worker/DO service capabilities. It does
not contain Iceberg metadata code, an object-store client, file-format writer,
or provider credentials. The existing cache-node Catalog authority remains the
sole metadata and storage implementation.

## Immutable deployment configuration

The Catalog object stores one canonical configuration row and SHA-256 digest in
Turso. A later activation with a different digest is a hard error; delete and
recreate the object. The Wrangler example supplies:

- `CATALOG_ID` — the named Catalog Durable Object identity.
- `CATALOG_AUTHORITY_BINDING` — the only privileged service or Durable Object
  binding the Catalog may invoke.
- `CATALOG_AUTHORITY_OBJECT` — the named authority object when that binding is
  a Durable Object namespace. A direct service binding ignores this name.
- `CATALOG_WAREHOUSE`, `CATALOG_BUCKET`, `CATALOG_NAMESPACE`, and
  `CATALOG_TABLE` — the immutable deployment destination.
- `CATALOG_SINK_ID` — the only Sink identity allowed to commit through this
  Catalog.

The request cannot select a different warehouse, bucket, namespace, table, or
Sink. Warehouse is deployment state and is represented by the configured
authority object; it is not accepted from the Sink body.

## Public REST protocol

The Worker forwards only standard Iceberg REST paths under `/v1` to the named
Catalog object. It forwards the caller's method, query string, headers, and
body to the authority. In particular, caller authorization headers are
preserved for the authority's authorization layer. The Catalog adds no
provider credential and never exposes one. `/catalog/commit` and
`/catalog/status` are not public Worker routes.

The public allowlist is:

- `GET /v1/config`;
- namespace list/create/get/delete and namespace property updates;
- table and view list/create/load/head/delete/commit/rename;
- table registration.

Unknown paths and methods return 404. The Catalog object has the same REST
allowlist, plus the two internal controls described below.

## Sink commit protocol

Only this internal request is accepted for delivery:

```text
POST https://verglas.internal/catalog/commit
content-type: application/json
x-verglas-sink-id: <sink id>
x-verglas-batch-id: <deterministic batch id>
x-verglas-file-id: verglas/<sink id>/batch-<sha256(batch id)>.parquet
x-verglas-pipeline-id: <pipeline id>
x-verglas-sql-digest: <lowercase SHA-256 digest>
```

The body is the exact frozen envelope in [`system/sink/README.md`](../sink/README.md):
`batch_id`, `file_id`, `sink_id`, `pipeline_id`, `sql_digest`, `source`,
sequence bounds, `bucket`, `namespace`, `table`, `format`, `compression`, roll
policy, and `records`. The Catalog requires `format` to be exactly `parquet`,
validates the deterministic identities, and applies 8 MiB and 10,000-row hard
ceilings.

The component computes a recursively canonical JSON SHA-256 payload digest.
The Turso ledger is keyed by `batch_id` and stores that digest, file identity,
snapshot identity, committed row count, and the complete authority receipt. An
exact replay returns the stored receipt without an authority call. Reusing a
batch identity with a different payload returns 409. A failed or lost authority
response inserts no ledger row, so retry sends the same batch and file
identity; authority-side Iceberg idempotency closes the snapshot-before-ledger
window.

A successful authority response must be 2xx JSON with these exact values:

```json
{
  "committed": true,
  "batch_id": "<same batch id>",
  "file_id": "<same file id>",
  "snapshot_id": "<non-empty opaque id>",
  "rows_committed": 2
}
```

The receipt is inserted only after all identity and row-count checks pass.
`GET https://verglas.internal/catalog/status` reports configuration identity
and confirmed receipt count; it is internal only.

## Authority binding contract

`CATALOG_AUTHORITY_BINDING` is injected by deployment. It may be either:

1. a service binding exposing `fetch(request)`; or
2. a Durable Object namespace exposing `idFromName(name)` and `get(id).fetch(request)`.

For REST, the authority receives the original method, `/v1/...` path and query,
headers, and body. For commit, it receives the canonical internal POST above
at `/catalog/commit` with the canonicalized frozen envelope and the required
Sink identity headers. The cloud/gateway is responsible for mapping this
binding to the existing cache-node Catalog authority, authenticating it, and
implementing Iceberg metadata and object operations. The guest Catalog does
not attempt a second path or fallback authority.

## Tests

```sh
npm test
```

The tests use a persisted SQLite host mock for Turso and a mock authority. They
cover REST forwarding and authorization preservation, public/internal route
separation, immutable configuration, ledger replay, lost responses and restart,
identity/configuration mismatches, hard ceilings, receipt validation, static
closure, and a real `world service` component build with no WASI imports.
