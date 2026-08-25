# Catalog system project

This is the prebuilt Catalog product: one ordinary Worker and one Turso-backed
Durable Object. Build it with the JavaScript SDK:

```sh
node sdks/worker-js/bin/build.mjs system/catalog --out /tmp/verglas-catalog-build
```

The component contains no storage client, file-format writer, provider
credential, or raw network capability. It declares one narrow service binding:

```json
{ "binding": "ICEBERG_COMMIT", "service": "verglas-runtime" }
```

`verglas-runtime` intercepts that exact declared binding for deterministic
Iceberg publication. It is infrastructure, not another public product.

## Durable state

The Catalog Durable Object owns its state in Turso:

- immutable deployment configuration and its canonical SHA-256 digest;
- namespace and table registry rows used by the public REST API;
- the confirmed Sink batch ledger.

Changing immutable configuration requires deleting and recreating the object.
The deployment variables are `CATALOG_ID`, `CATALOG_WAREHOUSE`,
`CATALOG_BUCKET`, `CATALOG_NAMESPACE`, `CATALOG_TABLE`, and `CATALOG_SINK_ID`.
A request cannot select a different destination or Sink.

## Public REST protocol

The public Worker routes allowlisted `/v1` requests to the named Catalog object.
The object serves namespace and table state directly from Turso. It never
forwards REST to another process or object. The implemented write surface is:

- `POST /v1/namespaces`;
- `POST /v1/namespaces/{namespace}/tables`.

`GET` and `HEAD` can load registered tables, and `GET /v1/config` returns
non-secret defaults. Unsupported paths and methods fail closed. Internal
`/catalog/commit` and `/catalog/status` are not public Worker routes.

## Sink commit protocol

The internal endpoint accepts only `POST /catalog/commit` with the frozen Sink
envelope documented in [`system/sink/README.md`](../sink/README.md). It validates
content type, deterministic batch/file identities, destination ownership,
compression and roll policy, the 8 MiB body ceiling, and the 10,000-row ceiling.

The object computes a canonical payload digest and checks its Turso ledger. An
exact replay returns the stored receipt without invoking the runtime. Reusing a
batch identity with a different payload returns 409. A new batch invokes only
`env.ICEBERG_COMMIT.fetch(request)`. A valid runtime receipt must confirm the
same batch, file, and row count before the object inserts its ledger row.

A lost runtime response inserts no ledger row. Retry sends the identical batch
and file identity, and runtime Iceberg replay detection closes the
snapshot-before-ledger window.

## Tests

```sh
npm test
```

Tests use persisted SQLite as the Turso host seam. They cover Catalog-owned REST
state across restart with zero capability calls, public/internal route
separation, immutable configuration, exact ledger replay, lost responses,
identity and destination mismatches, hard ceilings, receipt validation, strict
static closure, and a real `world service` component build with no WASI imports.
