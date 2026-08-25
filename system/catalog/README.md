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

`verglas-runtime` intercepts that exact declared binding to write immutable
Iceberg data and metadata proposals. It is infrastructure, not another public
product, and it does not own a Catalog head.

## Durable state

The Catalog Durable Object is the Catalog authority. Turso/SQLite stores:

- immutable deployment configuration and its canonical SHA-256 digest;
- multipart namespace arrays and string properties;
- each table's complete Iceberg metadata and current metadata location;
- standard UUIDv7 REST idempotency receipts;
- confirmed Sink batch receipts.

A runtime proposal becomes visible only when the object installs its metadata
location and receipt in the host-owned event transaction. Local Foyer contents
and unreferenced immutable objects are not Catalog state.

Changing immutable configuration requires deleting and recreating the object.
The deployment variables are `CATALOG_ID`, `CATALOG_WAREHOUSE`,
`CATALOG_BUCKET`, `CATALOG_NAMESPACE`, `CATALOG_TABLE`, and `CATALOG_SINK_ID`.
The final four fence the configured Sink destination; they do not restrict the
public REST namespace/table registry.

## Iceberg REST protocol

The public Worker allowlists standard `/v1` requests and forwards them to the
named Catalog object. The object implements:

- `GET /v1/config`;
- namespace list/create/load/property-update/delete;
- table list/create/load/HEAD/commit/delete;
- table registration from an existing metadata location;
- atomic table rename.

Namespaces use Iceberg's multipart array representation and unit-separator path
encoding. Table load and commit responses contain both `metadata-location` and
complete Iceberg `metadata`. Non-success responses use the standard nested
Iceberg error envelope. Table commits preserve `requirements` and `updates` for
the runtime's patched Iceberg validator, then advance only the SQLite head.

Internal `/catalog/commit` and `/catalog/status` are not public Worker routes.
The REST behavior is independently exercised through PyIceberg 0.11.1 under
`interoperability/`.

## Sink commit protocol

The internal endpoint accepts only `POST /catalog/commit` with the frozen Sink
envelope documented in [`system/sink/README.md`](../sink/README.md). It validates
content type, deterministic batch/file identities, destination ownership,
compression and roll policy, the 8 MiB body ceiling, and the 10,000-row ceiling.

The object computes a canonical payload digest and checks its Turso ledger. An
exact replay returns the stored receipt without invoking runtime. Reusing a
batch identity with a different payload returns 409. A new batch sends the
current SQLite metadata location and deterministic batch to the runtime. After
validating the returned metadata proposal, the object installs the table head
and Sink receipt in the same event transaction before acknowledging Sink.

A lost runtime response installs no SQLite head or receipt. Retrying may leave
an unreferenced immutable metadata object, but only the pointer committed by the
Catalog DO is visible to Iceberg clients.

## Tests

```sh
npm test
/Users/jfbrown/code/cascadelabs/.venv/bin/python \
  interoperability/pyiceberg_rest_compat.py
```

The unit/build suite covers persisted SQLite restart behavior, multipart
namespaces, complete metadata responses, standard commits, registration,
rename/drop, idempotency conflicts, public/internal route separation, immutable
configuration, Sink replay, lost responses, hard ceilings, receipt validation,
static capability closure, and a real `world service` component build without
WASI imports.
