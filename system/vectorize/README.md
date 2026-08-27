# Vectorize system project

This prebuilt Worker and Durable Object implements the current Cloudflare
Vectorize V2 Worker binding over embedded Turso vectors. Build it with:

```sh
npx verglas-worker-build system/vectorize --out /tmp/verglas-vectorize-build
```

`VECTORIZE_INDEX_NAME`, `VECTORIZE_DIMENSIONS`, and `VECTORIZE_METRIC` are
creation-only configuration. Metrics are `cosine`, `euclidean`, and
`dot-product`. Reopening an object with different configuration fails.

Tenant Workers bind the product with Cloudflare syntax:

```jsonc
{
  "vectorize": [
    { "binding": "VECTORIZE", "index_name": "documents" }
  ]
}
```

The binding exposes `insert`, `upsert`, `query`, `queryById`, `getByIds`,
`deleteByIds`, and `describe`. Mutation promises resolve after the ordinary
Durable Object Turso commit and explicit WAL checkpoint, so acknowledged
changes are immediately visible.

Vectors are stored as dimensioned `F32_BLOB` values. Queries use Turso native
cosine, L2, or negative-dot-product functions after namespace and declared
metadata filtering. The pinned Turso engine does not expose a dense ANN index;
queries with more than 10,000 eligible rows fail explicitly. There is no Vamana,
S3 Vectors, Puffin, Arrow mutation, or lake-offload fallback.

Metadata index declarations use the authenticated product Worker endpoints:

```text
POST /vectorize/metadata-index/create { "propertyName": "kind", "indexType": "string" }
POST /vectorize/metadata-index/list   {}
POST /vectorize/metadata-index/delete { "propertyName": "kind" }
```

Set `VECTORIZE_AUTH_TOKEN` to require a bearer token on that HTTP surface.
