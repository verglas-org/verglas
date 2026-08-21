# `verglas-cache-node`

`VERGLAS_CATALOG` controls the hosted Iceberg catalog explicitly:

- `off` is the default. The embedded catalog and its catalog consensus groups
  stay closed. Ring-backed Neon WAL and erasure-coded consensus still run.
- `on` requires `[catalog_server]` in the node config. An invalid value fails
  startup.

A three-node (or larger) ring is the production topology. It provides quorum
  durability for ring-backed data and WAL. A one-node deployment is a cache
  edge or a solo catalog host: it has one copy, no durability, and no write
  acceleration. Object writes go directly to the S3 origin at origin speed.
  Object write-back requires both a three-or-more-node ring and
  `[cache.writeback].enabled = true`.

With `VERGLAS_CATALOG=on`, a one-node deployment can host the single-voter
catalog group. The catalog still has only one copy and is not a durability
substitute for a multi-node ring.
