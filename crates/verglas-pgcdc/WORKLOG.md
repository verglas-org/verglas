# verglas-pgcdc worklog

- feat/pg-cdc-iceberg: New crate. Turns PostgreSQL logical replication (pgoutput,
  proto_version 1) into Iceberg change-log tables — the CDC runner behind Verglas
  Cloud's zero-ETL feature. Modules: `pgoutput` (from-scratch pgoutput decoder,
  no postgres-protocol dep), `pgtype` (PG type oid + typmod to Arrow DataType),
  `schema` (reserved `_vg_`-prefixed change-row schema and add/type-change diff),
  `rows` (decoded tuples to Arrow RecordBatch with total non-panicking text
  parsing and a parse-error counter), `iceberg_sink` (thin wrapper over
  verglas-iceberg: catalog open, ensure-table, change-row append with CDC
  snapshot props, and hand-rolled add-nullable-column evolution via the vendored
  `TableCommit::from_parts`), `runner` (the drain-tick control flow behind the
  `PgSource`/`Sink` traits so resync-on-missing-slot, advance-after-append, and
  parse-error accounting are unit-tested with fakes; live sqlx `PgConn` and
  `IcebergSink` impls are compile-checked), and `status` (the serde status
  contract the control plane surfaces). CDC data files are Parquet today; an Avro
  streaming-tier seam is documented at the append call (no Avro writer in the
  vendored iceberg 0.9.1).

- #393: Switched from in-tree `vendor/iceberg` to the pinned `verglas-org/iceberg-rust` fork (`verglas/v0.9.1` @ a40f9268) for `TableCommit::from_parts`. Same patch, maintained out of tree; drop when upstream exposes overwrite/replace commits.

- #66: Rewrote the crate description and module docs to describe zero-ETL Postgres-to-Iceberg CDC without naming Verglas Cloud.
- #130: Converted the CDC sink's resolved Iceberg connection to an endpoint list. A single self-hosted endpoint remains a one-member list, while managed CDC can use its database's complete cache ring.
