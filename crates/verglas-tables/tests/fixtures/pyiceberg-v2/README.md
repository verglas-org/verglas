# pyiceberg-v2 — real writer-authentic Iceberg metadata

A small Iceberg **v2** table written by **pyiceberg 0.8.1** (pyarrow 18.1.0),
checked in verbatim so `verglas-tables`' lenient metadata parser is proven
against metadata a real writer produced — not only the crate's own hand-built,
fields-we-read-only manifests (which would make the validation circular).
Consumed by `tests/real_metadata.rs`; #50 builds directly on this parser.

What makes these files "real" (vs. the hand-built fixtures in
`tests/support/`):

- Manifests/manifest lists carry pyiceberg's **full embedded Avro schemas**
  (`manifest_entry` with `content`, `sequence_number`, `file_sequence_number`,
  per-column `*_counts`/`*_bounds` maps, `split_offsets`, …; `manifest_file`
  with the full v2 field set).
- The **partition tuple is a v2 field-id-labelled record** (`r102` with
  `"field-id": 1000`), not a string map.
- `status` is a real **int enum**, nullable fields are real **Avro unions**,
  and every URI is a real `s3://lake/...` path.
- Three `metadata.json` versions (create, append, append) exactly as pyiceberg
  wrote them.

## Table shape

`db.events`, location `s3://lake/warehouse/db/events`, identity-partitioned on
`category` (string). Columns: `id` long (required), `category` string
(required), `amount` double (nullable). Two snapshots, both appends:
6 rows (partitions `a`, `b`), then 4 rows (partitions `b`, `c`) — 4 Parquet
data files, 2 manifests, 2 manifest lists, ~64 KB total.

## Layout

- `objects/<key>` — every object of the table, keyed by its object key (the
  path under the `lake` bucket). Loaded into an in-memory store by the test.
- `fixture.json` — sidecar recorded at generation time: bucket, current
  metadata key, snapshot ids, data-file keys, pinned generator versions.
- `regenerate.py` — the generator (see below).

## Regenerating

The table is generated **offline** (no MinIO/network): `regenerate.py`
registers a local-directory filesystem as pyiceberg's handler for the `s3`
scheme, so pyiceberg writes byte-real metadata with embedded `s3://` URIs
while the bytes land in a temp dir. Nothing is post-processed.

```bash
python3.13 -m venv /tmp/vg-pyiceberg-venv
/tmp/vg-pyiceberg-venv/bin/pip install \
    'pyiceberg[sql-sqlite,pyarrow]==0.8.1' 'pyarrow==18.1.0' botocore
/tmp/vg-pyiceberg-venv/bin/python \
    crates/verglas-tables/tests/fixtures/pyiceberg-v2/regenerate.py
```

(`botocore` is only needed because pyiceberg's fsspec module imports it at
module level; the S3 client path is never exercised.)

Snapshot ids and UUID-bearing file names change on every regeneration — the
test reads them from `fixture.json`, so regenerating is safe, but do it only
when the pinned pyiceberg version changes (bump the pin here, in
`regenerate.py`, and in this README together).
