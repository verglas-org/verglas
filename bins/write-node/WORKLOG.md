# verglas-write role worklog

- #3: Added the isolated `verglas-write` process for bounded Arrow commits and CSV/JSONL/Parquet ingestion through the Verglas object-cache endpoint.
- #11: Forwarded ingestion idempotency keys into Iceberg table commits. Retried CloudEvents can now replay safely without appending duplicate JSONL batches.
- #81: The isolated write role now requires the caller bearer only from the inherited
  per-run environment. It refuses serialized catalog credentials, then uses that
  ephemeral bearer for every database-scoped internal catalog request.
- #20: Made the isolated Iceberg writer publish every acknowledged snapshot to the database table-event route before returning write success. Snapshot IDs are stable publication identities, so an idempotent write retry also repairs an interrupted event publication without duplicating queue state.
- #130: Replaced the selected cache URL with the complete workload cache-ring endpoint list. Data, delete, manifest, metadata, and Puffin writes now enter the shared FileIO pool before the catalog commit.
