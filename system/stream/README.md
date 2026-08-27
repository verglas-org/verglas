# Stream system project

Build this literal Cloudflare-style Worker project with the JavaScript SDK:

```sh
npx verglas-worker-build system/stream --out /tmp/verglas-stream-build
```

`STREAM_NAME` selects the named object identity. Set `STREAM_AUTH_TOKEN` to
require `Authorization: Bearer <token>` on HTTP ingestion. Set
`STREAM_CORS_ORIGIN` to add the configured CORS response headers. Both settings
are optional and are carried by the manifest `vars` surface.

## Immutable structured schema

`STREAM_SCHEMA` is optional. When omitted, the Stream remains unstructured and
accepts every JSON value subject only to the hard request, record, field, and
list ceilings. When present, it must contain exactly a non-empty `fields`
array. Each field has exactly `name`, `type`, and boolean `required`, plus
`items` for `list` or nested `fields` for `struct`. Supported types are
`string`, `int32`, `int64`, `float32`, `float64`, `bool`, `timestamp`, `json`,
`binary`, `list`, and `struct`. Unknown schema keys, duplicate names, missing
nested definitions, and unsupported types fail object initialization.

The schema is persisted as the creation configuration. A later activation with
a different schema fails; there is no update, migration, version, or fallback
path. Every record is validated before the event commits, but ingestion stores
the original JSON, its sequence, and its deterministic validation outcome.
Ingestion confirms both valid and invalid records. Processing reads omit invalid
positions and increment the documented `deserialization` user-error family.
Its error types are `missing_field`, `type_mismatch`, `parse_failure`, and
`null_value`. The append `errors` list retains deterministic Verglas validation
outcomes (`invalid_json`, `not_array`, `request_limit`, `record_limit`,
`field_limit`, `list_limit`, `missing_required_field`, `unknown_field`, and
`schema_type_mismatch`) that map to those metric types. A mixed structured append
returns `accepted`, `invalid`, a contiguous `sequences` array, and ordered
`errors`; its read response returns
valid records, `skipped` validation positions, and `next_after` at the last
scanned sequence. For example, a five-record batch with valid positions 1
and 5 still stores source sequences 1 through 5; its processing read returns
valid sequences 1 and 5, never renumbering them. Only processable records
participate in downstream exactly-once processing. An unstructured successful
batch keeps the original
`{ accepted, sequences }` acknowledgement shape.

The hard ceilings are a 5 MiB encoded request, 10,000 records per request, a
1 MiB encoded record, 64 counted record fields, 1,000 list items, 64 schema
fields, 8 schema nesting levels, 128-byte field names, and a 64 KiB schema.

The operator endpoint is `GET https://verglas.internal/stream/metrics` and
returns durable `input_bytes`, `input_records`, `decode_errors`, and
`user_errors: { deserialization: { missing_field, type_mismatch, parse_failure,
null_value } }`. The `extensions` object contains only labeled Verglas additions:
`ordering_violations`, `backpressure_events`, and `lag_records`.

The internal append route is `POST https://verglas.internal/stream/append` with
a JSON array body. The bounded read route is
`GET https://verglas.internal/stream/read?after=<u64>&limit=<u32>`; `after` is
exclusive and `limit` is at most 1000. An optional
`x-verglas-producer-event-id` header supplies one identity for a one-record
append or a JSON string array with one identity per record.

## Consumer retention

Pipeline remains the processing-cursor authority. Stream stores a separate,
monotonic retention acknowledgment for each explicitly registered consumer:

```text
POST /stream/consumers/register { "consumer_id": "pipeline-id" }
POST /stream/consumers/ack      { "consumer_id": "pipeline-id", "next_after": 42 }
POST /stream/consumers/detach   { "consumer_id": "pipeline-id" }
GET  /stream/retention
```

Registration starts at the Stream's already-collected watermark and is
idempotent. Acknowledgment rejects unknown consumers and positions beyond the
append head; replaying the same or a lower position is harmless. Stream deletes
record and validation payload rows only through the minimum acknowledgment of
all registered consumers. Collection is limited to 1000 positions per event;
an immediate alarm continues any remaining work. Detach is explicit—an offline
consumer never expires and cannot cause early deletion. Detaching recomputes
the minimum immediately.

Producer-event identity tombstones remain after payload collection, so a late
producer retry returns its original sequence instead of appending a duplicate.
A read below the collected watermark returns HTTP 410 with `retained_through`.
Deleting SQL rows reclaims active logical records in the per-DO CAS database;
physical reclamation of database pages and superseded immutable S3 blocks
belongs to CAS lineage compaction and is not performed by the Stream component.
