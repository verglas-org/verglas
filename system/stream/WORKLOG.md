# Worklog

- #171: Added the prebuilt Stream Worker and Durable Object. The Worker routes authenticated JSON POST requests to the named object, while the object stores ordered records in SQL, deduplicates optional producer identities, and exposes independent bounded reads.
- #171: Fixed the internal protocol for parent runtime wiring: append uses `POST https://verglas.internal/stream/append` with a JSON-array body; read uses `GET https://verglas.internal/stream/read?after=<u64>&limit=<u32>`, with `limit` capped at 1000. The optional `x-verglas-producer-event-id` header is one identity for a single record or a JSON string array matching the request records.
