# Worklog

- #0: Added the Turso Durable Object crate scaffold and acceptance tests first. The initial test run is expected to fail because the store API is intentionally not implemented yet; implementation must preserve one serialized event transaction and the remote push durability boundary.
- #0: TDD baseline confirmed with `cargo test -p verglas-do-turso --features test-support`: compilation failed as intended with unresolved `TursoStore`, `OutboxKey`, `OutboxRecord`, and crate `Error` exports (`E0432`/`E0425`).
- #0: Implemented the pinned Turso 0.7.2 store with explicit remote bootstrap/pull/schema validation, a serialized `BEGIN IMMEDIATE` event transaction, Worker KV/alarm/attachment tables, honest JSON rows, and a deterministic transactional Stream outbox with leases and reclaim. The local-only constructor is feature-gated and explicitly named `open_for_test`; no production fallback or custom transaction engine remains.
