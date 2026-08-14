# verglas-vector worklog

Append-only log of changes to this crate, by feature. Every PR touching this
crate adds an entry (see /AGENTS.md, "Worklog discipline").

- index registry: Made the in-memory index registry a PROJECTION of the durable
  `verglas_sys.indexes` registry rather than the source of truth. Added
  `VectorService::rehydrate(catalog, ident, key, config)` and `RehydrateOutcome`:
  on daemon boot it restores the maintenance config into the in-memory registry
  and either loads the present shadow-store blob into the serving cache (serve
  immediately, no rebuild) or rebuilds a missing blob via the maintenance MV. The
  durable registry write stays in the daemon (which owns the `SystemCatalog`), so
  this crate keeps no dependency on the platform control plane; the daemon hands
  the reconstructed config back through `rehydrate`. New tests in rehydrate.rs
  prove a present blob serves the same nearest neighbors without rebuilding (over
  a real temp-dir shadow store across two service instances) and a missing blob
  takes the rebuild path.

- id encoding: Added `IdEncoding` (`Integer` default, `UuidHash`) and
  `uuid_hash_id` so a table keyed by a non-integer identity is indexable without
  a schema migration. `MaintenanceConfig` gained `id_encoding`
  (`with_id_encoding` builder); `parse_row` reads the id column per the encoding
  (`uuidHash` folds the `uuid` string to a stable `i64`). The memory index over
  `agent_memory.memories` (keyed by the `uuid` `memory_id`) uses `UuidHash`; the
  recall path re-derives the same `i64` to map an ANN neighbor back to its
  memory. Added `VectorService::served(key)` so the recall seed source can check
  for a built blob before searching and otherwise take its own brute-force path
  (the turn-off) rather than the search's whole-table fallback scan.

- register: Added `VectorService::register(key, config)` — the config-only half
  of `declare` (no build), so the daemon can register the memory index's
  maintenance config at boot before its source table exists; the build lands on
  the first `refresh`/rehydration once rows exist.
- chore: Remove docs/ cross-references after deleting the docs directory. Crate module docs are the reference now.
- #91: Replaced the cluster-local shadow-store Vamana format with a first-class
  Iceberg Puffin statistics attachment bound to the exact reflected snapshot.
  Search and refresh now discover state from table metadata and reject missing
  or stale attachments without rebuilding or scanning.
- #91: Updated vector route documentation for the renamed `verglas-server`
  process. Snapshot-bound Puffin attachment behavior is unchanged.
- #72: Full builds that scan source rows but index zero of them because every
  id is unreadable under the active encoding (default integer; optional
  uuidHash) now return `VectorError::Field` with a clear integer / UUID
  uuidHash message instead of `Ok(None)`. Truly empty tables still yield no
  index. Tests cover the string-id footgun and a uuidHash full-build path.
- #137: Added a lossless arbitrary-string key bridge for Vamana. Each semantic
  index persists a collision-free monotonic `i64` mapping in a sibling Puffin
  blob attached to the same Iceberg snapshot as its ANN graph, so updates,
  deletes, and a restarted reader recover exact caller keys without a hash-only
  identifier or an authoritative in-memory map.
