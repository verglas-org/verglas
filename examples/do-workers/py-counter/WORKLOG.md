# Worklog

- #0: Added the Python SQL-backed Durable Object counter example with the same
  HTTP and WebSocket behavior as the sibling JavaScript counter.
- #0: Kept the counter on the supported create/insert/select SQL surface by appending one row per increment and aggregating `COUNT(*)`; this preserves the observed value through replica replay without relying on unsupported UPDATE.
- #0: Rewrote the example as a literal Cloudflare Python Worker: `Default`
  routes through `env.COUNTER.id_from_name().get().fetch()` to `Counter`, whose
  DurableObject constructor and fetch method use `ctx.storage.sql.exec()`.
  The Wrangler file now declares a compatibility date and SQLite migration; no
  Verglas-specific import or callback remains.
