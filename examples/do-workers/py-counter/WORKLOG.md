# Worklog

- #0: Added the Python SQL-backed Durable Object counter example with the same
  HTTP and WebSocket behavior as the sibling JavaScript counter.
- #0: Kept the counter on the supported create/insert/select SQL surface by appending one row per increment and aggregating `COUNT(*)`; this preserves the observed value through replica replay without relying on unsupported UPDATE.
