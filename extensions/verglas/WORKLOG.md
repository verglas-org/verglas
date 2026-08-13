# Worklog

- #124: Added the native DuckDB extension scaffold and data-plane table
  functions for SQL, graph, and vector reads. The functions use only the
  configured authenticated Verglas endpoint and keep HTTP failures bounded.
- #124: Replaced the evaluator-specific native shim with a Rust loadable
  extension that decodes the real Arrow and JSON responses from Verglas. The
  package uses the official DuckDB build tools and documents custom repository
  installation for DuckDB clients.
- #124: Streamed query Arrow batches into bounded DuckDB vectors and made graph
  and vector response decoding strict and typed. Removed template leftovers,
  added graph-index vector routing, and documented the unsigned custom-repository path.
