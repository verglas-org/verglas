# Worklog

- #135: Added the SF10 TPC-H durability harness and its frozen fail-closed
  report validator. It starts the fixed four-voter `k=2,m=2,w=3` topology,
  streams PyIceberg data through Verglas, and records checksums, quorum refusal,
  MinIO parity, restart convergence, and archival evidence instead of accepting
  a reduced or synthetic run.
- #135: Added a small Rust WAL driver that imports the engine's canonical
  `WalRequest` and `WalResponse` codec. The fault protocol kills a declared
  leader during a 256 MiB append stream, verifies reads before and after restart,
  and waits for checkpoint-gated archival of all complete WAL segments.
