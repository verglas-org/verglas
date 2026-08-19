# Frozen objective: write-back client TPS

Written before any candidate exists. A candidate is selected against this text
and the evaluator output, never against its own summary.

## The defect

Every write-back PUT blocks on a consensus commit before the client is acked:

```rust
// crates/verglas-writeback/src/coordinator.rs, finish_stream_ack
self.committer.commit(StagedObject { .. }).await?;   // synchronous Raft round trip
```

Keys hash to one of `OBJECT_CONSENSUS_SHARDS = 4` groups per (binding, bucket)
(`bins/cache-node/src/consensus.rs:270`). Each Raft group serializes its log, so
client throughput is capped at `SHARDS / raft_round_trip` no matter how many
clients, connections, or ingress nodes are used.

## Measured baseline

Four nodes in Docker, `k=2 m=2 w=3`, origin Cloudflare R2 (WNAM), 4 KiB objects,
distinct keys, round-robin across all four ingresses.

| conc | verglas TPS | direct-to-R2 TPS | ratio | vg p50 | R2 p50 |
| --- | --- | --- | --- | --- | --- |
| 1 | 12.3 | 4.5 | 2.75 | 65.5 ms | 211.2 ms |
| 2 | 22.2 | 8.2 | 2.70 | 84.8 ms | 232.1 ms |
| 4 | 21.7 | 17.0 | 1.28 | 182.1 ms | 223.9 ms |

Above concurrency 4 the write path fails outright:
`write-back quorum requires 3 durable fragments; only 1 available`.

`4 / 0.18s ≈ 22` matches the plateau, so the model is the shard count divided by
the Raft round trip.

## Metric

**M1 — `verglas_tps` at concurrency 16 (transactions/second, maximize). Primary.**

Client-observed acked PUTs per second, 4 KiB objects, distinct keys,
round-robin across four ingresses, measured by `tps.py`. Concurrency 16 is
chosen because the current code cannot complete it at all; a candidate must
first make it survive, then make it fast.

Baseline: **fails** at concurrency 16. Best sustained today is 22.2 TPS at
concurrency 2.

Target: exceed 22.2 TPS at concurrency 16 with zero errors. Rank by TPS
descending.

**M2 — `p99_ms` at concurrency 16 (milliseconds, minimize). Tie-break.**

## Hard gates

| Gate | Check |
| --- | --- |
| P1 | Zero write errors across the whole run. A candidate that drops or rejects writes to raise TPS is rejected |
| P2 | Every acked object reads back byte-identical, before and after offload |
| P3 | An acked object survives the loss of one node of four at `k=2 m=2 w=3` |
| P4 | Durability is not weakened. Acking before the fragment quorum, lowering `w`, or making the commit best-effort without an ordering guarantee is a rejection |
| P5 | `cargo check`, `cargo clippy -D warnings`, `cargo fmt --check`, `cargo test --workspace` all pass |
| P6 | No code path assumes a key is locally owned; ownership routes through the ring |
| P7 | Worklog entry in every touched crate |

## Rejection rules

- Raising `OBJECT_CONSENSUS_SHARDS` alone is a partial answer, not a fix. It
  moves the ceiling by a constant and leaves throughput bound to the Raft round
  trip. It is acceptable only alongside a change that removes the per-object
  round trip.
- Do not weaken the acknowledgement contract to win the metric. The write is
  acked on a durable fragment quorum; that is the product's promise.
- One atomic change per candidate, so its effect is attributable.
