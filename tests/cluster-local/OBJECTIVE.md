# Frozen objective: #164 Step 3, the object offload stream

This file is the contract. It is written before any candidate exists and does
not change while a wave is open. A candidate is selected against this text and
the evaluator output, never against a worker's own summary.

## Scope

Section 4 of issue #164 only: replace per-object propagation with a
size-triggered offload stream on the object write-back path.

Out of scope for this wave, and each has its own step: read-your-writes beyond
what gate G5 checks (section 5), unifying WAL and catalog onto the engine
(section 6), distributing offload across nodes (section 7), streaming decode
and cache insert (section 8, blocked on #163), compaction before offload
(section 9).

## Hard gates

All are binary. A candidate failing any gate is rejected regardless of metrics.

| Gate | Check |
| --- | --- |
| G1 | `cargo check --workspace --all-targets` exits 0 |
| G2 | `cargo clippy --workspace --all-targets -- -D warnings` exits 0 |
| G3 | `cargo fmt --all --check` exits 0 |
| G4 | `cargo test --workspace` exits 0; no existing test deleted or `#[ignore]`d to pass |
| G5a | `list-objects-v2` on the written prefix enumerates all written keys. Packing must not make logical keys invisible to enumeration |
| G5b | Every written key returns byte-identical content on GET through the Verglas S3 endpoint |
| G6 | `propagate`, `propagate_locked`, and `propagate_once` are gone from `WriteCoordinator`. No fallback to per-object propagation remains |
| G7 | A `WORKLOG.md` entry exists in every crate the candidate touches |
| G8 | The pack index commits through `ConsensusGroup`. A sidecar file is a rejection |
| G9 | Aggregate-write tests exist and pass: many small objects produce one pack; a superseded key within one unflushed stream produces one entry, not several; an object over the size limit bypasses accumulation |
| G10 | EC durability tests exist and pass: an acknowledged object survives the loss of one node of four at `k=2 m=2 w=3`, both before flush (from fragments) and after flush (from the pack) |
| G11 | Read-on-write tests exist and pass: a read immediately after acknowledgement is served locally, before flush and after flush, when the cache has room. Admission rejection under budget pressure is a normal result and must not fail the offload |
| G12 | LIST resolves through the pack index. `list-objects-v2` enumerates every written key at every stage. Wave 1 rejected all three candidates here; the issue never stated it |

## Metrics

**M1 — `origin_put_delta` (count, minimize). Primary.**

Origin PUTs MinIO served while the measurement ran, read from
`minio_s3_requests_total{api="putobject"}`. MinIO is the witness; a candidate
counting its own uploads is not evidence.

Hard bound from the issue: `total_bytes / size_limit + 1`.
At the frozen protocol below that is `4096000 / 16777216 + 1` = **1**.

**M2 — `client_write_seconds` (seconds, minimize). Tie-break only.**

Wall-clock for the 1000 client PUTs. A candidate may not regress this by more
than 20% against the baseline. Buffering must not be paid for by the writer.

**M3 — `throughput_ratio` (dimensionless, maximize). Hard bound.**

Client-observed write throughput through Verglas divided by the same workload
written directly to the origin. The whole premise of the write-back path is that
acknowledging on an EC quorum beats waiting for an origin PUT, so this must
exceed 1.0. A candidate at or below 1.0 is rejected: it has added a packing
layer and bought nothing.

Measured by `./run.sh throughput`, which runs the identical object set twice —
once against the Verglas endpoint, once against MinIO directly — and reports
both rates and the ratio.

M1, M2, and M3 are not scalarized together. Rank by M1 ascending, then M3
descending, then M2 ascending.

## Benchmark protocol

Fixed. A candidate that changes it is rejected.

- 4 nodes, `k=2 m=2 w=3`, write-back enabled for every key
- Origin: MinIO, bucket `verglas-test`
- 1000 objects, 4096 bytes each, distinct keys `measure/obj-N`
- PUTs through node1's S3 endpoint at client concurrency 4. The AWS CLI
  default of 10 overruns the ring's peer-RPC timeouts on Docker Desktop's VM
  network and fails the write quorum; 4 is sustained. This is a property of the
  harness host, not of the code under test
- After the last client PUT, poll the origin PUT counter until it stops moving
  (4 consecutive equal reads, 5 s apart), then read it. A fixed drain window
  silently rewards a candidate that defers uploads past it; quiescence does
  not. The run reports `quiesced=yes|no`, and `quiesced=no` invalidates M1
- Command: `./run.sh measure`

## Baseline

Recorded before mutation, on commit `4a920f26`. See `BASELINE.md`.

## Rejection rules

Beyond the gates, reject a candidate that:

- leaves dead code or an unrequested compatibility path
- patches a symptom rather than the owning invariant
- guesses at parsing or typing instead of using the existing domain types
- assumes a key is locally owned instead of routing through the ring
- makes more than one atomic change, so its effect cannot be attributed
- exceeds a budget; budgets are hard ceilings, not targets
