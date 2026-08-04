# Write-back tier benchmark (#180)

Measures the erasure-coded NVMe write-back tier against a live origin: seed-phase
PUT latency and single 32/128 MiB PUT ack latency, write-back ON (quorum ack)
versus OFF (write-through), on a local 3-node dev pod. Nothing here runs in CI.

## Run

Point it at a real origin. Source an `.env` with the origin credentials first
(never commit it; it is git-ignored):

```
export AWS_ENDPOINT=https://<origin>
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_REGION=...
```

```
cargo build --release -p verglas -p verglas-server
set -a; source .env; set +a
benchmarks/writeback/run.sh
```

It boots a 3-node pod (`verglas dev --nodes 3`) twice — once without
`--writeback` (write-through) and once with it (k=2/m=1/w=3, which fits a 3-node
pod) — waits for the gossip view to converge, runs `verglas bench --seed` for the
seed-phase PUT p50, times single 32/128 MiB PUTs, reads the write-back counters
from `/admin/stats`, and tears the pod down. It kills only the `verglas-server`
children it spawned (matched by its own temp cache dir), so a benchmark running
in `benchmarks/tpch` is never touched. Results land in `results/<timestamp>.json`
and a table is printed.

Knobs: `WB_PORT` (base port, default 9333), `WB_BUCKET` (default `hyperglas`),
`WB_BIG_SAMPLES` (single-PUT samples, default 3).

## What "good" looks like

Write-back acks before the origin upload, so the seed PUT p50 and the single-PUT
ack drop versus write-through; the write-back counters show `acked_via_quorum`
rising and `propagated` catching up in the background. On a small or degraded
pod, or a single node, the write degrades to write-through automatically and the
bytes at the origin are byte-identical to today's path.

## Codec throughput

The SIMD-vs-scalar Reed-Solomon encode comparison that justifies the codec
backend is a separate example (it needs no origin):

```
cargo run --release -p verglas-cache --example codec_throughput
```
