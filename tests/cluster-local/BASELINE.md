# Baseline: #164 Step 3

Recorded before any mutation. Frozen. A candidate is compared against these
numbers under the protocol in `OBJECTIVE.md`.

## Environment

- Commit: `4a920f26`
- 4 nodes in Docker, image built from this tree, `k=2 m=2 w=3`
- Write-back enabled for every key, no prefix restriction
- Origin: MinIO `RELEASE.2025-07-23T15-54-02Z`, bucket `verglas-test`
- Catalog: off. Nothing writes `catalog_archive` during the run
- Host: Docker Desktop on macOS

## Result

```
objects_written=1000
client_concurrency=4
object_bytes=4096
total_bytes=4096000
origin_put_before=1103
origin_put_after=2103
origin_put_delta=1000
client_write_seconds=55
readback_mismatched=0
```

## Reading

`origin_put_delta=1000` for 1000 objects. One origin PUT per client PUT,
exactly 1:1. This reproduces the claim #164 opens with: the object write-back
path issues the same number of S3 requests as write-through, and its only
present gain is acknowledgement latency.

`readback_mismatched=0` establishes that read-your-writes holds today, so gate
G5 is a real regression check and not a new requirement.

## Target

The bound in the issue is `total_bytes / size_limit + 1`. At a 16 MiB size
limit and 4,096,000 total bytes that is **1 PUT**.

Baseline 1000 → target ≤ 1.

`client_write_seconds=55` is the M2 reference. A candidate may not exceed
66 s (+20%).

## Known measurement caveats

- MinIO's counter is process-wide. Nothing else writes to it during a run, and
  the measurement is a before/after delta, so unrelated earlier traffic (the
  count starts at 1103 from setup probes) does not affect the result.
- The 30 s drain window is fixed. A candidate that defers uploads past it would
  report a falsely low count; gate G5's post-drain readback and the explicit
  drain requirement in section 4 are what catch that. Any candidate reporting
  a delta of 0 must be checked for deferred work rather than accepted.
