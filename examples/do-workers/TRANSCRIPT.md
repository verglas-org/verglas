# Six-product cold-restart acceptance transcript

Captured on 2026-08-25 from `/Users/jfbrown/code/verglas` on macOS. This
transcript predates the supervisor rename and retains the then-current
`verglas-celld` executable name as factual evidence. The current executable is
`verglasd`; no old binary alias remains. The run used real self-hosted
`verglas-gateway`, supervisor, and `verglas-runtime` processes. The object store
was an S3-compatible R2 fixture; no tenant component received its endpoint or
credentials.

## Toolchain

```text
$ node --version
v25.5.0
$ python --version
Python 3.13.12
$ cargo --version
cargo 1.96.1 (356927216 2026-06-26)
$ rustc --version
rustc 1.96.1 (31fca3adb 2026-06-26)
```

The harness received only the generic `VERGLAS_S3_BUCKET`,
`VERGLAS_S3_ENDPOINT`, `VERGLAS_S3_ACCESS_KEY_ID`, and
`VERGLAS_S3_SECRET_ACCESS_KEY` inputs. It generated a temporary mode-0600 AWS
credentials file for the privileged runtime host configuration and removed both
private files before exit, including when `--keep` retained public diagnostics.

## Product topology

Both generated aggregate manifests contained exactly these artifact products:

```text
$ jq '.artifacts | keys' /tmp/verglas-do-cold-chain-iP8G14/js/gateway.json
[
  "catalog",
  "durable_object",
  "pipeline",
  "sink",
  "stream",
  "worker"
]
$ jq '.artifacts | keys' /tmp/verglas-do-cold-chain-iP8G14/py/gateway.json
[
  "catalog",
  "durable_object",
  "pipeline",
  "sink",
  "stream",
  "worker"
]
```

`ICEBERG_COMMIT → verglas-runtime` is a service binding, not another artifact
product. Requests followed the then-current `edge → verglas-gateway → verglas-celld →
verglas-runtime` process path.

## Assertions executed for each language

The harness performed the following sequence against production debug binaries:

1. Build Worker, Durable Object, Stream, Pipeline, Sink, and Catalog components.
2. Start the then-current `verglas-celld` binary and gateway with a fresh embedded-Turso data root.
3. Send two Worker `/incr` requests and require counts 1 and 2.
4. Process Stream records through Pipeline, Sink, and Catalog.
5. Require Pipeline cursor 2 with no pending batch.
6. Require exactly one confirmed Sink batch.
7. Require exactly one Catalog publication.
8. Send SIGINT to gateway and the supervisor; each child runtime closes admission,
   rejects pending outbox work, checkpoints its embedded WAL, and exits.
9. Restart both against the same Turso data root and compiled-component cache.
10. Require count 2, cursor 2, no pending batch, and the same single Sink and
    Catalog receipts.
11. Replay `/process` and require cursor 2 with no duplicate publication.

## JavaScript result

The final combined run used the current runtime shutdown fence and persisted
compiled-component caches. Its JavaScript phase printed:

```text
PASS js: {"language":"js","manifestPath":"/tmp/verglas-do-cold-chain-iP8G14/js/gateway.json","dataRoot":"/tmp/verglas-do-cold-chain-iP8G14/js/data","logs":"/tmp/verglas-do-cold-chain-iP8G14/js/logs","warehouse":"s3://cascadelabs/verglas/cold-restart/1787684256328-83215821-5d3b-46f8-976d-5cef6554a486/js"}
```

Retained diagnostics: `/tmp/verglas-do-cold-chain-iP8G14/js`.

## Python result

The same process then completed Python initial execution, graceful shutdown,
full process restart, state recovery, and replay:

```text
PASS py: {"language":"py","manifestPath":"/tmp/verglas-do-cold-chain-iP8G14/py/gateway.json","dataRoot":"/tmp/verglas-do-cold-chain-iP8G14/py/data","logs":"/tmp/verglas-do-cold-chain-iP8G14/py/logs","warehouse":"s3://cascadelabs/verglas/cold-restart/1787684256328-83215821-5d3b-46f8-976d-5cef6554a486/py"}
PASS JS and Python six-product cold-restart runs
```

Retained diagnostics: `/tmp/verglas-do-cold-chain-iP8G14/py`.

## Independent S3 publication evidence

A credentialed `aws s3api list-objects-v2` was run after the processes exited.
Each warehouse contained one immutable data file and the three Iceberg metadata
objects produced by the runtime proposal.

JavaScript:

```text
analytics/events/data/verglas/primary_sink/batch-54fbc020240bc1e018518b5bb3fa6a631b9c05ab60c707a4f03d38b655590266.parquet  1357
analytics/events/metadata/00000-1acb29d6-dfca-4fac-bb0f-4dff5ebc48c8.metadata.json  1768
analytics/events/metadata/snap-6361422215090347199-verglas/manifest-list/batch-54fbc020240bc1e018518b5bb3fa6a631b9c05ab60c707a4f03d38b655590266.parquet.avro  1742
analytics/events/metadata/verglas/manifest/batch-54fbc020240bc1e018518b5bb3fa6a631b9c05ab60c707a4f03d38b655590266.parquet-6361422215090347199.avro  3658
```

Python:

```text
analytics/events/data/verglas/primary_sink/batch-54fbc020240bc1e018518b5bb3fa6a631b9c05ab60c707a4f03d38b655590266.parquet  1357
analytics/events/metadata/00000-2d4997d1-7085-41bd-8aa8-ce4939a21eac.metadata.json  1768
analytics/events/metadata/snap-6361422215090347199-verglas/manifest-list/batch-54fbc020240bc1e018518b5bb3fa6a631b9c05ab60c707a4f03d38b655590266.parquet.avro  1742
analytics/events/metadata/verglas/manifest/batch-54fbc020240bc1e018518b5bb3fa6a631b9c05ab60c707a4f03d38b655590266.parquet-6361422215090347199.avro  3658
```

The object files are immutable proposals. Catalog visibility and replay state
remained solely in each Catalog object's embedded Turso database.

## Failures reproduced and fixed

The passing evidence was not produced by suppressing failures. The factual runs
first reproduced these defects:

- macOS rejected per-object Unix sockets whose temporary paths exceeded
  `SUN_LEN`; the harness now uses a short `/tmp` root.
- Pipeline emitted Sink identity `primary_sink` while the stock Sink expected
  `primary`; the stock identity now agrees end to end.
- Catalog and its privileged runtime fence still expected `primary`; both now
  authorize the same `primary_sink` identity.
- componentized Python's `asyncio.run` called a trapping WASI monotonic-clock
  stub. Enabling unrestricted WASI correctly failed against the runtime's
  no-filesystem policy, so the SDK instead resolves its immediate synchronous
  WIT-backed coroutines without an event loop.

No remote Turso service, fake object store, fallback persistence path, mock
Catalog authority, or tenant storage credential was used.
