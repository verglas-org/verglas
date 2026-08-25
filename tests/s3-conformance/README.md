# Ceph s3-tests conformance harness

Runs [ceph/s3-tests](https://github.com/ceph/s3-tests) — the de-facto S3
compatibility suite — against a live `verglas-runtime` endpoint backed by MinIO. For
Verglas the suite is the **compatibility contract**: the curated skip list plus
the excluded feature-group markers ARE the documented statement of what
"S3-compatible" means for the server today (issue #22).

## What runs where

```
 pytest (ceph/s3-tests, pinned commit)
        │  S3 requests (SigV4, dev keypair)
        ▼
   verglas-runtime  ──(AWS_* env: origin creds)──▶  MinIO  (origin / capacity tier)
   (built from THIS repo — the system under test)
```

`docker-compose.yml` owns only **MinIO** (the origin). `verglas-runtime` is the system
under test, so it is built from this repo and run as a local process by
`run.sh` (mirroring `verglas dev`); there is no Rust image to build and no
libc/musl story to manage.

## Run it

```bash
just s3-tests            # full suite (release verglas-runtime, MinIO, full ceph suite)
just s3-tests --smoke    # fast subset (smoke-list.txt), seconds
just s3-tests --debug    # debug build of verglas-runtime (faster compile, local dev)
just s3-tests --list     # collect-only: what WOULD run, no server/docker
just s3-tests --keep     # leave MinIO + verglas-runtime up afterwards for poking

# equivalently, directly:
./tests/s3-conformance/run.sh --full
```

`run.sh` is idempotent: it clones the pinned suite into `.work/` (gitignored),
builds a Python venv, brings up MinIO via compose, builds+starts `verglas-runtime`, runs
pytest, and tears everything down on exit. Prerequisites: Docker (compose v2),
`python3.13` (or `python3`), and a Rust toolchain.

## The compatibility contract (two files, both review-gated)

- **`markers-exclude.txt`** — whole S3 feature GROUPS Verglas does not implement
  (versioning, tagging, lifecycle, encryption, object-lock, policy, website,
  IAM/STS, s3select, s3control, SNS, ...), excluded by pytest marker.
- **`skip-list.txt`** — the per-test residue that markers don't cover, each
  entry under a stated reason. Two kinds of reason: `UNSUPPORTED / BY DESIGN /
  DEVIATION / OUT OF SCOPE / BACKEND SEMANTICS` (what Verglas doesn't do), and
  `BLOCKED BY #NNN` (a real Verglas bug on the *supported* surface, filed as an
  issue; delete the entry when the fix lands).

Changing either file changes Verglas's stated compatibility surface, so **both
require review**. The harness deselects EXACTLY the `skip-list.txt` nodeids (no
prefix matching — pytest's own `--deselect` prefix-matches and would silently
hide passing tests), and in the nightly run warns if any entry no longer matches
a collected test, so the list cannot rot.

## Harness design notes

- **Bucket lifecycle → origin.** Verglas deliberately does not own bucket
  lifecycle (wildcard model: buckets are addressed, not created/enumerated;
  `ListBuckets` is empty; `CreateBucket`/`DeleteBucket` return `NotImplemented`).
  s3-tests creates a fresh random bucket per test, so `vg_s3tests_plugin.py`
  intercepts `CreateBucket`/`DeleteBucket` at the boto3 layer and services them
  directly against MinIO. This models production (buckets pre-exist at the
  origin; Verglas serves objects within them). Every object-level operation
  still flows through the Verglas endpoint. Tests that assert bucket-lifecycle
  *semantics* are on the skip list with that reason.
- **One account.** Verglas authenticates a single static keypair. `s3tests.conf`
  requires `[s3 main]`, `[s3 alt]`, `[s3 tenant]`; all three carry the SAME dev
  keypair. Every test that needs alt/tenant to be a *distinct* account
  (cross-account ACL/policy/ownership) is skipped with that reason.
- **Checksum pinning.** `run.sh` sets `AWS_REQUEST/RESPONSE_CHECKSUM_*` to the
  pre-2024 default so modern boto3's aws-chunked trailer checksums (a separate,
  excluded feature group) don't perturb the tested surface.

## TLS + addressing smoke (issue #11)

`tls-addressing-smoke.sh` is a separate, self-contained check that the S3
endpoint serves over TLS and accepts both addressing styles. It generates a
self-signed cert (SANs for the base domain, a wildcard, and 127.0.0.1), starts
MinIO and a TLS-configured `verglas-runtime` against it, seeds one object, then fetches
it back over HTTPS with `curl --aws-sigv4`:

- path-style with **CA injection** (`--cacert`) and with **`--no-verify`** (`-k`);
- **virtual-hosted style** (`bucket.<domain>`), which must resolve the same object.

```bash
./tests/s3-conformance/tls-addressing-smoke.sh
```

Requires docker, openssl, and a `curl` with `--aws-sigv4` (>= 7.75). The
zero-downtime certificate rotation is covered by the cache-node integration
tests.

## Files

- `run.sh` — the single entrypoint (setup, lifecycle, dispatch, teardown).
- `tls-addressing-smoke.sh` — TLS termination + addressing-style smoke (#11).
- `docker-compose.yml` — MinIO origin.
- `vg_s3tests_plugin.py` — pytest plugin: bucket-lifecycle interceptor + exact
  skip-list deselection.
- `s3tests.conf` — s3-tests configuration (dev keypair, one account ×3 sections).
- `markers-exclude.txt` — feature-group marker exclusions.
- `skip-list.txt` — the curated per-test skip list (the contract).
- `smoke-list.txt` — the fast PR-path subset.
