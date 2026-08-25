# verglas-backend worklog

- #18: New crate owning backend client construction and policy. `BackendClient`
  selects the provider from the `backend.bucket` URI scheme (`s3://` built on
  `object_store`'s `AmazonS3Builder::from_env`; `gs://`/`az://` recognized but
  returning a clear "not yet supported" error), sources S3 credentials from the
  standard AWS chain (env keys → web identity → IMDS instance role, never from
  Verglas config), and preserves the `AWS_*` endpoint/region/path-style
  overrides dev and MinIO rely on. `LimitedStore` decorates any backend store
  with a `tokio::sync::Semaphore` so at most `backend.max_concurrent_requests`
  requests are in flight; a streamed GET holds its permit until the body drains.
- #18: Added `tests/minio.rs`, the fill integration test against a real
  S3-compatible origin (PUT, HEAD size, streamed ranged/full GET, delete),
  gated on `VERGLAS_MINIO_URL` so `cargo test` stays network-free by default.
  CI starts MinIO and sets that var (see `.github/workflows/ci.yml`), so the
  test — and the limiter's per-operation guards it exercises — run on every PR.
- #132: Replaced the single `BackendClient` with a `BackendRegistry`: a
  mutex'd bucket->client map that builds each bucket's concurrency-limited
  client lazily on first request and memoizes it, so a miss storm on one bucket
  cannot exhaust another's permits (each bucket gets its own `LimitedStore`).
  Added the `BackendStores` trait the passthrough routes through, a `from_config`
  (production S3 factory) / `with_factory` (test hook) / `wildcard_single`
  (single-origin convenience) trio, and dropped `Provider`/scheme selection
  (wildcard buckets arrive as bare names and are always S3). `tests/registry.rs`
  covers lazy build+memoization, per-bucket limiter isolation, and error
  propagation; `tests/scheme.rs` was removed with the code it tested.
- #20: Added the retry/backoff + circuit-breaker resilience layer each bucket's
  store now wears (`ResilientStore` over the existing `LimitedStore`). Retry
  sits *above* the limiter, so a transient failure (503 SlowDown/500/429/
  timeout — everything `object_store` maps to `Error::Generic`) is retried with
  jittered exponential backoff (or a `Retry-After` from a `ThrottleHint`) up to
  `[backend.retry]`'s count/budget, and the backoff sleep releases the
  concurrency permit instead of starving the pool. Non-retryable errors (404/
  403/…) return at once. `object_store`'s own in-call retry is disabled
  (`max_retries = 0`) so ours is the single engine that can both release the
  permit and feed the breaker.
- #20: Added `CircuitBreaker` — a per-bucket closed→open→half-open state
  machine (`[backend.breaker]`) fed one outcome per fill: sustained throttle/5xx
  trips it, it sheds misses fast for a cooldown (a clear `CircuitOpen` error),
  then half-opens to probe recovery. Exposed as the `BackendStores::breaker_for`
  interface so other subsystems (e.g. #51 prefetch) can yield to an open breaker.
  Tests (`breaker.rs`, `resilience.rs`) drive a manual clock and a scripted
  fault-injecting origin to pin retry-then-succeed, non-retryable-not-retried,
  Retry-After honoured, budget exhaustion, the breaker's transitions, fast-fail
  while open, and that a backoff sleep does not hold a limiter permit.
- #189: Added `raw.rs`, a SigV4-signed raw-HTTP S3 client (`RawS3`) that bypasses
  `object_store`'s typed `Path` to carry byte-exact keys (trailing slashes, empty
  segments, control chars) and the `Expires` header `object_store` 0.14 cannot
  model. Keys are single-percent-encoded (unreserved + `/`) for both the URL and
  the SigV4 canonical URI; bodies stream with `UNSIGNED-PAYLOAD` in bounded
  memory. Provides head/get/put/delete and `list_v2` (`encoding-type=url`,
  keys percent-decoded), with an axum mock-origin test proving byte-exact
  round-trips, `Expires`/metadata fidelity, and pagination.
- #189 (cont.): PUT now takes a materialized chunk slice (S3 answers 411 without
  a Content-Length, so streaming-chunked PUT cannot work) and the client gained
  the raw multipart trio (`create_multipart`/`put_part`/`complete_multipart`/
  `abort_multipart`) so larger raw-only bodies still stream in bounded memory;
  a conditional `head_if_none_match` for #14 revalidation; `InvalidRange` (416);
  ranged GETs report the full object size off `Content-Range`; list keys decode
  unquote-plus style (AWS encodes a space as `+` under `encoding-type=url`).
  `BackendStores::raw_for` resolves a memoized per-bucket `RawS3` on registries
  built `from_config` (real S3 origins); factory/test registries answer `None`.
- #189 (cont.): Extended the raw-client mock origin (multipart initiate/part/
  complete/abort, conditional HEAD, ranged GET with `Content-Range`/416, the
  synthetic `missing-bucket`/`denied-bucket` buckets, and AWS-parity `+`-for-
  space list encoding) and added eight assertion-rich tests covering the raw
  multipart trio, `head_if_none_match` 304 mapping, ranged-GET range/total
  resolution, `InvalidRange`/`NoSuchBucket`/`AccessDenied` error mapping, and
  byte-exact unquote-plus list decoding. This lifts `raw.rs` line coverage
  substantially over its previous 64%.
- #189 (review): the raw path now sits behind the SAME per-bucket resilience
  stack as the typed client — `ResilientRawS3` wraps every registry-built raw
  client with the bucket's shared `CircuitBreaker`, `RetryPolicy`, and
  concurrency semaphore (`LimitedStore::with_semaphore` shares one budget per
  bucket; a streamed raw GET holds its permit for the body's life). The
  breaker/retry loop was generalized (`guard_generic`/`retry_generic`) so both
  error surfaces share one implementation. Red-first tests pin: raw+typed draw
  one budget, an open breaker sheds raw ops without touching the origin, and a
  flaky origin is retried under the shared policy.
- #152: `RawS3::forward_bucket` (and its `ResilientRawS3` wrapper) forward a
  bucket-level request verbatim to the origin, re-signed with the node
  credentials, returning the origin's status/headers/buffered body as
  `RawForwardResponse`. The status is never mapped to a `RawError` — a 404/403
  is a legitimate answer the front-end streams back. This is the origin side of
  the unmodeled-operation passthrough (HeadBucket, GetBucketLocation).
- #153/#154/#156: extended the raw S3 client with the multipart-surface and
  attribute operations `object_store` cannot express. Added `get_ext`/`head_ext`
  (carrying versionId/partNumber query params and the checksum-mode header),
  `upload_part_copy`, `list_multipart_uploads`, and `get_object_attributes`,
  plus `RawReadExtras`, `RawChecksums`/`RawWriteChecksum`, and the uploads/parts
  response parsers. `RawObjectMeta`/`RawPutOutcome` now also carry version ID,
  parts count, and forwarded checksums. `ResilientRawS3` wraps every new call so
  they share the bucket's budget/breaker/retry stack. `RawError::InvalidPart`
  and a 4xx body-code mapping surface out-of-range part/copy errors.
- #208: The raw multipart trio forwards checksums. `create_multipart` sends the
  `x-amz-checksum-algorithm`/`-type` selection and returns the origin's echoed
  values (`RawMultipartCreation`); `put_part` sends any per-part checksum and
  returns the origin's echo (`RawPartUpload`); `complete_multipart` emits each
  part's checksums in the manifest and sends the object-level checksum header,
  returning the origin's composite. `RawWriteChecksum` gained `checksum_type`.
- #208: `complete_multipart` now sends `x-amz-checksum-type` on the completion
  request when the object checksum carries one. A FULL_OBJECT upload's
  completion needs the type restated or MinIO rejects it with `InvalidArgument`
  ("checksum type mismatch"); `append_pairs` still omits it so a plain PUT and a
  part never carry it. A `raw_roundtrip` test whose mock origin rejects a
  FULL_OBJECT completion without the header locks the behaviour in.
- #221: threaded the new `[backend]` endpoint/region/addressing/credentials into
  both S3 client builders. `BackendSettings::resolve` resolves config over the
  AWS env chain (config wins, env fills unset fields, region defaults to
  us-east-1) and loads credentials from a config-named AWS-format credentials
  file (kept under ~/.verglas/credentials, mode 0600) or the AWS env. The typed
  `AmazonS3` client (`build_s3`) and the byte-exact `RawS3` client
  (`from_settings`) both read the resolved settings, so a configured OCI/MinIO
  endpoint no longer needs env exports. Secret keys never come from config.toml.
- #221: the startup credential-mode log now reports `credentials-file` when
  `[backend] credentials_file` is set, so the operator sees the daemon is reading
  keys from ~/.verglas/credentials rather than mis-reporting the env-based guess.

- #216: Exposed `read_aws_keypair`, a public reader for an AWS-format
  credentials file's access/secret pair. The daemon uses it to load the endpoint
  keypair named by `[auth] credentials_file`, the same parser the backend already
  uses for origin credentials.
- #226: reverted to single-bucket serving; deleted the #132 per-bucket registry; backend.bucket is now required and gates serving. Multi-bucket is deferred to #226.
- #216: generalized backend client construction beyond S3. `build_typed` now
  matches on `backend.provider` and builds an AmazonS3, MicrosoftAzure, or
  GoogleCloudStorage client, all wrapped in the same limiter/breaker/retry stack.
  Added per-provider credential resolvers and parsers: AWS-INI for s3, a
  key=value file for azure (account key / SAS / client-secret) and gcp
  (service-account path), each with an env fallback. The byte-exact raw client
  stays S3-only; azure/gcp route through the typed client (raw = None).
- #235: Reworked BackendStore from one eager client to a served bucket SET
  (`BucketSet`: single bucket + globs) with a lazy, memoized per-bucket client
  registry. Each served bucket builds its own limiter/breaker/retry stack on
  first request; a bucket outside the set is NoSuchBucket without building a
  client. Added `with_glob_factory`, `bucket_set`, and an aggregate breaker
  snapshot across built buckets for node metrics.
- #245: Changed the S3 typed and raw clients to resolve origin credentials at
  signing time rather than retaining the startup key, so SDK-managed sessions
  refresh and a configured AWS credentials file can rotate without a restart.
  Failed refreshes return an operator-safe error without sending a stale
  origin request or consuming the origin retry budget.
- #245: Moved typed origin construction behind provider-specific S3, Azure, and
  GCP adapters. Azure re-reads a configured shared-key or SAS file for every
  request, while client-secret OAuth and GCP application-default identities use
  their native token refreshers; a static GCP service-account path remains an
  explicit operator choice.
- #233: Removed the silent empty-store fallback: a typed-client build failure now
  propagates from `from_config` instead of degrading to an in-memory store that
  answers NoSuchKey for every read. Added `BackendStore::probe`, which HEADs the
  configured single bucket through the same refreshing provider chain the read
  path uses so a backend that cannot be reached or authenticated fails startup;
  a NotFound answer counts as reachable, and a glob-only config is a no-op.
- #91: Renamed backend process documentation from daemon terminology to
  `verglas-server`. Backend resolution and startup probing are unchanged.
- #66: Replaced “which cloud” / “real cloud” wording with object-store language in backend docs.
- #84: Added a fail-closed dynamic registry that routes immutable storage bindings to independent backend clients and applies inserts or removals without a cache restart. Each binding retains its own provider, limiter, breaker, and retry state while the cache above remains shared.
- #core-cleanup: Moved the small bucket glob matcher into the backend crate before
  deleting the unused core glob module. Bucket-set matching remains exact and
  allocation-bounded without retaining cache-node configuration helpers.
- #171: Corrected public backend documentation links so strict workspace rustdoc resolves only public items and the owning factory method. Runtime behavior is unchanged.
