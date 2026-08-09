# verglas-s3 worklog

Append-only log of changes to this crate, by issue. Every PR touching this
crate adds an entry (see /AGENTS.md, "Worklog discipline").

- #1: Scaffolded as part of the initial cargo workspace: stub with module-level
  docs, placeholder types wiring real dependency edges, and an integration
  test directory. Toolchain pinned (1.96.1), workspace clippy lints applied.
- #6: Implemented the read path: `ObjectRead` trait (the interface #12/#18 fill in)
  with range-aware, streaming `get`/`head`; a passthrough implementation over
  `object_store` (S3 from config via the standard AWS credential chain, any
  store in tests); and the s3s front-end mapping GetObject/HeadObject onto the
  trait — all three Range forms with correct 206/Content-Range, S3-shaped
  ETag/Content-Type/Last-Modified/Accept-Ranges headers, NoSuchKey/NoSuchBucket
  error XML, and bodies streamed end to end (bounded-memory test proves the
  producer never runs ahead of the consumer). SigV4 wiring is a permissive
  static-key check pending real validation in #7.
- #9: Implemented the write passthrough: an `ObjectWrite` trait (put, delete,
  batch delete, copy, and the full multipart lifecycle — deliberately separate
  from `ObjectRead`, because reads gain a cache while writes stay passthrough)
  plus an `Invalidation` hook fired with the affected keys between
  backend-durable and client ack, per #21's ordering (no-op today; #12/#14
  fill it). `PassthroughWrite` streams PUT bodies in bounded memory (≤8 MiB
  buffered: one atomic PUT for small bodies, backend multipart for large),
  round-trips backend upload IDs verbatim, and answers ListParts from its own
  upload records (object_store cannot forward it; #18's client will). Write
  surface wired into the s3s front-end with S3-shaped error XML
  (NoSuchBucket/NoSuchKey/NoSuchUpload/InvalidPart), only the configured
  bucket served. Tests: byte-identical put/copy round trips, delete →
  NoSuchKey, batch delete, multipart lifecycle/abort/bogus-ID, ordering
  (failed backend PUT ⇒ error + no invalidation; failed invalidation ⇒ no
  ack), and a counting-producer proof that PUT bodies stream bounded.
- #7: Enforced SigV4 on the engine-facing endpoint: `StaticAuth` looks up secrets
  from the configured dev keypair and returns S3-shaped `InvalidAccessKeyId` for
  unknown keys; removed the permissive `AllowAll` access policy so unsigned
  requests are rejected; added response middleware for `x-amz-request-id` headers
  and `RequestId`/standard `Message` elements in error XML (enrichment gated on
  4xx/5xx so large `application/xml` success bodies stream untouched).
  Integration tests cover header auth, presigned URLs, clock skew,
  `UNSIGNED-PAYLOAD`, and aws-chunked `STREAMING-AWS4-HMAC-SHA256-PAYLOAD` PUTs
  (now that the write path landed in #9). s3-tests conformance (#22) remains the
  long-tail checklist for trailer variants.
- #121 review follow-up: the `read`/`write` trait modules moved to
  verglas-core; this crate now re-exports them for convenience and keeps
  only the protocol layer and the passthroughs. No behavior change (the
  whole test suite passes unmodified).
- #18: Dropped the dev-grade `s3_store_from_backend` constructor and the
  local `MultipartObjectStore` trait; both now live in verglas-backend. The
  passthroughs are unchanged in behaviour — they drive whatever store they are
  handed — but production now hands them verglas-backend's concurrency-limited
  client (re-exporting `MultipartObjectStore` from there so callers keep the
  same name).
- #132: Routed the passthrough read/write traits by the request's bucket through
  the new `BackendStores` map instead of a single hardcoded store — deleted the
  `check_bucket` guards. Origin `NoSuchBucket`/`AccessDenied` are recognized and
  passed through (wildcard serving); cross-bucket `CopyObject` is refused with a
  clear NotImplemented; multipart tracking is now bucket-scoped. Added a
  ListBuckets handler returning an empty list (documented v1 choice: any bucket
  is served, so there is no fixed set to enumerate). Constructors now take an
  `Arc<dyn BackendStores>`.
- #8: Wired the `ObjectList` trait into the front-end (VerglasS3 and `router`
  gain a lister) and added `list_path` integration tests derived from the
  acceptance criteria (>2,000-key pagination, delimiter roll-up, raw-string
  prefixes, awkward keys, v1 marker + v2 token/start-after/max-keys walks,
  IsTruncated sequencing, aws-s3-ls-recursive parity vs a direct listing). Tests
  land red against s3s's default 501; the implementation follows.
- #21: Hardened and pinned the write-through ordering invariant (durable →
  invalidate → ack) across *every* mutating op. The front-end already ordered
  all ops correctly (#120), so this PR audited each (CopyObject and
  CompleteMultipartUpload especially) and added the missing test coverage:
  per-op error-path tests in `write_path.rs` (backend failure ⇒ no ack + no
  invalidation; invalidation failure ⇒ no ack, even though the backend write
  landed) for DELETE/CopyObject/CompleteMultipartUpload, matching PUT; plus a
  new `write_ordering.rs` with a faithful in-test model cache (key→ETag mapping
  + the #124 epoch fence) driving the #24 property test — under randomized,
  seeded read/write interleavings across all four op types, no read issued
  after an ack ever returns pre-write bytes — and a concurrent read/write
  under-load test on one contended key. Verified red-first: removing the
  front-end's invalidation calls makes the property test fail with a stale-read
  assertion. No production code changed.
- #14: implemented `PassthroughRead::revalidate` as a native conditional
  `If-None-Match` HEAD-shaped GET, so an unchanged mutable object revalidates for
  a bare 304 (no body) instead of the trait's HEAD-based default; a changed
  object returns the new metadata and a missing one maps to `Vanished`.
- #144: fixed the LIST continuation cursor for delimited pages that end on a
  rolled-up CommonPrefix. `PassthroughList::list` now resumes from the last
  *emitted* entry — the common prefix (which ends with the delimiter), not a raw
  key inside the group — and, because `list_with_offset` is byte-exclusive and a
  prefix like `boo/` sorts before `boo/bar`, skips any key under a common-prefix
  resume cursor so the group is never re-rolled onto the next page.
- #145: fixed five LIST response-shape deviations in the front-end handlers:
  `max-keys=0` now reports `IsTruncated=false` with no next token/marker; an
  empty `Delimiter=` is treated as no delimiter and omitted from the echo; v1
  always includes `<Marker>` (empty string when none was sent); v2
  `fetch-owner=true` attaches a static `<Owner>`; and the v1 top-level `<Prefix>`
  echo is returned verbatim (botocore never url-decodes it) so control-character
  prefixes round-trip unencoded. The trailing-slash-key tests grouped under
  #144/#145 are a separate object_store limitation (see skip-list) and stay
  skipped.
- #143: threaded object metadata end to end — PutObject / CreateMultipartUpload /
  CopyObject build a `WriteMetadata` from the request headers, the passthrough
  writes it as `object_store` attributes (and, for CopyObject REPLACE, re-streams
  the source through a fresh PUT since object_store's copy cannot set metadata),
  and GET/HEAD emit the full header set + `x-amz-meta-*` read back off the origin.
  #146: `map_write_error`/`map_multipart_error` now recover the origin's S3
  `<Code>` (InvalidPart / EntityTooSmall / InvalidRequest / …) so a client error
  reaches the client as a 4xx, not a 500; CopyObject-to-self without a metadata
  change is rejected as InvalidRequest at the front-end. #155: validate
  `Content-MD5` at the edge (InvalidDigest for a malformed digest, BadDigest for a
  mismatch computed while streaming) and enforce the 1000-key DeleteObjects cap
  (MalformedXML).

- #143/#155: conformance-driven refinements — strip `aws-chunked` tokens from
  `Content-Encoding` (a SigV4 transfer artifact S3 never stores), and reject a
  present-but-empty `Content-MD5` as InvalidDigest (s3s parses an empty header
  value to `None`, so the raw header is consulted). Deleted the now-passing
  #143/#146/#155 skip-list entries; two residual edge tests stay documented
  deviations — `test_object_write_expires` (object_store 0.14 has no Expires
  attribute) and `test_object_delete_key_bucket_gone` (asserts a 404 for an
  *unsigned* request, which the SigV4-only invariant rejects at the auth layer).

- #254: Bound live S3 response bodies to 16 and retain each permit until the
  stream drains or the client disconnects. This applies backpressure before the
  daemon accumulates stalled socket writes under an engine's range-read fan-out.

- #189: byte-exact keys + Expires via the raw-request path. object_store's typed
  `Path` corrupts keys on write AND list (drops trailing slashes/empty segments,
  percent-encodes `# % ~ < > [ ] { } ^ | \ " * ?` and control chars), and its
  0.14 attribute model has no `Expires` in either direction. Against a real S3
  origin (`BackendStores::raw_for` answers `Some`) the passthrough now routes
  through `verglas_backend::RawS3`: reads (GET/HEAD/revalidate) and LIST always
  (full header fidelity incl. `Expires`; byte-exact listed keys via
  `encoding-type=url` + unquote-plus decode), and writes/multipart exactly when
  the typed client would drop data (a `Path`-unrepresentable key, or an
  `Expires` header) — lossless writes keep the typed client and its resilience
  stack. Raw-only copies re-stream source into destination retaining (COPY) or
  replacing (REPLACE) headers. The front-end threads `Expires` through
  `WriteMetadata`/`ObjectMeta` (HTTP-date string) and emits it on GET/HEAD;
  `WriteError::Unsupported` maps to 501 for raw-requiring requests against
  registries with no raw surface. Test registries over injected typed stores
  keep the typed paths throughout. Unskipped the 3 trailing-slash LIST tests and
  `test_object_write_expires`; added a mock-origin proxy property test pinning
  byte-exact preservation of pathological keys (trailing slash, empty segments,
  control chars) through PUT/GET/LIST/DELETE and the Expires round-trip.

- #189 (review): the passthrough's raw operations now go through
  `verglas_backend::ResilientRawS3` — the same per-bucket concurrency budget,
  circuit breaker, and retry policy as the typed client — instead of a bare
  `RawS3`. No routing change; the resilience regression (raw ops bypassing
  #129's limiter and #20's breaker/retry) is closed.
- #10: conditional GET/HEAD. A new `preconditions` module evaluates the four
  condition headers (If-Match / If-None-Match / If-[Un]Modified-Since) against
  the object's metadata in S3's RFC 7232 precedence. The GET/HEAD handlers
  resolve metadata through `ObjectRead::head` FIRST when any condition is
  present, so a failing precondition answers 304/412 before any body fetch —
  and, because `head` resolves the key→ETag mapping the TTL-aware way (#14), a
  cached object costs zero backend traffic. The 304 is rendered as a bodyless
  reply carrying the object's ETag; the response middleware strips the body a
  304 must not carry.
- #152: passthrough-by-default for unmodeled bucket-config operations. A new
  `passthrough_route` module implements an s3s `S3Route` that, before s3s's
  typed dispatch, forwards an allowlist of operations (HeadBucket,
  GetBucketLocation) verbatim to the origin via the reused `ResilientRawS3`
  client and streams the response back. `router_with_passthrough` installs it;
  `router` stays passthrough-free (mock origin / unit tests). The client
  signature is validated as today; the upstream request is re-signed with the
  node credentials. Operations not on the allowlist keep s3s's default 501.
- #11: Added `router_with_domain`, which builds the S3 endpoint with a base
  domain so s3s parses virtual-hosted-style requests (`bucket.<domain>`)
  alongside path-style; `router` now delegates to it with no domain (path-style
  only), so every existing caller is unchanged. Re-exported `CacheKey` for
  front-end consumers and tests.
- #153: implemented UploadPartCopy, partNumber GET/HEAD, and
  ListMultipartUploads as origin passthrough (raw client where `object_store`
  has no call). partNumber reads return the part's bytes with `PartsCount`;
  UploadPartCopy is same-bucket-only like CopyObject.
- #154: implemented GetObjectAttributes (passthrough, attributes come from the
  origin) and checksum forwarding — checksum request headers ride the raw write
  path, checksum-mode GET/HEAD returns the origin's stored checksums, and both
  are echoed on the PUT response. The cache never computes a checksum.
- #156: GET/HEAD with `versionId` forward the parameter to the origin and serve
  the response uncached (serve-only in v1 — a version's bytes are immutable, so
  caching is safe but not yet wired; documented here as the v1 decision).
- #208: The multipart front-end and passthrough forward per-part and composite
  checksums. CreateMultipartUpload forwards the checksum algorithm/type and
  echoes them back; UploadPart forwards and echoes the part checksum;
  CompleteMultipartUpload carries each part's checksum into the manifest, sends
  the object-level checksum, and echoes the composite. A checksummed upload
  routes the whole lifecycle through the raw path (object_store cannot carry
  checksum headers). The cache still computes nothing.
- #209: GetObjectAttributes reports the ETag unquoted, but s3s always quotes the
  XML ETag via a blanket `SerializeContent for ETag` with no per-response hook.
  The handler tags its response with `UnquoteAttributesEtag`; a marker-gated
  router layer strips the quotes on that one bounded body only, touching no
  other success response. Upstream s3s issue filed
  (https://github.com/s3s-project/s3s/issues/629); this is the smallest correct
  local workaround until it lands.
- #208: The passthrough records the checksum type chosen at CreateMultipartUpload
  on the tracked upload and restates it on CompleteMultipartUpload when the
  completion request omits it. The client names the type only on create, but a
  FULL_OBJECT completion must carry it or the origin rejects the object
  checksum. Also fixes the read of MinIO's header-reported algorithm/type on
  create and the `PartsCount` element name in GetObjectAttributes.
- #226: reverted to single-bucket serving; deleted the #132 per-bucket registry; backend.bucket is now required and gates serving. Multi-bucket is deferred to #226.
- #46: the S3 front-end records request metrics. GET times the whole request and
  observes the duration histogram when its body drains (the point the serving
  tier is known); HEAD/LIST/PUT record at return. `router_with_passthrough` and
  `VerglasS3::with_metrics` take an optional `NodeMetrics` handle; the tenant
  label is `default` until per-tenant auth (#33) lands.
- #235: Updated the passthrough docs and ListBuckets note for bucket-set serving
  (a single bucket and/or globs); routing already resolved by request bucket, so
  only the docs and the NoSuchBucket rationale changed.
- #245: Surface an unavailable origin credential refresh as a backend error on
  the raw passthrough path. The S3 response now tells the operator why the
  origin request could not be signed, while preserving the existing mapping for
  origin-generated S3 errors.
- #254: Replaced the equal-cost 16-response gate with a 64 MiB weighted egress
  budget. Small Parquet ranges now share the budget proportionally while large
  bodies retain the proven sixteen-stream worst-case bound, preserving stalled
  socket protection without throttling high-fan-out warm reads.
- #61: Added `trace_request`, the outermost front-end layer: it mints one
  `request_id` per request, scopes it for the whole request future, opens the
  root span, and returns the same id in `x-amz-request-id` (the id the logs
  carry is now the id the client sees). Each request emits one INFO completion
  line (op, tier, status, duration) tagged with the id; the object key is never
  logged.
- (incident 2026-08-02, R2 InvalidPart): Fixed the internal multipart split of
  large streamed PUTs producing non-uniform part sizes. `fill_slice` stopped at
  "at least 8 MiB", so each non-trailing part absorbed up to one chunk of
  overshoot and part lengths varied with network chunk boundaries; R2 rejects a
  completion whose non-trailing parts differ in length, failing pageserver
  layer uploads with 400 InvalidPart. Replaced it with `PartSlicer`, which
  splits the boundary chunk zero-copy at exactly `PUT_PART_SIZE` and carries
  the tail into the next part — same one-part memory bound, both the raw and
  typed split paths. Added `tests/multipart_forwarding.rs` (recording backend:
  uniform-split repro, client-part 1:1 forwarding, single-part, part retry,
  completion ETag mapping) and taught the raw-fidelity mock origin R2's
  uniform-length completion rule with a raw-path repro.
- serving surface: Added `serving_api` — a `ServingApi` trait over an owned
  `ApiRequest`/`ApiResponse` pair, an s3s `V1ServingRoute` that forwards
  `/v1/query` and `/v1/tables/...` to it, and a `CompositeRoute` so the
  bucket-config passthrough and the serving route can share the single s3s
  route slot. `router_with_passthrough` takes an optional serving API; when
  present it installs the composite route and a `ServingNameValidation` that
  reserves the two-character `v1` path prefix (which AWS bucket rules already
  forbid) so s3s parses `/v1/...` far enough to reach the route instead of
  rejecting it `InvalidBucketName`. The route does not override `check_access`,
  so the existing SigV4 gate rejects unsigned `/v1` requests with `AccessDenied`
  before the handler runs. Added `tests/serving_route.rs` (unsigned → 403, and
  SigV4-signed `/v1/query` and `/v1/tables/{t}/commit` reaching a stub and
  round-tripping) plus unit tests for path matching and the name validation.
- #3: Restricted the SigV4 serving extension to query, write, and ingest execution routes; catalog metadata paths are no longer proxied.
- #91: Updated S3 serving and authentication documentation for the renamed
  `verglas-server` process. The protocol surface is unchanged by the process
  rename.
- #29: Routed the KV data-plane path through the existing SigV4 serving boundary and carried its authenticated tenant identity into the REST extension. All other serving routes retain their existing role restrictions.
- #84: Routed every read, write, list, multipart, raw, and bucket passthrough request through its immutable storage binding. S3 endpoints now require a binding identity instead of selecting an origin from bucket name alone.
