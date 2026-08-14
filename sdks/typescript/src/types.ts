// Public types for the Verglas SDK.
//
// The SDK is deliberately thin: it never reads Parquet or writes Iceberg in JS.
// It speaks a small HTTP contract to a Verglas *endpoint* (the self-hosted
// server) and moves rows as JSON. The endpoint owns the catalog, the snapshots,
// and the content-addressed write path.

/** One row. Shapes are table-specific; callers narrow with a generic. */
export type Row = Record<string, unknown>;

/** JSON Schema fragment published by a reflected Integration method. */
export type NamespaceJsonSchema = boolean | Record<string, unknown>;

/** Whether a reflected method is bounded, mutating, or streaming. */
export type NamespaceMethodMode = "read" | "write" | "stream";

/** One callable operation published by an Integration namespace. */
export interface NamespaceMethodManifest {
  /** Human-readable operation purpose available to agents and management UIs. */
  description: string;
  /** Execution and authorization behavior. */
  mode: NamespaceMethodMode;
  /** JSON Schema for the single method argument. */
  input: NamespaceJsonSchema;
  /** JSON Schema for a bounded result or each streamed item. */
  output: NamespaceJsonSchema;
}

/** Reflection document through which one Integration composes into the SDK. */
export interface NamespaceManifest {
  /** Stable, unique SDK property below `client.namespace`. */
  namespace: string;
  /** User-facing Integration title. */
  title: string;
  /** User-facing Integration purpose. */
  description: string;
  /** Dot-separated method paths and their machine-readable contracts. */
  methods: Record<string, NamespaceMethodManifest>;
}

declare const namespaceMethodType: unique symbol;

/** Static type marker for an optionally generated Integration SDK method. */
export interface NamespaceMethod<Input = unknown, Output = unknown, Mode extends NamespaceMethodMode = "read"> {
  readonly [namespaceMethodType]: {input: Input; output: Output; mode: Mode};
}

/** An invocation is awaitable for bounded methods and async-iterable for streams. */
export interface NamespaceCall<Output> extends PromiseLike<Output>, AsyncIterable<Output> {}

/** Recursive static description accepted by `connect<Namespaces>()`. */
export type NamespaceRegistry = object;

declare const dynamicNamespaceRegistryType: unique symbol;

/** Default registry marker which keeps reflected namespaces dynamically callable. */
export interface DynamicNamespaceRegistry {
  readonly [dynamicNamespaceRegistryType]: true;
}

/** Untyped reflected node used when no generated namespace types are supplied. */
export interface DynamicNamespaceNode {
  (input?: unknown): NamespaceCall<unknown>;
  [segment: string]: DynamicNamespaceNode;
}

/** Maps reflected type markers into the callable SDK shape used by generated code. */
export type NamespaceBinding<Node> =
  Node extends NamespaceMethod<infer Input, infer Output, NamespaceMethodMode>
    ? (input: Input) => NamespaceCall<Output>
    : Node extends Record<string, unknown>
      ? {[Key in keyof Node]: NamespaceBinding<Node[Key]>}
      : never;

/** The composed namespace tree on a connected client. */
export type NamespaceBindings<Namespaces extends NamespaceRegistry> =
  Namespaces extends DynamicNamespaceRegistry
    ? Record<string, DynamicNamespaceNode>
    : {[Key in keyof Namespaces]: NamespaceBinding<Namespaces[Key]>};

/**
 * An opaque high-watermark token that marks a position in a table's history.
 * The SDK never parses it; it hands the value straight back to the endpoint on
 * the next `delta`/`follow`. A table advances its watermark when a new snapshot
 * commits. Treat it as a cursor, not as a timestamp.
 */
export type Watermark = string;

/** Options for `connect`. */
export interface ConnectOptions {
  /**
   * The Verglas endpoint base URL (e.g. `http://127.0.0.1:8334` for a local
   * self-hosted server).
   */
  endpoint: string;
  /** Access/queue edge when it differs from the query endpoint. */
  accessEndpoint?: string;
  /** Bearer token for the endpoint. Never logged. */
  token: string;
  /**
   * Iceberg REST catalog base URL. When omitted, the client discovers it from
   * `GET /admin/access` on first catalog operation (`ensureTable` / `createTable`).
   */
  catalogUri?: string;
  /**
   * Override the `fetch` used for requests. Defaults to the global `fetch`
   * (present in edge/serverless runtimes and Node 18+). Handy for tests and for
   * runtimes that expose fetch under a different name.
   */
  fetch?: typeof fetch;
  /** Per-request timeout in milliseconds. Default 30000. */
  timeoutMs?: number;
}

/** Options for one durable KV put. */
export interface KvPutOptions {
  /** Relative logical lifetime in seconds. */
  ttlSeconds?: number;
  /** Absolute logical expiration in Unix milliseconds. */
  expiresAtMs?: number;
  /** MIME type returned with the raw value. */
  contentType?: string;
  /** Bounded application metadata mapped directly to response headers. */
  metadata?: Record<string, string>;
  /** Required current version. */
  ifMatch?: string;
  /** Require the key to have no live value. */
  ifNoneMatch?: boolean;
  /** Identity for one logical write. The SDK never retries it itself. */
  idempotencyKey?: string;
}

/** Result of a committed KV put. */
export interface KvPutResult {
  /** Opaque committed version/ETag. */
  version: string;
  /** Whether the server replayed an existing idempotent result. */
  idempotent: boolean;
}

/** One raw KV value and its bounded metadata. */
export interface KvValue {
  /** Raw value bytes. */
  bytes: Uint8Array;
  /** Opaque committed version/ETag. */
  version: string;
  /** MIME type supplied on write. */
  contentType?: string;
  /** Commit time in Unix milliseconds. */
  modifiedAtMs: number;
  /** Logical expiration time in Unix milliseconds. */
  expiresAtMs?: number;
  /** Bounded application metadata. */
  metadata: Record<string, string>;
  /** Local tier that served the value. */
  tier: "ram" | "nvme" | "unspecified";
}

/** Metadata-only KV list entry. */
export interface KvListEntry {
  key: string;
  version: string;
  modifiedAtMs: number;
  expiresAtMs?: number;
  contentType?: string;
  metadata: Record<string, string>;
}

/** Options for one bounded prefix list. */
export interface KvListOptions {
  prefix?: string;
  limit?: number;
  cursor?: string;
}

/** One metadata-only KV list page. */
export interface KvListPage {
  entries: KvListEntry[];
  nextCursor?: string;
}

/** Result of an idempotent KV delete. */
export interface KvDeleteResult {
  removed: boolean;
}

/** Options for `Table.scan`. */
export interface ScanOptions {
  /** Maximum rows to return in this page. The endpoint may cap it. */
  limit?: number;
  /** Opaque page cursor from a previous `ScanResult.nextCursor`. */
  cursor?: string;
}

/** A page of rows from the current snapshot. */
export interface ScanResult<T extends Row = Row> {
  rows: T[];
  /** Watermark at the scanned snapshot — pass to `delta`/`follow` to continue. */
  watermark: Watermark;
  /** The snapshot the rows were read from. */
  snapshotId: string;
  /** Present when more rows remain; pass back as `ScanOptions.cursor`. */
  nextCursor?: string;
}

/** Rows committed after a watermark. */
export interface DeltaResult<T extends Row = Row> {
  rows: T[];
  /** The new watermark to persist and pass on the next call. */
  watermark: Watermark;
  /** The snapshot the delta reads up to. */
  snapshotId: string;
}

/** A table's current snapshot, without reading any rows (a cheap metadata poll). */
export interface Snapshot {
  snapshotId: string;
  watermark: Watermark;
  /** Total live rows, when the endpoint reports it. */
  recordCount?: number;
}

/** Options for `client.followRows`. */
export interface FollowRowsOptions {
  /**
   * Start following after this watermark. Omit to start from the current
   * snapshot (only rows committed after the follow began are delivered).
   */
  fromWatermark?: Watermark;
  /** Max rows per delivered batch (paged internally). The endpoint may cap it. */
  batchSize?: number;
  /** Abort to stop following (equivalent to calling `close()`). */
  signal?: AbortSignal;
  /**
   * Called when a delta read or the handler throws. Following continues. Omit to
   * have the error surface on `closed` (which rejects) and end the subscription.
   */
  onError?: (err: unknown) => void;
  /**
   * Change-feed replay position. Omit or pass `null` for live-only (only commits
   * after this subscription attaches). Pass a feed `seq` to replay from there.
   */
  cursor?: number | null;
  /** Called when the edge drops the replay history this subscription asked for. */
  onResync?: (info: { reason: string }) => void;
}

/** The handler `client.followRows` invokes for each newly committed batch. */
export type FollowHandler<T extends Row = Row> = (
  newRows: T[],
  watermark: Watermark,
) => void | Promise<void>;

// --- Catalog change feed (edge websocket) --------------------------------
//
// `client.follow` subscribes to table-commit notifications over the platform's
// edge feed — a single websocket per client. This is a *notification* stream
// (seq, table, snapshot id, commit time), not a row stream: the tenant backend
// scales to zero, so a long-lived socket is held by the edge Durable Object, not
// by the backend. To read the rows a change points at, do a `delta` against the
// table (a short request that wakes the backend). Contrast `Table.follow`, which
// polls the backend directly for rows.

/** One table-commit notification pushed over the change feed. */
export interface ChangeEvent {
  /** The feed's monotonic sequence number for this change. */
  seq: number;
  /** The fully-qualified table that committed, `namespace.table`. */
  table: string;
  /** The id of the snapshot the commit produced. */
  snapshotId: string;
  /** When the commit landed, ISO 8601. */
  committedAt: string;
}

/** The handler `client.follow` invokes for each matching change. */
export type ChangeHandler = (change: ChangeEvent) => void;

/** Options for `client.follow`. */
export interface FollowFeedOptions {
  /**
   * Replay position. Omit or pass `null` for live-only — only changes committed
   * after this subscription attaches. Pass a feed `seq` to replay every change
   * with a greater seq (as far as the edge still retains), then continue live.
   */
  cursor?: number | null;
  /**
   * Called when the edge has dropped the replay history this subscription asked
   * for (`reason: "cursor-too-old"`). The feed re-subscribes live automatically;
   * this callback is your signal that there is a gap, so you can reconcile by
   * other means (e.g. a full `scan`). The SDK never polls on your behalf.
   */
  onResync?: (info: { reason: string }) => void;
  /** Abort to end this subscription (equivalent to calling `close()`). */
  signal?: AbortSignal;
}

/** A running `client.follow` subscription. */
export interface FeedSubscription {
  /**
   * End this subscription. Idempotent. When it is the client's last follow, the
   * shared socket is closed too.
   */
  close(): void;
  /** Resolves when the subscription ends (via `close` or an aborted signal). */
  closed: Promise<void>;
}

/** Options for `Table.append`. */
export interface CommitOptions {
  /**
   * Idempotency key. Retrying a commit with the same key returns the original
   * result instead of writing twice; `CommitResult.idempotent` is then true.
   */
  idempotencyKey?: string;
}

/** The result of a committed `append`. */
export interface CommitResult {
  /** The snapshot the batch committed as. */
  snapshotId: string;
  /** Rows actually written. */
  rowsCommitted: number;
  /** The table's watermark after the commit. */
  watermark: Watermark;
  /** True when a prior commit with the same idempotency key was returned. */
  idempotent: boolean;
}

/** The result of enqueuing rows onto a queue. */
export interface QueueEnqueueResult {
  /** Stable ordered positions assigned by PostgreSQL. */
  positions: number[];
}

/** One idempotent message published to an exact topic. */
export interface QueueMessage<T extends Row = Row> {
  /** Producer-defined idempotency identity. */
  id: string;
  /** Exact subscription topic. */
  topic: string;
  /** Caller-supplied message body. */
  payload: T;
}

/** Opaque proof that one consumer owns a delivery generation. */
export interface QueueReceipt {
  /** Stable message position. */
  position: number;
  /** Consumer process owning this lease. */
  owner: string;
  /** Monotonic generation fencing prior deliveries. */
  generation: number;
}

/** One exclusively leased queue message. */
export interface QueueDelivery<T extends Row = Row> {
  /** Stable message position. */
  position: number;
  /** Exact topic that matched this subscription. */
  topic: string;
  /** Caller-supplied JSON payload. */
  payload: T;
  /** Receipt required for acknowledgement. */
  receipt: QueueReceipt;
  /** RFC 3339 lease deadline. */
  expiresAt: string;
}

/** Exclusive deliveries returned by one poll. */
export interface QueuePollResult<T extends Row = Row> {
  /** Messages claimed under distinct fenced receipts. */
  deliveries: QueueDelivery<T>[];
}

/**
 * One column of an explicit table schema: a name, an Arrow type spelled as a
 * string, and whether it admits nulls. The endpoint parses `type` — one of
 * `int64`, `int32`, `float64`, `float32`, `utf8`/`string`, `boolean`, `date32`,
 * or `decimal128(precision,scale)`. `nullable` defaults to true when omitted.
 */
export interface ColumnSpec {
  name: string;
  type: string;
  nullable?: boolean;
}

/**
 * One partition column of an explicit table: the source column and the transform
 * to apply. `transform` is `identity` (the column value) or `month` (the month of
 * a date/timestamp column). Several partition columns may be given, in order.
 */
export interface PartitionSpec {
  source: string;
  transform: "identity" | "month";
}

/**
 * The definition passed to `createTable`: the ordered column schema and an
 * optional ordered partition spec. This exists because schema inference on the
 * first commit cannot express exact types (decimals, dates), per-column
 * nullability, or month/multi-column partitioning — the caller declares them.
 */
export interface TableDefinition {
  schema: ColumnSpec[];
  partitions?: PartitionSpec[];
}

/** The result of `createTable`: the table's name and its final column list. */
export interface CreateTableResult {
  table: string;
  columns: string[];
}

/** Result of ensuring an exact table definition. */
export type EnsureTableResult = "existing" | "created";

/** Optional snapshot or timestamp pin for one table referenced by a SQL query. */
export interface QueryAt {
  /** Snapshot ID or RFC 3339 timestamp understood by the query runtime. */
  reference: string;
  /** Table whose snapshot is pinned for this query. */
  table: string;
}
