// Public types for the Verglas SDK.
//
// The SDK is deliberately thin: it never reads Parquet or writes Iceberg in JS.
// It speaks a small HTTP contract to a Verglas *endpoint* — either the local
// server or a cloud Verglas endpoint — and moves rows as JSON. The endpoint owns
// the catalog, the snapshots, and the content-addressed write path.

/** One row. Shapes are table-specific; callers narrow with a generic. */
export type Row = Record<string, unknown>;

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
   * The Verglas endpoint base URL. Local: the server's base URL (e.g.
   * `http://127.0.0.1:8334`). Cloud: the tenant's Verglas endpoint. The SDK
   * abstracts over the two — same interface, different endpoint.
   */
  endpoint: string;
  /** Bearer token for the endpoint. Never logged. */
  token: string;
  /**
   * Override the `fetch` used for requests. Defaults to the global `fetch`
   * (present in edge/serverless runtimes and Node 18+). Handy for tests and for
   * runtimes that expose fetch under a different name.
   */
  fetch?: typeof fetch;
  /** Per-request timeout in milliseconds. Default 30000. */
  timeoutMs?: number;
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
  /** Rows appended this call. */
  enqueued: number;
  /** The position one past the last appended record (the queue's end). */
  endPosition: number;
}

/** One record polled from a queue: its stable global position and the row. */
export interface QueueRecord<T extends Row = Row> {
  /** The record's stable global position in the queue. */
  position: number;
  /** The row payload. */
  row: T;
}

/** A page polled from a queue for one consumer group. */
export interface QueuePollResult<T extends Row = Row> {
  /** The records at or after the group's watermark, in order. */
  records: QueueRecord<T>[];
  /** The group's current watermark (the position it has acked through). */
  watermark: number;
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

/** JSON row representation of a SQL query result. */
export interface QueryResult {
  /** Result column names in order. */
  columns: string[];
  /** Rows keyed by result-column name. */
  rows: Record<string, unknown>[];
  /** Total returned rows. */
  row_count: number;
}

// ----- Graph verb family -----
//
// A graph is a namespace holding two plain Iceberg tables (nodes and edges) plus
// a snapshot-bound adjacency index. These shapes mirror the server's
// `/v1/graphs/...` wire contract; the `Graph` handle in `client.ts` speaks them.

/** The direction to follow edges relative to a node. */
export type GraphDirection = "out" | "in" | "both";

/** A node to insert. Only `id` is required. */
export interface GraphNodeInput {
  id: string;
  labels?: string[];
  properties?: Record<string, unknown>;
  agentId?: string;
  namespace?: string;
}

/**
 * An edge (triplet) to insert. `srcId`, `predicate`, `dstId`, and `provenance`
 * are required; `edgeId` is assigned by the endpoint when omitted and
 * `confidence` defaults to 1.0.
 */
export interface GraphEdgeInput {
  edgeId?: string;
  srcId: string;
  predicate: string;
  dstId: string;
  confidence?: number;
  provenance: string;
  supersedes?: string;
  validFrom?: number;
  agentId?: string;
  namespace?: string;
  properties?: Record<string, unknown>;
}

/** The predicate/confidence filter and time-travel pin shared by the reads. */
export interface GraphReadOptions {
  /** Only follow edges with this predicate. */
  predicate?: string;
  /** Only follow edges whose confidence is at least this. */
  minConfidence?: number;
  /** The direction to follow edges. Defaults to `out`. */
  direction?: GraphDirection;
  /** Read the graph as of this edge snapshot (time travel). */
  asOf?: number;
}

/** Options for `Graph.kHop`. */
export interface GraphKHopOptions extends GraphReadOptions {
  /** The number of hops to expand. */
  hops: number;
}

/** Options for `Graph.paths`. */
export interface GraphPathsOptions extends GraphReadOptions {
  /** The maximum number of hops to search. */
  maxHops: number;
}

/** Which path served a graph read: the bound index, or a table scan. */
export type GraphBackend = "index" | "scan";

/** One neighbor in a `neighbors` result. */
export interface GraphNeighbor {
  nodeId: string;
  predicate: string;
  confidence: number;
  edgeId: string;
  provenance: string;
  direction: GraphDirection;
}

/** One reached node in a `kHop` result. */
export interface GraphReached {
  nodeId: string;
  hops: number;
  pathConfidence: number;
}

/** A triplet with its receipt, inside a subgraph or a path. */
export interface GraphTriplet {
  srcId: string;
  predicate: string;
  dstId: string;
  confidence: number;
  edgeId: string;
  provenance: string;
}

/** A path between two nodes. */
export interface GraphPath {
  nodes: string[];
  edges: GraphTriplet[];
  confidence: number;
}

/** The result of `create`: the namespace and its two backing tables. */
export interface GraphCreateResult {
  namespace: string;
  nodesTable: string;
  edgesTable: string;
}

/** The result of a node or edge insert: the new snapshot and the row count. */
export interface GraphInsertResult {
  snapshotId: number | null;
  count: number;
}

/** The result of `buildIndex`. */
export interface GraphIndexResult {
  built: boolean;
  snapshotId?: number | null;
  nodeCount: number;
  edgeCount: number;
  blobPath?: string;
  blobBytes?: number;
  mode?: string;
}

/** The result of `show`: the backing tables, counts, and index state. */
export interface GraphShowResult {
  namespace: string;
  nodesTable: string;
  edgesTable: string;
  nodeCount?: number;
  edgeCount?: number;
  indexed: boolean;
  snapshotId?: number | null;
}

// --- Vector (ANN) indexes -------------------------------------------------

/** Options for `Table.addIndex`: the metric, id column, and Vamana params. */
export interface AddIndexOptions {
  /** Distance metric: "cosine" (default) or "l2". */
  metric?: "cosine" | "l2";
  /** Identity column keying vectors (default "id"). */
  idField?: string;
  /** Vamana parameters; omitted fields take engine defaults (R=64, L=100, alpha=1.2). */
  params?: { r?: number; l?: number; alpha?: number };
}

/** The report from declaring/refreshing a vector index. */
export interface IndexReport {
  target: string;
  field: string;
  metric: string;
  reflectedSnapshot?: number | null;
  fullBuild: boolean;
  inserts: number;
  deletes: number;
  consolidated: boolean;
  liveCount: number;
  tombstones: number;
  blobLocation?: string;
  blobBytes: number;
}

/** One nearest neighbor. */
export interface SearchHit {
  id: number;
  distance: number;
}

/** Options for `Table.searchIndex`. */
export interface SearchIndexOptions {
  /** Number of neighbors (default 10). */
  k?: number;
  /** Candidate-list size L. */
  l?: number;
}

/** The result of an indexed ANN search. */
export interface SearchResult {
  /** Always "index"; a missing exact-snapshot attachment is an error. */
  source: "index";
  neighbors: SearchHit[];
}

/** One declared index, as listed by `Table.listIndexes`. */
export interface IndexInfo {
  target: string;
  field: string;
  metric: string;
  reflectedSnapshot?: number | null;
  liveCount?: number;
}
