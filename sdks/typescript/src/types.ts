// Public types for the Verglas SDK.
//
// The SDK is deliberately thin: it never reads Parquet or writes Iceberg in JS.
// It speaks a small HTTP contract to a Verglas *endpoint* (the self-hosted
// server) and moves rows as JSON. The endpoint owns the catalog, the snapshots,
// and the content-addressed write path.

/** One row. Shapes are table-specific; callers narrow with a generic. */
export type Row = Record<string, unknown>;

/**
 * An opaque high-watermark token that marks a position in a table's history.
 * The SDK never parses it; it hands the value straight back to the endpoint on
 * the next `delta`. A table advances its watermark when a new snapshot
 * commits. Treat it as a cursor, not as a timestamp.
 */
export type Watermark = string;

/** Options for `connect`. */
export interface ConnectOptions {
  /**
   * The Verglas endpoint base URL supplied by the deployment. Local gateways
   * choose their listen address when they start.
   */
  endpoint: string;
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
  /** Watermark at the scanned snapshot — pass to `delta` to continue. */
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
