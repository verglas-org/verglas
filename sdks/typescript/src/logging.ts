// Standardized worker logging + observability for the Verglas SDK runtime.
//
// Callers can emit structured run logs to a standard `<name>_LOGS` table with a
// fixed shape, batch rows into one append, and key a retry by run ID.
//
// Framework-neutral: nothing here knows about any specific domain. It is generic
// over any worker and any table.

import type { VerglasClient, Table } from "./client";
import type { CommitOptions, Row, Watermark } from "./types";

/** Suffix appended to a pipeline name to name its logs table. */
export const LOGS_SUFFIX = "_LOGS";

/**
 * The logs table for a pipeline. Same namespace as the pipeline, `_LOGS` suffix:
 * `app.points` → `app.points_LOGS`. Auto-created on the first log commit (the
 * commit path creates a table on first write); the endpoint partitions it by the
 * `day` column.
 */
export function logsTableName(pipeline: string): string {
  return `${pipeline}${LOGS_SUFFIX}`;
}

/** True for a logs table (so the runner never logs its own log commits). */
export function isLogsTable(name: string): boolean {
  return name.endsWith(LOGS_SUFFIX);
}

export type LogKind = "worker";
export type LogPlacement = "local" | "remote";
export type LogLevel = "info" | "warn" | "error";

/**
 * The standardized log row. EVERY pipeline's logs share this exact shape, so a
 * one log consumer can process any `<name>_LOGS` table.
 *
 * `ts` is epoch nanoseconds as a string (kept as a string to avoid float loss).
 * `day` (YYYY-MM-DD) is the partition column, derived from `ts`.
 */
export type LogRow = {
  /** Event time, epoch nanoseconds, as a decimal string. */
  ts: string;
  /** The pipeline name these logs belong to. */
  pipeline: string;
  kind: LogKind;
  placement: LogPlacement;
  /** Unique per run; also the log-commit idempotency key. */
  run_id: string;
  /** run_start | commit | run_end | error | any author-defined step. */
  event: string;
  level: LogLevel;
  /** Rows involved in this event (committed / delivered). */
  rows: number;
  /** Wall time for this event, milliseconds. */
  duration_ms: number;
  /** Watermark at this event, when known. */
  watermark: string | null;
  message: string;
  /** Error text on failure, else null. Never a token/key/credential. */
  error: string | null;
  /** Partition column: YYYY-MM-DD derived from `ts`. */
  day: string;
};

/** The fields an author (or the runner) may set on a log row. */
export interface LogFields {
  level?: LogLevel;
  rows?: number;
  duration_ms?: number;
  watermark?: Watermark | null;
  message?: string;
  error?: string | null;
}

function msToNs(ms: number): string {
  return `${Math.trunc(ms)}000000`;
}

function msToDay(ms: number): string {
  return new Date(ms).toISOString().slice(0, 10);
}

/** Reads epoch milliseconds from the runtime's required monotonic clock. */
function runtimeNow(): number {
  const clock = globalThis.performance;
  if (!clock || typeof clock.timeOrigin !== "number") {
    throw new Error("RunLogger: performance.timeOrigin is required when now is omitted");
  }
  return clock.timeOrigin + clock.now();
}

/** Generates a run id using the required Web Crypto UUID primitive. */
export function newRunId(): string {
  const randomUUID = globalThis.crypto?.randomUUID;
  if (typeof randomUUID !== "function") {
    throw new Error("newRunId: crypto.randomUUID is required");
  }
  return randomUUID.call(globalThis.crypto);
}

/** Loopback endpoints are local; anything else is remote. */
export function inferPlacement(endpoint: string): LogPlacement {
  return /\/\/(127\.0\.0\.1|localhost|\[::1\])(:|\/|$)/.test(endpoint) ? "local" : "remote";
}

/** Turns the author's `ctx.log(event, meta)` meta into safe standard fields.
 *  Only the fixed columns are accepted — arbitrary keys are dropped so a stray
 *  value (e.g. a URL carrying an API key) can never land in the logs table. */
function toFields(meta?: Row): LogFields {
  if (!meta) return {};
  const out: LogFields = {};
  if (meta.level === "info" || meta.level === "warn" || meta.level === "error") out.level = meta.level;
  if (typeof meta.rows === "number") out.rows = meta.rows;
  if (typeof meta.duration_ms === "number") out.duration_ms = meta.duration_ms;
  if (typeof meta.watermark === "string") out.watermark = meta.watermark;
  if (typeof meta.message === "string") out.message = meta.message;
  if (typeof meta.error === "string") out.error = meta.error;
  return out;
}

export interface RunLoggerOptions {
  /** The value stamped into every log row's `pipeline` column — the deployment
   *  name. Identifies who produced the run, and prefixes emit idempotency keys. */
  pipeline: string;
  /** The table whose `_LOGS` sibling receives this run's logs. The logs table is
   *  `<logsTarget>_LOGS` — the deployment's TARGET table, so consumers read one
   *  logs table per target even when several deployments share it. Defaults
   *  to `pipeline`: on the direct local path the label and the table coincide, so
   *  `<name>_LOGS` is unchanged. Decoupled from `pipeline` so the two concerns —
   *  which table the logs land in vs. which name they carry — thread separately. */
  logsTarget?: string;
  kind: LogKind;
  placement: LogPlacement;
  /** Reuse across a retry to keep the log commit idempotent. */
  runId?: string;
  /** Injectable epoch-millisecond clock (tests). Defaults to performance. */
  now?: () => number;
  /** Where best-effort warnings go when a log write fails. Defaults to console. */
  warn?: (message: string, err?: unknown) => void;
}

/**
 * Buffers standard log rows for one run and commits them to `<name>_LOGS` once,
 * best-effort. A failure to write logs never propagates — it is swallowed and
 * reported via `warn`, so logging can never break a pipeline run.
 */
export class RunLogger {
  readonly runId: string;
  readonly pipeline: string;
  readonly kind: LogKind;
  readonly placement: LogPlacement;
  readonly logsTable: string;
  private readonly clock: () => number;
  private readonly warn: (message: string, err?: unknown) => void;
  private readonly buffer: LogRow[] = [];

  constructor(opts: RunLoggerOptions) {
    this.runId = opts.runId ?? newRunId();
    this.pipeline = opts.pipeline;
    this.kind = opts.kind;
    this.placement = opts.placement;
    // The logs table follows the target, not the pipeline label; they coincide
    // only when logsTarget is omitted (the direct local path).
    this.logsTable = logsTableName(opts.logsTarget ?? opts.pipeline);
    this.clock = opts.now ?? runtimeNow;
    this.warn = opts.warn ?? ((m, e) => console.warn(m, e));
  }

  /** Current clock reading in milliseconds (for run/step durations). */
  now(): number {
    return this.clock();
  }

  /** Buffers one standard log row. Author steps and runner steps share this. */
  log(event: string, fields: LogFields = {}): void {
    const ms = this.clock();
    this.buffer.push({
      ts: msToNs(ms),
      pipeline: this.pipeline,
      kind: this.kind,
      placement: this.placement,
      run_id: this.runId,
      event,
      level: fields.level ?? "info",
      rows: fields.rows ?? 0,
      duration_ms: fields.duration_ms ?? 0,
      watermark: fields.watermark ?? null,
      message: fields.message ?? "",
      error: fields.error ?? null,
      day: msToDay(ms),
    });
  }

  /** Buffered-but-not-yet-flushed row count (for tests/introspection). */
  get pending(): number {
    return this.buffer.length;
  }

  /**
   * Commits the buffered rows to `<name>_LOGS` as a single batch, keyed by the
   * run id so a retried flush (same run id) is a no-op rather than a double
   * write. Best-effort: any error is swallowed and reported via `warn`.
   *
   * Uses the raw client (never an instrumented one) so writing logs does not
   * recurse into more logging.
   */
  async flush(client: VerglasClient): Promise<void> {
    if (this.buffer.length === 0) return;
    const rows = this.buffer.splice(0, this.buffer.length);
    try {
      await client.table<LogRow>(this.logsTable).append(rows, { idempotencyKey: this.runId });
    } catch (err) {
      this.warn(`verglas logging: failed to write ${rows.length} log rows to ${this.logsTable}`, err);
    }
  }

  /** A `ctx.log`-shaped function that routes author steps into this buffer. */
  contextLog(): (message: string, meta?: Row) => void {
    return (message, meta) => this.log(message, toFields(meta));
  }
}

/** Turns an unknown thrown value into a message safe to store (no secrets). */
export function errorMessage(err: unknown): string {
  if (err instanceof Error) return err.message;
  if (typeof err === "string") return err;
  try {
    return JSON.stringify(err);
  } catch {
    return String(err);
  }
}

/**
 * Wraps a client so every `table(name).append(...)` on a non-logs table also
 * buffers a `commit` log row (rows committed + duration + watermark). Reads and
 * the logs table pass straight through. Implemented with Proxies so the wrapped
 * value keeps the exact `VerglasClient`/`Table` type — nothing in client.ts is
 * touched.
 */
export function instrumentClient(client: VerglasClient, logger: RunLogger): VerglasClient {
  return new Proxy(client, {
    get(target, prop, receiver) {
      if (prop === "table") {
        return <T extends Row>(name: string): Table<T> => {
          const table = target.table<T>(name);
          // Never instrument the logs table itself (avoid recursion).
          if (name === logger.logsTable) return table;
          return instrumentTable(table, logger, name);
        };
      }
      return Reflect.get(target, prop, receiver);
    },
  });
}

function instrumentTable<T extends Row>(table: Table<T>, logger: RunLogger, name: string): Table<T> {
  return new Proxy(table, {
    get(target, prop, receiver) {
      if (prop === "append") {
        return async (rows: T[], opts?: CommitOptions) => {
          const t0 = logger.now();
          try {
            const res = await target.append(rows, opts);
            logger.log("commit", {
              rows: res.rowsCommitted,
              duration_ms: logger.now() - t0,
              watermark: res.watermark,
              message: `committed to ${name}`,
            });
            return res;
          } catch (err) {
            logger.log("error", {
              level: "error",
              rows: rows.length,
              duration_ms: logger.now() - t0,
              error: errorMessage(err),
              message: `commit to ${name} failed`,
            });
            throw err;
          }
        };
      }
      return Reflect.get(target, prop, receiver);
    },
  });
}
