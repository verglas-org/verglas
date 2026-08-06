// The worker contract: the ONE user-facing programming primitive on the platform.
//
// A worker is a handler `worker(ctx)` invoked once per trigger event. It reads and
// writes tables through the connected client, and it is a PURE FUNCTION of its
// inputs: the trigger event (which, for a cron trigger, carries the logical time
// the platform scheduled this run for) and the data currently committed. A worker
// holds nothing between runs — there is no durable per-job watermark in the
// programming model. The platform owns scheduling, backfill, and replay; a worker
// only computes the run it was handed.
//
// Triggers themselves (cron | webhook | event) are
// DEPLOYMENT config, not code. The SDK types them (`TriggerSpec`) so a definition
// can declare what it expects, but the platform's deploy path registers them; the
// worker just receives the resulting CloudEvent on `ctx.trigger`.
//
// The runner (`runWorker`) does standardized, automatic run logging: without the
// author writing any logging code, each run emits structured rows (`run_start`,
// one `commit` per append, `run_end`, and an `error` row on failure) to a
// `<name>_LOGS` table. See logging.ts for the schema, batching/idempotency, and
// the automatic charting spec.

import type { VerglasClient } from "./client";
import type { Row } from "./types";
import {
  RunLogger,
  errorMessage,
  inferPlacement,
  instrumentClient,
  newRunId,
  type LogPlacement,
} from "./logging";

// ---------------------------------------------------------------------------
// CloudEvents — the only runtime payload a worker receives on `ctx.trigger`.
// ---------------------------------------------------------------------------

/** One structured CloudEvents 1.0 envelope. */
export interface CloudEvent<T = unknown> {
  /** CloudEvents version. Verglas accepts exactly 1.0. */
  specversion: "1.0";
  /** Producer-scoped stable event identity. */
  id: string;
  /** Stable URI-reference identifying the producer. */
  source: string;
  /** Event kind used by worker subscription filters. */
  type: string;
  /** Optional resource within the source that the event concerns. */
  subject?: string;
  /** Optional RFC 3339 event timestamp. */
  time?: string;
  /** Optional media type of `data`. */
  datacontenttype?: string;
  /** Optional schema URI for `data`. */
  dataschema?: string;
  /** Event-specific structured payload. */
  data?: T;
  /** Event-specific binary payload encoded as base64. */
  data_base64?: string;
  /** CloudEvents extension attributes. */
  [extension: string]: unknown;
}

// ---------------------------------------------------------------------------
// Trigger specs — deployment config the SDK types (registration is deploy-time).
// ---------------------------------------------------------------------------

/**
 * A scheduled trigger. `schedule` is a cron expression the platform parses.
 * Backfill is first-class: with `startDate` in the past, the platform replays
 * every scheduled interval from `startDate` up to live, running each as its own
 * dispatch with its own logical interval. `catchup` chooses how the backlog runs.
 */
export interface CronTriggerSpec {
  type: "cron";
  /** Cron expression, platform-parsed. */
  schedule: string;
  /** Backfill anchor: catch up scheduled intervals from here until live. */
  startDate?: string;
  /**
   * How to run a `startDate` backlog:
   *   - `sequential` — one interval at a time, oldest first (ordered backfill);
   *   - `parallel`   — intervals fanned out concurrently (fast, unordered);
   *   - `none`       — skip the backlog, start at the next live interval.
   */
  catchup?: "sequential" | "parallel" | "none";
}

/** An HTTP trigger: the platform routes matching requests to the worker. */
export interface WebhookTriggerSpec {
  type: "webhook";
  /** Path the webhook is mounted at (platform-scoped). */
  path?: string;
}

/** A CloudEvent subscription with exact attribute matching. */
export interface EventTriggerSpec {
  type: "event";
  /** Exact CloudEvent type to accept. */
  eventType: string;
  /** Optional exact CloudEvent source filter. */
  source?: string;
  /** Optional exact CloudEvent subject filter. */
  subject?: string;
}

/** Any trigger a deployment can declare. Registration happens at deploy time;
 *  this type exists so a `WorkerDefinition` can name what it expects. */
export type TriggerSpec =
  | CronTriggerSpec
  | WebhookTriggerSpec
  | EventTriggerSpec;

// ---------------------------------------------------------------------------
// The worker contract.
// ---------------------------------------------------------------------------

/**
 * The runtime a worker handler receives. Everything the run needs and nothing it
 * doesn't: the connected client, the trigger that invoked it, the
 * deployment-configured output table(s), the environment (secrets + config), a
 * structured log sink, and an abort signal. There is deliberately NO watermark
 * accessor — progress is the trigger's logical time, not durable job state.
 */
export interface WorkerContext<Env = Record<string, unknown>> {
  /** The connected SDK instance, including reflected Integration namespaces. */
  verglas: VerglasClient;
  /** @deprecated Use `verglas`; this is the same instance for older Workers. */
  client: VerglasClient;
  /** The event that invoked this run. */
  trigger: CloudEvent;
  /**
   * The deployment-configured output table. Output is deployment config, never
   * hardcoded in worker code — the platform passes it in (the fleet harness maps
   * the `TARGET` binding here). `outputs` lists every configured output when a
   * deployment declares more than one; `output` is the first.
   */
  output: string;
  /** Every deployment-configured output table (>= 1). `output` is `outputs[0]`. */
  outputs: string[];
  /** The deployment environment: declared secret bindings and config values. */
  env: Env;
  /**
   * Structured log sink. Baseline run logging is automatic — call this only to
   * add your own steps (e.g. `ctx.log("fetch", { rows })`). Only the standard
   * fields on `meta` (level, rows, duration_ms, message, error) are recorded.
   * Never write secrets here.
   */
  log: (message: string, meta?: Row) => void;
  /** Abort to stop long-running work (a paged fetch, a stream drain). */
  signal?: AbortSignal;
}

/** What a worker may return: an optional summary the runner folds into the run
 *  log and the host's JSON response. A worker that returns nothing is fine. */
export interface WorkerResult {
  /** Rows the run wrote, for the summary. Optional; the runner also counts
   *  committed rows automatically via the instrumented client. */
  rowsWritten?: number;
  [k: string]: unknown;
}

/** A worker handler: one invocation is one bounded run against one trigger. */
export type WorkerHandler<Env = Record<string, unknown>> = (
  this: { verglas: VerglasClient },
  ctx: WorkerContext<Env>,
) => Promise<WorkerResult | void> | WorkerResult | void;

/**
 * A deployed worker: the handler plus the deploy-time metadata the host and the
 * deploy path read. `defineWorker` builds this; a module default-exports it.
 */
export interface WorkerDefinition<Env = Record<string, unknown>> {
  /** True brand so the host can recognize a worker module's default export. */
  readonly kind: "worker";
  /** Deployment name for run logs (`<name>_LOGS`) and the summary. Optional;
   *  the host falls back to the configured output table. */
  readonly name?: string;
  /** The handler invoked once per trigger. */
  readonly handler: WorkerHandler<Env>;
  /** The trigger(s) this worker expects. Informational in the SDK — the deploy
   *  path is what actually registers them. */
  readonly triggers?: TriggerSpec[];
  /** Names of secret bindings the deployment must provide in the environment. */
  readonly secrets?: string[];
}

/**
 * Defines a worker. Pass a `{ handler, ... }` definition to carry deploy metadata
 * (name, triggers, secrets), or a bare handler for the trivial case. Returns a
 * `WorkerDefinition` to default-export from the module.
 *
 * This is the single replacement for the retired Source/Sink/MV artifact kinds
 * and their `serve()` wrapper: the handler is the only code an author writes, and
 * the definition object is the inspectable artifact the host and deploy path read
 * (the way `serve(job, manifest)` used to return `{ job, manifest }`).
 */
export function defineWorker<Env = Record<string, unknown>>(
  def: WorkerHandler<Env> | Omit<WorkerDefinition<Env>, "kind">,
): WorkerDefinition<Env> {
  if (typeof def === "function") {
    return { kind: "worker", handler: def };
  }
  return { kind: "worker", ...def };
}

/** Options for `runWorker`. */
export interface RunWorkerOptions {
  /** Override the deployment name (defaults to the definition's name, else the
   *  configured output table). Stamped into the `pipeline` log column. */
  name?: string;
  /** `local` | `remote`; defaults to a loopback heuristic on the endpoint. */
  placement?: LogPlacement;
  /** Reuse a run id across a retry so the log commit stays idempotent. */
  runId?: string;
  /** Injectable millisecond clock, for tests. Defaults to Date.now. */
  now?: () => number;
  /** Set false to disable automatic logging entirely (falls back to a raw run). */
  logging?: boolean;
}

/**
 * Runs a worker once against one trigger, with automatic run logging. Builds the
 * `WorkerContext`, invokes the handler, and returns its result. Every append the
 * handler makes auto-logs a `commit` row (through an instrumented client); the
 * runner brackets the run with `run_start` / `run_end` (or an `error` row on
 * failure). Log rows batch into one commit to `<name>_LOGS`, keyed by the run id.
 * Logging is best-effort and never changes the run's result or the error thrown.
 */
export async function runWorker<Env>(
  worker: WorkerDefinition<Env>,
  ctx: WorkerContext<Env>,
  opts?: RunWorkerOptions,
): Promise<WorkerResult | void> {
  const pipeline = opts?.name ?? worker.name ?? ctx.output;

  if (opts?.logging === false) {
    return worker.handler.call({ verglas: ctx.verglas }, ctx);
  }

  // pipeline label = the deployment name; logs table = <ctx.output>_LOGS.
  const logger = new RunLogger({
    pipeline,
    logsTarget: ctx.output,
    kind: "worker",
    placement: opts?.placement ?? inferPlacement(ctx.client.endpoint),
    runId: opts?.runId ?? newRunId(),
    now: opts?.now,
  });
  const client = instrumentClient(ctx.client, logger);
  const runCtx: WorkerContext<Env> = {
    ...ctx,
    verglas: client,
    client,
    log: logger.contextLog(),
  };
  const start = logger.now();
  logger.log("run_start", { message: `worker ${logger.pipeline} (${ctx.trigger.type})` });
  try {
    const result = await worker.handler.call({ verglas: client }, runCtx);
    logger.log("run_end", {
      rows: typeof result?.rowsWritten === "number" ? result.rowsWritten : 0,
      duration_ms: logger.now() - start,
    });
    await logger.flush(ctx.client);
    return result;
  } catch (err) {
    const message = errorMessage(err);
    logger.log("error", { level: "error", error: message, duration_ms: logger.now() - start, message: `worker ${logger.pipeline} failed` });
    logger.log("run_end", { level: "error", error: message, duration_ms: logger.now() - start });
    await logger.flush(ctx.client);
    throw err;
  }
}
