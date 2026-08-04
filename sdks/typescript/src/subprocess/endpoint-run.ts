// The fleet COMMIT-ENDPOINT run mode: the bun entry a fleet microVM execs to run
// a tenant WORKER against the live commit service, mirroring exactly what the
// platform's cron dispatch does.
//
// One invocation = one bounded worker run for one scheduled interval, exactly
// like one cron dispatch. This file maps the process environment onto a
// `WorkerContext`, builds the cron trigger from the logical-time bindings the
// control plane injects, runs the worker's handler once through the SDK's
// `runWorker` (so run logging to `<TARGET>_LOGS` is identical to the platform
// path), and writes the harness result file.
//
// There is no durable watermark here: a worker is a pure function of its trigger
// (logical time) and the committed data. The control plane owns scheduling,
// backfill, and replay; this entry just runs the interval it was handed.
//
// Usage: `bun endpoint-run.ts <module>` where <module> default-exports a worker
// (a `WorkerDefinition` from `defineWorker`). Environment:
//
//   VERGLAS_ENDPOINT  (required) commit service URL, e.g. https://api.verglas.dev
//   VERGLAS_TOKEN     (required) per-deployment commit token — never printed
//   DEPLOYMENT        (required) deployment/pipeline name (the `pipeline` log column)
//   TARGET            (required) output table (its `<TARGET>_LOGS` sibling gets the run log)
//
// Trigger delivery — the control plane's fleet dispatch injects the trigger
// payload as launch env vars (its channel for what the cloud dispatch carries
// as body/headers):
//
//   VERGLAS_TRIGGER        (optional) the trigger type. Absent or "cron" runs
//                          the worker with a cron trigger; anything else is
//                          refused — this entry only runs cron dispatches.
//   VERGLAS_LOGICAL_DATE   (optional) the run's nominal scheduled instant (ISO 8601)
//   VERGLAS_INTERVAL_START (optional) inclusive start of the run's logical interval (ISO 8601)
//   VERGLAS_INTERVAL_END   (optional) exclusive end of the run's logical interval (ISO 8601)
//
//   RESULT_PATH       (optional) where the result JSON lands; default /run/result.json
//
// Worker secrets are already in the environment — the worker reads them off
// `ctx.env`, the way a fleet entry module binds its own env keys into its config.
//
// The result file is always written: {"rows": n, "error": null} on success,
// {"rows": 0, "error": "<message>"} on failure; the process exits 0/1 to match.

import { mkdirSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";
import { connect } from "../client";
import {
  runWorker,
  type CronTriggerEvent,
  type WorkerContext,
  type WorkerDefinition,
  type WorkerResult,
} from "../contracts";

/** The result-file shape the fleet harness reads. */
export interface EndpointRunResult {
  /** Rows the run committed to the output table. */
  rows: number;
  /** null on success; the failure message otherwise. */
  error: string | null;
}

/** Test seams. Production uses the defaults: global fetch and console.log. */
export interface EndpointRunOptions {
  /** Injectable transport. Unset in production, so the SDK uses global fetch
   *  against the public endpoint. Tests inject a fake commit service here. */
  fetch?: typeof fetch;
  /** Progress-line sink (each line already carries the [endpoint-run] prefix). */
  log?: (line: string) => void;
}

/** The env this mode itself requires; worker secrets are the worker's business. */
const REQUIRED_ENV = ["VERGLAS_ENDPOINT", "VERGLAS_TOKEN", "DEPLOYMENT", "TARGET"] as const;

const DEFAULT_RESULT_PATH = "/run/result.json";

function failure(message: string): EndpointRunResult {
  return { rows: 0, error: message };
}

/** Builds the cron trigger from the VERGLAS_* logical-time bindings, if present.
 *  A dispatch that carries none leaves the interval fields undefined. */
function cronTrigger(env: Record<string, string | undefined>): CronTriggerEvent {
  const trigger: CronTriggerEvent = { type: "cron" };
  if (env.VERGLAS_LOGICAL_DATE) trigger.logicalDate = env.VERGLAS_LOGICAL_DATE;
  if (env.VERGLAS_INTERVAL_START) trigger.intervalStart = env.VERGLAS_INTERVAL_START;
  if (env.VERGLAS_INTERVAL_END) trigger.intervalEnd = env.VERGLAS_INTERVAL_END;
  return trigger;
}

/**
 * One bounded run of `worker`: builds the client (routing through the injected
 * transport in tests, global fetch in production), the cron trigger, and the
 * `WorkerContext` from the environment, then runs the handler once through
 * `runWorker` (run logging included). Returns the harness result.
 */
export async function endpointRun(
  worker: WorkerDefinition,
  env: Record<string, string | undefined>,
  opts?: EndpointRunOptions,
): Promise<EndpointRunResult> {
  const log = opts?.log ?? ((line: string) => console.log(line));

  const missing = REQUIRED_ENV.filter((name) => !env[name]);
  if (missing.length > 0) return failure(`missing required env: ${missing.join(", ")}`);

  // This entry runs cron dispatches only. An absent VERGLAS_TRIGGER is treated
  // as cron; any other type is refused loudly.
  if (env.VERGLAS_TRIGGER && env.VERGLAS_TRIGGER !== "cron") {
    return failure(`unsupported VERGLAS_TRIGGER "${env.VERGLAS_TRIGGER}": this entry runs cron dispatches only`);
  }

  const deployment = env.DEPLOYMENT as string;
  const target = env.TARGET as string;

  log(`[endpoint-run] deployment=${deployment} target=${target} endpoint=${env.VERGLAS_ENDPOINT}`);

  try {
    const client = connect({
      endpoint: env.VERGLAS_ENDPOINT as string,
      token: env.VERGLAS_TOKEN as string,
      fetch: opts?.fetch,
    });
    const ctx: WorkerContext = {
      client,
      trigger: cronTrigger(env),
      output: target,
      outputs: [target],
      env,
      log: (message, meta) => console.log(message, meta ?? {}),
    };

    const result = (await runWorker(worker, ctx, { name: deployment })) as WorkerResult | void;
    // The runner's instrumented client counts committed rows into the run log;
    // the worker may also report a total. Prefer the reported total when present.
    const rows = typeof result?.rowsWritten === "number" ? result.rowsWritten : 0;
    log(`[endpoint-run] run complete rows=${rows}`);
    return { rows, error: null };
  } catch (err) {
    return failure(err instanceof Error ? err.message : String(err));
  }
}

/** Writes the result JSON where the fleet harness reads it. */
export function writeResult(path: string, result: EndpointRunResult): void {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, JSON.stringify(result) + "\n");
}

/**
 * The process entry, factored for tests: loads the module, runs it, writes the
 * result file, and returns the exit code (0 success, 1 failure). Every failure
 * — bad argv, bad module, missing env, a thrown run — still writes the result
 * file with its message.
 */
export async function main(
  argv: string[],
  env: Record<string, string | undefined>,
  opts?: EndpointRunOptions,
): Promise<number> {
  const log = opts?.log ?? ((line: string) => console.log(line));
  const resultPath = env.RESULT_PATH || DEFAULT_RESULT_PATH;

  let result: EndpointRunResult;
  try {
    const modulePath = argv[0];
    if (!modulePath) throw new Error("usage: endpoint-run.ts <module>");
    const mod = (await import(modulePath)) as { default?: WorkerDefinition; worker?: WorkerDefinition };
    const worker = mod.default ?? mod.worker;
    if (!worker || worker.kind !== "worker") {
      throw new Error("module does not default-export a worker");
    }
    result = await endpointRun(worker, env, opts);
  } catch (err) {
    result = failure(err instanceof Error ? err.message : String(err));
  }

  if (result.error !== null) log(`[endpoint-run] failed: ${result.error}`);

  try {
    writeResult(resultPath, result);
    log(`[endpoint-run] result written to ${resultPath}`);
  } catch (err) {
    log(`[endpoint-run] could not write ${resultPath}: ${err instanceof Error ? err.message : String(err)}`);
    return 1;
  }

  return result.error === null ? 0 : 1;
}

// Run only as a direct bun entry (`bun endpoint-run.ts <module>`); under a test
// runner import.meta.main is absent/false and this stays inert.
if ((import.meta as { main?: boolean }).main === true) {
  main(process.argv.slice(2), process.env).then((code) => process.exit(code));
}
