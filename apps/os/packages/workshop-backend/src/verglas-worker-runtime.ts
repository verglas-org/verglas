import { connectAdmin, connectScheduler } from "@verglas/sdk";

/** Environment values used to register and configure local Verglas workers. */
export interface VerglasWorkerRuntimeEnv {
  VERGLAS_ADMIN_URL?: string;
  VERGLAS_SCHEDULER_URL?: string;
  VERGLAS_SCHEDULER_CONTROL_TOKEN?: string;
}

/** One portable worker deployment written to Verglas's append-only registry. */
export interface VerglasWorkerDeployment {
  name: string;
  code: string;
  triggers: string;
  output: string;
  config: string;
  created_by: string;
}

/** Resolved endpoints for the local worker control plane. */
export interface VerglasWorkerRuntimeConfig {
  adminEndpoint: string;
  schedulerEndpoint: string;
  schedulerToken: string;
}

/** Rejects generated modules that do not implement the executable Verglas worker contract. */
export function validateVerglasWorkerModule(module: string): void {
  const requirements: Array<[RegExp, string]> = [
    [/from\s+["']\/sdks\/typescript\/src\/index\.ts["']/, "import the bundled Verglas SDK"],
    [/defineWorker\s*(?:<[^>]+>)?\s*\(/, "call defineWorker()"],
    [/export\s+default\b/, "default-export the worker definition"],
    [/\bhandler\s*\(/, "define a handler(ctx) method"],
    [/ctx\.client\.ensureTable\s*\(\s*ctx\.output\s*,\s*\{\s*schema\s*:/s,
      "ensure ctx.output with a schema array"],
    [/ctx\.client\.table\s*\(\s*ctx\.output\s*\)\.append\s*\(/,
      "append rows through ctx.client.table(ctx.output)"],
    [/\browsWritten\b/, "return rowsWritten for scheduler observability"],
  ];
  for (const [pattern, instruction] of requirements) {
    if (!pattern.test(module)) throw new Error(`A Source module must ${instruction}.`);
  }
  if (/ctx\.output\.append\s*\(/.test(module)) {
    throw new Error("ctx.output is a table name; append through ctx.client.table(ctx.output).");
  }
  if (/\b(?:async\s+)?run\s*\(\s*ctx\b/.test(module)) {
    throw new Error("defineWorker() invokes handler(ctx), not run(ctx).");
  }
}

/** Resolves an all-or-nothing local worker control-plane configuration. */
export function resolveVerglasWorkerRuntimeConfig(
    env: VerglasWorkerRuntimeEnv): VerglasWorkerRuntimeConfig | null {
  const adminEndpoint = env.VERGLAS_ADMIN_URL?.trim();
  const schedulerEndpoint = env.VERGLAS_SCHEDULER_URL?.trim();
  const schedulerToken = env.VERGLAS_SCHEDULER_CONTROL_TOKEN?.trim();
  if (!adminEndpoint && !schedulerEndpoint && !schedulerToken) return null;
  if (!adminEndpoint || !schedulerEndpoint || !schedulerToken) {
    throw new Error(
      "VERGLAS_ADMIN_URL, VERGLAS_SCHEDULER_URL, and " +
      "VERGLAS_SCHEDULER_CONTROL_TOKEN must be configured together.",
    );
  }
  return {
    adminEndpoint: adminEndpoint.replace(/\/+$/, ""),
    schedulerEndpoint: schedulerEndpoint.replace(/\/+$/, ""),
    schedulerToken,
  };
}

/** Client for worker declarations, secret bindings, and manual runs via @verglas/sdk. */
export class VerglasWorkerRuntimeClient {
  readonly #adminEndpoint: string;
  readonly #schedulerEndpoint: string;
  readonly #schedulerToken: string;
  readonly #fetch: typeof fetch;

  constructor(config: VerglasWorkerRuntimeConfig, fetcher: typeof fetch = fetch) {
    this.#adminEndpoint = config.adminEndpoint;
    this.#schedulerEndpoint = config.schedulerEndpoint;
    this.#schedulerToken = config.schedulerToken;
    this.#fetch = fetcher.bind(globalThis);
  }

  async putSecret(name: string, value: string): Promise<void> {
    await connectScheduler({
      endpoint: this.#schedulerEndpoint,
      token: this.#schedulerToken,
      fetch: this.#fetch,
    }).putSecret(name, value);
  }

  async register(deployment: VerglasWorkerDeployment): Promise<void> {
    await connectAdmin({
      endpoint: this.#adminEndpoint,
      token: "",
      fetch: this.#fetch,
    }).registerWorker(deployment);
  }

  async run(name: string, idempotencyKey: string): Promise<{job_id: string, created: boolean}> {
    return await connectAdmin({
      endpoint: this.#adminEndpoint,
      token: "",
      fetch: this.#fetch,
    }).runWorker(name, idempotencyKey);
  }
}
