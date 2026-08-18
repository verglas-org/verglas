// Typed client for the Verglas scheduler control plane: /v0/workers (moved off
// the old v1 workers route) and /v1/secrets. Product control planes use
// this client instead of duplicating HTTP route logic.
//
// The retired tenant-local access-service era (v1 access-token and
// authorization CRUD, v1 dynamic database CRUD, v1 Vessel
// container runtime) has no client here. Database queries run through the /v0
// data client's `sql()` (see ./data.ts); tenant catalog and S3 data-plane
// access is unchanged and lives in ./client.ts.

import { makeTransport, type Transport } from "./http";

const DEFAULT_TIMEOUT_MS = 30_000;

/** Options shared by control-plane connectors. */
export interface ControlConnectOptions {
  /** Base URL for the selected control-plane listener. */
  endpoint: string;
  /** Scoped bearer credential accepted by the listener. */
  token: string;
  /** Optional fetch implementation for non-browser runtimes and tests. */
  fetch?: typeof fetch;
  /** Request timeout in milliseconds. */
  timeoutMs?: number;
}

/** Creates a JSON transport after validating the shared connector options. */
function transport(options: ControlConnectOptions): Transport {
  if (!options.endpoint) throw new Error("control connect: endpoint is required");
  if (!options.token) throw new Error("control connect: token is required");
  const fetchImpl = options.fetch ?? globalThis.fetch;
  if (typeof fetchImpl !== "function") {
    throw new Error("control connect: no global fetch; pass ControlConnectOptions.fetch");
  }
  return makeTransport(
    options.endpoint.replace(/\/+$/, ""),
    options.token,
    fetchImpl,
    options.timeoutMs ?? DEFAULT_TIMEOUT_MS,
  );
}

/** One worker revision returned by the admin API. */
export interface WorkerRow {
  name: string;
  code: string;
  triggers: string;
  output: string | null;
  config: string;
  state: string;
  placement: string;
  created_by: string;
  created_at: string;
  revision: number;
}

/** Worker revision accepted by the admin API. */
export interface WorkerSpec {
  name: string;
  code: string;
  triggers: string;
  output: string;
  config: string;
  created_by: string;
}

/** Extracts a TypeScript entrypoint from a worker configuration's file map. */
export function extractWorkerSource(configJson: string): string | undefined {
  try {
    const config = JSON.parse(configJson) as { files?: Record<string, string> };
    const files = config.files ?? {};
    return (
      files["source.ts"] ??
      files["src/worker.ts"] ??
      files["worker.ts"] ??
      Object.entries(files).find(([path]) => path.endsWith(".ts"))?.[1]
    );
  } catch {
    return undefined;
  }
}

/** Client for database-scoped data queries and catalog routes on the admin listener. */
/** Client for scheduler workers (/v0/workers) and secrets (/v1/secrets). */
export class VerglasSchedulerClient {
  /** Binds this client to one validated transport. */
  constructor(private readonly transport: Transport) {}

  /** Lists current workers or every retained worker revision. */
  listWorkers(view: "active" | "all" = "active"): Promise<WorkerRow[]> {
    return this.transport.request<WorkerRow[]>("GET", "/v0/workers", { query: {view} });
  }

  /** Returns the current revision for one worker. */
  getWorker(name: string): Promise<WorkerRow> {
    return this.transport.request<WorkerRow>("GET", `/v0/workers/${encodeURIComponent(name)}`);
  }

  /** Registers a new immutable worker revision. */
  registerWorker(spec: WorkerSpec): Promise<WorkerRow> {
    return this.transport.request<WorkerRow>("POST", "/v0/workers", {body: spec});
  }

  /** Changes whether the scheduler may run a worker. */
  setWorkerState(name: string, state: "running" | "paused" | "archived"): Promise<WorkerRow> {
    return this.transport.request<WorkerRow>(
      "PUT",
      `/v0/workers/${encodeURIComponent(name)}/state`,
      {body: {state}},
    );
  }

  /** Enqueues an immediate run with caller-provided idempotency. */
  runWorker(name: string, idempotencyKey: string): Promise<{job_id: string; created: boolean}> {
    return this.transport.request<{job_id: string; created: boolean}>(
      "POST",
      `/v0/workers/${encodeURIComponent(name)}/run`,
      {headers: {"idempotency-key": idempotencyKey}},
    );
  }

  /** Lists secret names without exposing secret values. */
  listSecretNames(): Promise<string[]> {
    return this.transport
      .request<{ secrets: string[] }>("GET", "/v1/secrets")
      .then((body) => body.secrets);
  }

  /** Creates or replaces one scheduler secret. */
  putSecret(name: string, value: string): Promise<void> {
    return this.transport.request<void>("PUT", `/v1/secrets/${encodeURIComponent(name)}`, {
      body: { value },
    });
  }

  /** Deletes one scheduler secret. */
  deleteSecret(name: string): Promise<void> {
    return this.transport.request<void>("DELETE", `/v1/secrets/${encodeURIComponent(name)}`);
  }
}

/** Connects to the scheduler control listener. */
export function connectScheduler(options: ControlConnectOptions): VerglasSchedulerClient {
  return new VerglasSchedulerClient(transport(options));
}
