// Typed clients for the Verglas access, admin, scheduler, and local container-runtime APIs.
// Product control planes use these clients instead of duplicating HTTP route logic.

import { databasePathSegment, makeTransport, type Transport } from "./http";
import type {QueryAt} from "./types";

const DEFAULT_TIMEOUT_MS = 30_000;

/** Options shared by control-plane connectors. */
export interface ControlConnectOptions {
  /** Base URL for the selected control-plane listener. */
  endpoint: string;
  /** Bearer credential accepted by the listener. */
  token?: string;
  /** Optional fetch implementation for non-browser runtimes and tests. */
  fetch?: typeof fetch;
  /** Request timeout in milliseconds. */
  timeoutMs?: number;
}

/** Creates a JSON transport after validating the shared connector options. */
function transport(options: ControlConnectOptions): Transport {
  if (!options.endpoint) throw new Error("control connect: endpoint is required");
  const fetchImpl = options.fetch ?? globalThis.fetch;
  if (typeof fetchImpl !== "function") {
    throw new Error("control connect: no global fetch; pass ControlConnectOptions.fetch");
  }
  return makeTransport(
    options.endpoint.replace(/\/+$/, ""),
    options.token ?? "",
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

/** Public resolved declaration for one dynamic tenant database. */
export type DatabaseView =
  | {
      /** Discriminator for an Iceberg lakehouse. */
      type: "lakehouse";
      /** Stable tenant-local database name. */
      name: string;
      /** Managed storage or a customer S3 path selected through a scoped secret. */
      storage: { mode: "managed" } | { mode: "scoped-secret"; data_path: string };
      /** Managed Lakekeeper or a customer Iceberg REST catalog. */
      catalog:
        | { mode: "managed-lakekeeper" }
        | { mode: "external"; uri: string; warehouse: string };
    }
  | {
      /** Discriminator for an independent managed Postgres database. */
      type: "postgres";
      /** Stable tenant-local database name. */
      name: string;
      /** Managed Postgres engine selection. */
      engine: { mode: "managed-neon" };
    };

/** Create request accepted by the dynamic tenant database API. */
export type CreateDatabaseRequest = DatabaseView;

/** Bounded run-history entry returned by the scheduler. */
export interface JobSummary {
  job_id: string;
  worker: string;
  state: string;
  ready_at: string;
  created_at: string;
  completed_at?: string;
  rows_produced?: number;
  error_message?: string;
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
export class VerglasAdminClient {
  /** Binds this client to one validated transport. */
  constructor(private readonly transport: Transport) {}

  /** Executes a SQL statement against one tenant database through the bounded JSON query route. */
  query(database: string, sql: string, at?: QueryAt): Promise<{
    columns: string[];
    rows: Record<string, unknown>[];
    row_count: number;
  }> {
    if (!sql) throw new Error("query: sql is required");
    return this.transport.request("POST", `/v1/databases/${databasePathSegment(database)}/query`, {
      body: {sql, ...(at === undefined ? {} : {at})},
    });
  }

  /** Performs a typed JSON GET for catalog routes without a dedicated method. */
  getJson<T>(path: string, query?: Record<string, string | number | undefined>): Promise<T> {
    return this.transport.request<T>("GET", path, { query });
  }

  /** Performs a typed JSON POST for catalog routes without a dedicated method. */
  postJson<T>(
    path: string,
    body: unknown,
    query?: Record<string, string | number | undefined>,
  ): Promise<T> {
    return this.transport.request<T>("POST", path, { body, query });
  }

  /** Performs a typed JSON DELETE for catalog routes without a dedicated method. */
  deleteJson<T>(path: string, query?: Record<string, string | number | undefined>): Promise<T> {
    return this.transport.request<T>("DELETE", path, { query });
  }
}

/** Client for protected database resources on the tenant access service. */
export class VerglasAccessClient {
  /** Binds this client to one validated access-service transport. */
  constructor(private readonly transport: Transport) {}

  /** Lists public definitions for every database in the connected tenant. */
  listDatabases(): Promise<DatabaseView[]> {
    return this.transport
      .request<{ databases: DatabaseView[] }>("GET", "/v1/databases")
      .then((body) => body.databases);
  }

  /** Returns one public database definition by tenant-local name. */
  getDatabase(name: string): Promise<DatabaseView> {
    return this.transport.request<DatabaseView>(
      "GET",
      `/v1/databases/${encodeURIComponent(name)}`,
    );
  }

  /** Creates one dynamic database from an explicit managed or scoped declaration. */
  createDatabase(request: CreateDatabaseRequest): Promise<DatabaseView> {
    return this.transport.request<DatabaseView>("POST", "/v1/databases", { body: request });
  }

  /** Deletes one database by tenant-local name. */
  deleteDatabase(name: string): Promise<void> {
    return this.transport.request<void>(
      "DELETE",
      `/v1/databases/${encodeURIComponent(name)}`,
    );
  }
}

/** Client for scheduler secrets and bounded job history. */
export class VerglasSchedulerClient {
  /** Binds this client to one validated transport. */
  constructor(private readonly transport: Transport) {}

  /** Lists current workers or every retained worker revision. */
  listWorkers(view: "active" | "all" = "active"): Promise<WorkerRow[]> {
    return this.transport.request<WorkerRow[]>("GET", "/v1/workers", { query: {view} });
  }

  /** Returns the current revision for one worker. */
  getWorker(name: string): Promise<WorkerRow> {
    return this.transport.request<WorkerRow>("GET", `/v1/workers/${encodeURIComponent(name)}`);
  }

  /** Registers a new immutable worker revision. */
  registerWorker(spec: WorkerSpec): Promise<WorkerRow> {
    return this.transport.request<WorkerRow>("POST", "/v1/workers", {body: spec});
  }

  /** Changes whether the scheduler may run a worker. */
  setWorkerState(name: string, state: "running" | "paused" | "archived"): Promise<WorkerRow> {
    return this.transport.request<WorkerRow>(
      "PUT",
      `/v1/workers/${encodeURIComponent(name)}/state`,
      {body: {state}},
    );
  }

  /** Enqueues an immediate run with caller-provided idempotency. */
  runWorker(name: string, idempotencyKey: string): Promise<{job_id: string; created: boolean}> {
    return this.transport.request<{job_id: string; created: boolean}>(
      "POST",
      `/v1/workers/${encodeURIComponent(name)}/run`,
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

  /** Lists recent jobs belonging to one worker. */
  listWorkerJobs(name: string, limit = 20): Promise<JobSummary[]> {
    return this.transport.request<JobSummary[]>(
      "GET",
      `/v1/workers/${encodeURIComponent(name)}/jobs`,
      { query: { limit } },
    );
  }

  /** Returns the current state of one job. */
  getJob(jobId: string): Promise<JobSummary> {
    return this.transport.request<JobSummary>("GET", `/v1/jobs/${encodeURIComponent(jobId)}`);
  }
}

/** Client for Application and Integration Vessels on the local container runtime. */
export class VerglasRuntimeClient {
  readonly #endpoint: string;
  readonly #token: string;
  readonly #fetch: typeof fetch;

  /** Binds runtime operations and raw probes to one listener. */
  constructor(
    private readonly transport: Transport,
    endpoint: string,
    token: string,
    fetchImpl: typeof fetch,
  ) {
    this.#endpoint = endpoint;
    this.#token = token;
    this.#fetch = fetchImpl;
  }

  /** Returns the normalized runtime endpoint. */
  get endpoint(): string {
    return this.#endpoint;
  }

  /** Lists all Vessels known to the runtime. */
  listVessels<T = unknown>(): Promise<T[]> {
    return this.transport.request<T[]>("GET", "/v1/vessels");
  }

  /** Removes one Vessel and its managed container. */
  deleteVessel(name: string): Promise<void> {
    return this.transport.request<void>("DELETE", `/v1/vessels/${encodeURIComponent(name)}`);
  }

  /** Creates or replaces a low-level Vessel declaration. */
  putVessel(name: string, body: unknown): Promise<void> {
    return this.transport.request<void>("PUT", `/v1/vessels/${encodeURIComponent(name)}`, { body });
  }

  /** Stops a Vessel and persists that it must remain stopped across reconciliation. */
  stopVessel(name: string): Promise<void> {
    return this.transport.request<void>(
      "POST",
      `/v1/vessels/${encodeURIComponent(name)}/stop`,
    );
  }

  /** Resumes a Vessel and persists that reconciliation must keep it running. */
  resumeVessel(name: string): Promise<unknown> {
    return this.transport.request(
      "POST",
      `/v1/vessels/${encodeURIComponent(name)}/resume`,
    );
  }

  /** Builds and applies a standalone Vessel project. */
  putVesselProject(name: string, body: unknown): Promise<unknown> {
    return this.transport.request("PUT", `/v1/vessels/${encodeURIComponent(name)}/project`, {
      body,
    });
  }

  /** Applies a composable Vessel manifest. */
  putVesselComposition(name: string, body: unknown): Promise<unknown> {
    return this.transport.request("PUT", `/v1/vessels/${encodeURIComponent(name)}/composition`, {
      body,
    });
  }

  /** Calls a Vessel's proxied JSON API. */
  vesselHttp<T = unknown>(
    name: string,
    path: string,
    init?: { method?: "GET" | "POST" | "PUT" | "DELETE"; body?: unknown },
  ): Promise<T> {
    const method = init?.method ?? "GET";
    const suffix = path.startsWith("/") ? path : `/${path}`;
    return this.transport.request<T>(
      method,
      `/v1/vessels/${encodeURIComponent(name)}/http${suffix}`,
      { body: init?.body },
    );
  }

  /** Performs an authorized request without converting non-success responses to exceptions. */
  fetch(path: string, init?: RequestInit): Promise<Response> {
    const headers = new Headers(init?.headers);
    headers.set("authorization", `Bearer ${this.#token}`);
    const relativePath = path.startsWith("/") ? path : `/${path}`;
    return this.#fetch(`${this.#endpoint}${relativePath}`, { ...init, headers });
  }

  /** Returns the runtime-proxied local preview URL for a Vessel. */
  previewUrl(name: string): string {
    return `${this.#endpoint}/apps/${encodeURIComponent(name)}/`;
  }
}

/** Connects to the database-scoped data-query and catalog listener. */
export function connectAdmin(options: ControlConnectOptions): VerglasAdminClient {
  return new VerglasAdminClient(transport(options));
}

/** Connects to the mandatory tenant access and database-resource service. */
export function connectAccess(options: ControlConnectOptions): VerglasAccessClient {
  if (!options.token) throw new Error("connectAccess: token is required");
  return new VerglasAccessClient(transport(options));
}

/** Connects to the scheduler control listener. */
export function connectScheduler(options: ControlConnectOptions): VerglasSchedulerClient {
  if (!options.token) throw new Error("connectScheduler: token is required");
  return new VerglasSchedulerClient(transport(options));
}

/** Connects to the local container-runtime listener. */
export function connectRuntime(options: ControlConnectOptions): VerglasRuntimeClient {
  if (!options.token) throw new Error("connectRuntime: token is required");
  const endpoint = options.endpoint.replace(/\/+$/, "");
  const fetchImpl = options.fetch ?? globalThis.fetch;
  if (typeof fetchImpl !== "function") throw new Error("connectRuntime: no global fetch");
  return new VerglasRuntimeClient(
    transport({ ...options, endpoint }),
    endpoint,
    options.token,
    fetchImpl.bind(globalThis),
  );
}
