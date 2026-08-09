import {
  extractWorkerSource,
  type JobSummary,
  type WorkerRow,
} from "@verglas/sdk";
import type {
  VerglasCatalogSnapshot,
  VerglasGraphSummary,
  VerglasIntegrationConfiguration,
  VerglasQueryResult,
  VerglasTableSummary,
  VerglasVesselSummary,
  VerglasVectorSummary,
  VerglasWorkerDetail,
  VerglasWorkerRunSummary,
  VerglasWorkerSummary,
} from "@verglas/workshop-shared/api";
import { verglasAdmin, verglasRuntime, verglasScheduler, type VerglasClientEnv } from "./verglas-clients";

/** Environment values used by the local Verglas catalog adapter. */
export type VerglasCatalogEnv = VerglasClientEnv;

type IcebergTableIdentifier = {
  namespace: string[];
  name: string;
};

type VectorIndexWire = {
  target?: string;
  field?: string;
  metric?: string;
  reflected_snapshot?: number;
  reflectedSnapshot?: number;
  live_count?: number;
  liveCount?: number;
};

function mapWorker(row: WorkerRow, runs?: VerglasWorkerRunSummary[]): VerglasWorkerSummary {
  return {
    name: row.name,
    state: row.state,
    placement: row.placement,
    output: row.output ?? "",
    triggers: row.triggers,
    createdBy: row.created_by,
    revision: row.revision,
    createdAt: row.created_at,
    recentRuns: runs,
    activeRun: runs?.some((run) => run.state === "running" || run.state === "pending"),
  };
}

function mapRun(job: JobSummary): VerglasWorkerRunSummary {
  return {
    jobId: job.job_id,
    state: job.state,
    createdAt: job.created_at,
    completedAt: job.completed_at,
    rowsProduced: job.rows_produced,
    errorMessage: job.error_message,
  };
}

/** Reads local worker and Vessel metadata through the official Verglas SDK. */
export class VerglasCatalogClient {
  readonly #env: VerglasCatalogEnv;
  readonly #fetch: typeof fetch;

  constructor(env: VerglasCatalogEnv, fetcher: typeof fetch = fetch) {
    const runtimeEndpoint = env.VERGLAS_CONTAINER_RUNTIME_URL?.trim();
    const runtimeToken = env.VERGLAS_CONTAINER_RUNTIME_TOKEN?.trim();
    if (Boolean(runtimeEndpoint) !== Boolean(runtimeToken)) {
      throw new Error("The local Verglas container runtime URL and token must be configured together.");
    }
    this.#env = env;
    this.#fetch = fetcher.bind(globalThis);
  }

  /** Lists active workers, optionally enriched with recent run dots. */
  async listWorkers(opts?: { withRuns?: boolean }): Promise<VerglasWorkerSummary[]> {
    const admin = verglasAdmin(this.#env, this.#fetch);
    const rows = await admin.listWorkers("active");
    if (!opts?.withRuns) return rows.map((row) => mapWorker(row));
    const scheduler = verglasScheduler(this.#env, this.#fetch);
    return await Promise.all(rows.map(async (row) => {
      try {
        const jobs = await scheduler.listWorkerJobs(row.name, 12);
        return mapWorker(row, jobs.map(mapRun));
      } catch {
        return mapWorker(row);
      }
    }));
  }

  /** Full worker detail for the Jobs page. */
  async getWorker(name: string): Promise<VerglasWorkerDetail> {
    const admin = verglasAdmin(this.#env, this.#fetch);
    const row = await admin.getWorker(name);
    let recentRuns: VerglasWorkerRunSummary[] | undefined;
    try {
      recentRuns = (await verglasScheduler(this.#env, this.#fetch).listWorkerJobs(name, 20)).map(mapRun);
    } catch {
      // Scheduler may be briefly unavailable; detail still useful.
    }
    return {
      ...mapWorker(row, recentRuns),
      sourceCode: extractWorkerSource(row.config),
      config: row.config,
    };
  }

  async listWorkerJobs(name: string, limit = 20): Promise<VerglasWorkerRunSummary[]> {
    return (await verglasScheduler(this.#env, this.#fetch).listWorkerJobs(name, limit)).map(mapRun);
  }

  async runWorker(name: string, idempotencyKey: string): Promise<{jobId: string, created: boolean}> {
    const result = await verglasAdmin(this.#env, this.#fetch).runWorker(name, idempotencyKey);
    return {jobId: result.job_id, created: result.created};
  }

  async setWorkerState(name: string, state: "running" | "paused" | "archived"): Promise<void> {
    await verglasAdmin(this.#env, this.#fetch).setWorkerState(name, state);
  }

  async listTables(): Promise<VerglasTableSummary[]> {
    const admin = verglasAdmin(this.#env, this.#fetch);
    const {warehouse} = await admin.getJson<{warehouse?: string}>("/admin/access");
    if (!warehouse) throw new Error("Verglas catalog access did not include a warehouse.");

    const config = await admin.getJson<{
      overrides?: {prefix?: string};
      defaults?: {prefix?: string};
    }>(`/catalog/v1/config`, {warehouse});
    const prefix = config.overrides?.prefix ?? config.defaults?.prefix;
    if (!prefix) throw new Error("Verglas catalog configuration did not include a prefix.");

    const catalogBase = `/catalog/v1/${encodeURIComponent(prefix)}`;
    const namespaceBody = await admin.getJson<{namespaces?: string[][]}>(`${catalogBase}/namespaces`);
    const namespaces = (namespaceBody.namespaces ?? []).slice(0, 100);
    const identifiers: IcebergTableIdentifier[] = [];
    for (const namespace of namespaces) {
      const encoded = encodeURIComponent(namespace.join("\u001f"));
      const tableBody = await admin.getJson<{identifiers?: IcebergTableIdentifier[]}>(
        `${catalogBase}/namespaces/${encoded}/tables`,
      );
      const remaining = 1000 - identifiers.length;
      identifiers.push(...(tableBody.identifiers ?? []).slice(0, Math.min(500, remaining)));
      if (identifiers.length === 1000) break;
    }

    return identifiers.map(({namespace, name}) => ({
      namespace,
      name,
      qualifiedName: [...namespace, name].map(quoteIdentifier).join("."),
    })).toSorted((a, b) => a.qualifiedName.localeCompare(b.qualifiedName));
  }

  async getCatalog(): Promise<VerglasCatalogSnapshot> {
    const tables = await this.listTables();
    const graphs = inferGraphs(tables);
    const vectors = await this.#listVectors(tables, graphs);
    return {tables, vectors, graphs};
  }

  async #listVectors(
    tables: VerglasTableSummary[],
    graphs: VerglasGraphSummary[],
  ): Promise<VerglasVectorSummary[]> {
    const admin = verglasAdmin(this.#env, this.#fetch);
    try {
      const body = await admin.getJson<VectorIndexWire[] | {indexes?: VectorIndexWire[]}>("/v1/indexes");
      return normalizeVectors(Array.isArray(body) ? body : body.indexes ?? []);
    } catch {
      // Older local runtimes expose only target-scoped index discovery.
    }

    const paths = [
      ...tables.map((table) => ({
        path: `/v1/tables/${encodeURIComponent([...table.namespace, table.name].join("."))}/indexes`,
        target: `tbl:${[...table.namespace, table.name].join(".")}`,
      })),
      ...graphs.map((graph) => ({
        path: `/v1/graphs/${encodeURIComponent(graph.namespace)}/indexes`,
        target: `graph:${graph.namespace}`,
      })),
    ];
    const discovered: VectorIndexWire[] = [];
    for (let offset = 0; offset < paths.length; offset += 16) {
      const batch = paths.slice(offset, offset + 16);
      const pages = await Promise.all(batch.map(async ({path, target}) => {
        try {
          const body = await admin.getJson<{indexes?: VectorIndexWire[]}>(path);
          return (body.indexes ?? []).map((index) => ({...index, target: index.target ?? target}));
        } catch {
          return [];
        }
      }));
      discovered.push(...pages.flat());
    }
    return normalizeVectors(discovered);
  }

  async query(sql: string, maxRows = 100): Promise<VerglasQueryResult> {
    const normalizedSql = sql.trim().replace(/;+\s*$/, "");
    if (!normalizedSql) throw new Error("SQL query is required.");
    const boundedRows = Math.min(Math.max(Math.trunc(maxRows), 1), 500);
    const boundedSql = `SELECT * FROM (${normalizedSql}) AS __verglas_query LIMIT ${boundedRows + 1}`;
    const result = await verglasAdmin(this.#env, this.#fetch).query(boundedSql);
    const truncated = result.rows.length > boundedRows;
    const rows = result.rows.slice(0, boundedRows);
    return {columns: result.columns, rows, rowCount: rows.length, truncated};
  }

  async listVessels(): Promise<VerglasVesselSummary[]> {
    const runtime = verglasRuntime(this.#env, this.#fetch);
    const vessels = await runtime.listVessels<VerglasVesselSummary>();
    return await Promise.all(vessels.map(async (vessel) => {
      if (vessel.role === "application") {
        return {...vessel, previewUrl: runtime.previewUrl(vessel.name)};
      }
      try {
        const schema = await runtime.vesselHttp<Pick<VerglasIntegrationConfiguration, "title" | "description">>(
          vessel.name,
          "/v1/config/schema",
        );
        return {...vessel, title: schema.title, description: schema.description};
      } catch {
        return vessel;
      }
    }));
  }

  async getIntegrationConfiguration(name: string): Promise<VerglasIntegrationConfiguration> {
    const runtime = verglasRuntime(this.#env, this.#fetch);
    const [schema, state] = await Promise.all([
      runtime.vesselHttp<Omit<VerglasIntegrationConfiguration, "configured">>(name, "/v1/config/schema"),
      runtime.vesselHttp<{configured: boolean}>(name, "/v1/config"),
    ]);
    return {...schema, configured: state.configured};
  }

  async configureIntegration(name: string, values: Record<string, string>): Promise<void> {
    await verglasRuntime(this.#env, this.#fetch).vesselHttp(name, "/v1/config", {
      method: "PUT",
      body: values,
    });
  }

  async deleteVessel(name: string): Promise<void> {
    try {
      await verglasRuntime(this.#env, this.#fetch).deleteVessel(name);
    } catch (error) {
      if (error && typeof error === "object" && "status" in error && error.status === 404) return;
      throw error;
    }
  }
}

function quoteIdentifier(value: string): string {
  return `"${value.replaceAll('"', '""')}"`;
}

function inferGraphs(tables: VerglasTableSummary[]): VerglasGraphSummary[] {
  const byNamespace = new Map<string, Map<string, VerglasTableSummary>>();
  for (const table of tables) {
    if (table.namespace.length !== 1) continue;
    const namespace = table.namespace[0];
    const entries = byNamespace.get(namespace) ?? new Map<string, VerglasTableSummary>();
    entries.set(table.name, table);
    byNamespace.set(namespace, entries);
  }
  const graphs: VerglasGraphSummary[] = [];
  for (const [namespace, entries] of byNamespace) {
    const nodes = entries.get("nodes");
    const edges = entries.get("edges");
    if (nodes && edges) {
      graphs.push({namespace, nodesTable: nodes.qualifiedName, edgesTable: edges.qualifiedName});
    }
  }
  return graphs.toSorted((a, b) => a.namespace.localeCompare(b.namespace));
}

function normalizeVectors(indexes: VectorIndexWire[]): VerglasVectorSummary[] {
  const vectors = indexes.flatMap((index): VerglasVectorSummary[] => {
    if (!index.target || !index.field) return [];
    return [{
      target: index.target,
      field: index.field,
      metric: index.metric ?? "cosine",
      reflectedSnapshot: index.reflected_snapshot ?? index.reflectedSnapshot,
      liveCount: index.live_count ?? index.liveCount,
    }];
  });
  return vectors.toSorted((a, b) =>
    a.target.localeCompare(b.target) || a.field.localeCompare(b.field));
}
