import {
  extractWorkerSource,
  type JobSummary,
  type WorkerRow,
} from "@verglas/sdk";
import type {
  VerglasCatalogSnapshot,
  VerglasCreateTableInput,
  VerglasDatabaseDetail,
  VerglasDatabaseSummary,
  VerglasGraphSummary,
  VerglasIntegrationConfiguration,
  VerglasQueryResult,
  VerglasTableSummary,
  VerglasTableDetail,
  VerglasTableUsageMetrics,
  VerglasVesselSummary,
  VerglasVectorSummary,
  VerglasWorkerDetail,
  VerglasWorkerRunSummary,
  VerglasWorkerSummary,
} from "@verglas/workshop-shared/api";
import {
  resolveLocalContainerRuntimeConfigured,
  verglasAdmin,
  verglasRuntime,
  verglasScheduler,
  type VerglasClientEnv,
} from "./verglas-clients";

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

type TableDescribeWire = {
  row_count?: number;
  file_count?: number;
  size_bytes?: number;
  current_snapshot_id?: number;
};

type TableMetricsWire = {
  tables?: Array<{
    table?: string;
    hits?: number;
    misses?: number;
    bytes_served?: number;
    cache_bytes?: number;
    requests_avoided?: number;
    latency_saved_seconds?: number;
  }>;
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
    resolveLocalContainerRuntimeConfigured(env);
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
    return (await this.#listCatalogTables()).tables;
  }

  async #listCatalogTables(): Promise<{namespaces: string[][]; tables: VerglasTableSummary[]}> {
    const {admin, catalogBase} = await this.#catalog();
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

    const tables = identifiers.map(({namespace, name}) => ({
      namespace,
      name,
      qualifiedName: [...namespace, name].map(quoteIdentifier).join("."),
    })).toSorted((a, b) => a.qualifiedName.localeCompare(b.qualifiedName));
    return {namespaces, tables};
  }

  async getCatalog(): Promise<VerglasCatalogSnapshot> {
    const {namespaces, tables} = await this.#listCatalogTables();
    const graphs = inferGraphs(tables);
    const vectors = await this.#listVectors(tables, graphs);
    return {databases: summarizeDatabases(namespaces, tables, vectors, graphs), tables, vectors, graphs};
  }

  /** Returns physical and cache-traffic metrics for the selected database namespace. */
  async getDatabase(name: string): Promise<VerglasDatabaseDetail> {
    const namespace = parseNamespace(name);
    const catalog = await this.getCatalog();
    const database = catalog.databases.find((candidate) => candidate.name === namespace.join("."));
    if (!database) throw new Error(`Database '${name}' was not found.`);
    const usage = await this.#listTableUsage();
    const tables = catalog.tables.filter((table) => sameNamespace(table.namespace, namespace));
    return {
      ...database,
      tables: await Promise.all(tables.map(async (table): Promise<VerglasTableDetail> => {
        const [physical, tableUsage] = await Promise.all([
          this.#describeTable(table),
          Promise.resolve(usage.get([...table.namespace, table.name].join("."))),
        ]);
        return {...table, physical, usage: tableUsage};
      })),
    };
  }

  /** Creates an empty Iceberg namespace, presented as a database by the OS. */
  async createDatabase(name: string): Promise<void> {
    const namespace = parseNamespace(name);
    const {admin, catalogBase} = await this.#catalog();
    await admin.postJson<void>(`${catalogBase}/namespaces`, {namespace, properties: {}});
  }

  /** Deletes an empty Iceberg namespace without cascading to any tables. */
  async deleteDatabase(name: string): Promise<void> {
    const namespace = parseNamespace(name);
    const {admin, catalogBase} = await this.#catalog();
    await admin.deleteJson<void>(`${catalogBase}/namespaces/${encodeNamespace(namespace)}`);
  }

  /** Creates one explicitly-schemaed Iceberg table. */
  async createTable(input: VerglasCreateTableInput): Promise<VerglasTableSummary> {
    const namespace = validateNamespace(input.namespace);
    const name = validateIdentifier(input.name, "Table name");
    if (!input.columns.length) throw new Error("A table requires at least one column.");
    const {admin, catalogBase} = await this.#catalog();
    await admin.postJson<void>(`${catalogBase}/namespaces/${encodeNamespace(namespace)}/tables`,
      createTableRequest(name, input));
    return {namespace, name, qualifiedName: [...namespace, name].map(quoteIdentifier).join(".")};
  }

  /** Deletes one Iceberg table. */
  async deleteTable(namespace: string[], name: string): Promise<void> {
    const validatedNamespace = validateNamespace(namespace);
    const validatedName = validateIdentifier(name, "Table name");
    const {admin, catalogBase} = await this.#catalog();
    await admin.deleteJson<void>(
      `${catalogBase}/namespaces/${encodeNamespace(validatedNamespace)}/tables/${encodeURIComponent(validatedName)}`,
    );
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

  async #catalog(): Promise<{admin: ReturnType<typeof verglasAdmin>; catalogBase: string}> {
    const admin = verglasAdmin(this.#env, this.#fetch);
    const {warehouse} = await admin.getJson<{warehouse?: string}>("/admin/access");
    if (!warehouse) throw new Error("Verglas catalog access did not include a warehouse.");
    const config = await admin.getJson<{
      overrides?: {prefix?: string};
      defaults?: {prefix?: string};
    }>("/catalog/v1/config", {warehouse});
    const prefix = config.overrides?.prefix ?? config.defaults?.prefix;
    if (!prefix) throw new Error("Verglas catalog configuration did not include a prefix.");
    return {admin, catalogBase: `/catalog/v1/${encodeURIComponent(prefix)}`};
  }

  async #describeTable(table: VerglasTableSummary): Promise<VerglasTableDetail["physical"]> {
    try {
      const result = await verglasAdmin(this.#env, this.#fetch).getJson<TableDescribeWire>(
        `/v1/tables/${encodeURIComponent([...table.namespace, table.name].join("."))}/describe`,
      );
      if (typeof result.row_count !== "number" || typeof result.file_count !== "number" ||
          typeof result.size_bytes !== "number") return undefined;
      return {
        rowCount: result.row_count,
        fileCount: result.file_count,
        sizeBytes: result.size_bytes,
        currentSnapshotId: result.current_snapshot_id,
      };
    } catch {
      return undefined;
    }
  }

  async #listTableUsage(): Promise<Map<string, VerglasTableUsageMetrics>> {
    try {
      const result = await verglasAdmin(this.#env, this.#fetch).getJson<TableMetricsWire>("/v1/metering/tables");
      const metrics = new Map<string, VerglasTableUsageMetrics>();
      for (const row of result.tables ?? []) {
        if (!row.table || typeof row.hits !== "number" || typeof row.misses !== "number" ||
            typeof row.bytes_served !== "number" || typeof row.cache_bytes !== "number" ||
            typeof row.requests_avoided !== "number" || typeof row.latency_saved_seconds !== "number") continue;
        metrics.set(row.table, {
          table: row.table,
          hits: row.hits,
          misses: row.misses,
          bytesServed: row.bytes_served,
          cacheBytes: row.cache_bytes,
          requestsAvoided: row.requests_avoided,
          latencySavedSeconds: row.latency_saved_seconds,
        });
      }
      return metrics;
    } catch {
      return new Map();
    }
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

  /** Persists the lifecycle state of an OSS Application Vessel. */
  async setApplicationState(name: string, state: "running" | "stopped"): Promise<void> {
    const runtime = verglasRuntime(this.#env, this.#fetch);
    const vessel = (await runtime.listVessels<VerglasVesselSummary>()).find((candidate) => candidate.name === name);
    if (!vessel) throw new Error(`Application '${name}' was not found.`);
    if (vessel.role !== "application") throw new Error(`Vessel '${name}' is not an Application.`);
    if (state === "running") await runtime.resumeVessel(name);
    else await runtime.stopVessel(name);
  }
}

function quoteIdentifier(value: string): string {
  return `"${value.replaceAll('"', '""')}"`;
}

function parseNamespace(name: string): string[] {
  return validateNamespace(name.split("."));
}

function validateNamespace(namespace: string[]): string[] {
  if (!namespace.length) throw new Error("Database name is required.");
  return namespace.map((part) => validateIdentifier(part, "Database name"));
}

function validateIdentifier(value: string, label: string): string {
  const normalized = value.trim();
  if (!normalized) throw new Error(`${label} is required.`);
  if (normalized.includes("\u001f")) throw new Error(`${label} cannot contain a unit separator.`);
  return normalized;
}

function encodeNamespace(namespace: string[]): string {
  return encodeURIComponent(namespace.join("\u001f"));
}

function sameNamespace(left: string[], right: string[]): boolean {
  return left.length === right.length && left.every((part, index) => part === right[index]);
}

function createTableRequest(name: string, input: VerglasCreateTableInput): unknown {
  const columns = new Map<string, number>();
  const fields = input.columns.map((column, index) => {
    const columnName = validateIdentifier(column.name, "Column name");
    const type = validateIdentifier(column.type, "Column type");
    if (columns.has(columnName)) throw new Error(`Duplicate column '${columnName}'.`);
    const id = index + 1;
    columns.set(columnName, id);
    return {id, name: columnName, required: column.nullable === false, type: catalogType(type)};
  });
  const partitionFields = (input.partitions ?? []).map((partition, index) => {
    const source = validateIdentifier(partition.source, "Partition source");
    const sourceId = columns.get(source);
    if (sourceId === undefined) throw new Error(`Partition source '${source}' is not a table column.`);
    return {
      "source-id": sourceId,
      "field-id": 1000 + index,
      name: `${source}_${partition.transform}`,
      transform: partition.transform,
    };
  });
  return {
    name,
    schema: {type: "struct", "schema-id": 0, fields},
    "partition-spec": {"spec-id": 0, fields: partitionFields},
  };
}

function catalogType(type: string): string {
  switch (type) {
    case "int64": return "long";
    case "int32": return "int";
    case "float64":
    case "double": return "double";
    case "float32":
    case "float": return "float";
    case "utf8":
    case "string": return "string";
    case "bool":
    case "boolean": return "boolean";
    case "date32": return "date";
    default: return type.startsWith("decimal") ? type.replace("decimal128", "decimal") : type;
  }
}

function summarizeDatabases(
  namespaces: string[][],
  tables: VerglasTableSummary[],
  vectors: VerglasVectorSummary[],
  graphs: VerglasGraphSummary[],
): VerglasDatabaseSummary[] {
  const tableCounts = new Map<string, number>();
  for (const table of tables) {
    const name = table.namespace.join(".");
    tableCounts.set(name, (tableCounts.get(name) ?? 0) + 1);
  }
  const graphNamespaces = new Set(graphs.map((graph) => graph.namespace));
  return namespaces.map((namespace) => {
    const name = namespace.join(".");
    return {
    name,
    tableCount: tableCounts.get(name) ?? 0,
    vectorCount: vectors.filter((vector) => vector.target === `tbl:${name}` ||
      vector.target.startsWith(`tbl:${name}.`)).length,
    graph: graphNamespaces.has(name),
    };
  }).toSorted((a, b) => a.name.localeCompare(b.name));
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
