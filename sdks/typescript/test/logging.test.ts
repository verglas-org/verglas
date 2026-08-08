import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
  connect,
  defineWorker,
  runWorker,
  type CloudEvent,
  type WorkerContext,
  type VerglasClient,
} from "../src/index";
import {
  logsCharting,
  logsTableName,
  observabilityFor,
  type LogRow,
} from "../src/logging";
import { startMockEndpoint, type MockEndpoint } from "./mock-endpoint";

let endpoint: MockEndpoint;
let client: VerglasClient;

beforeEach(async () => {
  endpoint = await startMockEndpoint();
  client = connect({ endpoint: endpoint.url, token: endpoint.token });
});
afterEach(() => endpoint.close());

const CRON: CloudEvent = {
  specversion: "1.0",
  id: "tick-1",
  source: "urn:verglas:scheduler:test",
  type: "org.verglas.schedule.tick",
  data: {
    logicalDate: "2026-08-01T00:05:00Z",
    intervalStart: "2026-08-01T00:00:00Z",
    intervalEnd: "2026-08-01T00:05:00Z",
  },
};

/** A minimal WorkerContext; log is a no-op (the runner installs its own logger). */
function ctx(output = "app.points", c: VerglasClient = client): WorkerContext {
  return { verglas: c, client: c, trigger: CRON, output, outputs: [output], env: {}, log: () => {} };
}

/** The log rows written to `<name>_LOGS`, typed. */
function logRows(name: string): LogRow[] {
  return endpoint.tableState(logsTableName(name)).rows.map((r) => r.row as LogRow);
}

const NOW = 1_770_000_000_000; // fixed clock (ms) so ts/day are deterministic

describe("automatic worker logging", () => {
  it("emits run_start, a commit per append, and run_end with the standard shape", async () => {
    const worker = defineWorker(async (c: WorkerContext) => {
      await c.client.table(c.output).append([{ n: 1 }, { n: 2 }]);
      return { rowsWritten: 2 };
    });

    const result = await runWorker(worker, ctx(), { now: () => NOW });
    expect(result?.rowsWritten).toBe(2);

    const rows = logRows("app.points");
    expect(rows.map((r) => r.event)).toEqual(["run_start", "commit", "run_end"]);

    // Standard shape / values.
    const start = rows[0];
    expect(start).toMatchObject({ pipeline: "app.points", kind: "worker", placement: "local", level: "info" });
    expect(start.run_id).toMatch(/.+/);
    expect(start.ts).toBe(`${NOW}000000`);
    expect(start.day).toBe(new Date(NOW).toISOString().slice(0, 10));

    const commit = rows.find((r) => r.event === "commit")!;
    expect(commit).toMatchObject({ rows: 2, watermark: "2", level: "info" });

    const end = rows.find((r) => r.event === "run_end")!;
    expect(end).toMatchObject({ rows: 2, level: "info" });

    // Every row shares one run_id.
    expect(new Set(rows.map((r) => r.run_id)).size).toBe(1);
  });

  it("records an author's own ctx.log steps alongside the automatic ones", async () => {
    const worker = defineWorker(async (c: WorkerContext) => {
      c.log("fetch", { rows: 3, message: "pulled upstream" });
      await c.client.table(c.output).append([{ n: 1 }]);
      return { rowsWritten: 1 };
    });
    await runWorker(worker, ctx(), { now: () => NOW });
    const rows = logRows("app.points");
    const fetch = rows.find((r) => r.event === "fetch")!;
    expect(fetch).toMatchObject({ rows: 3, message: "pulled upstream", kind: "worker" });
  });

  it("drops non-standard ctx.log fields so a stray secret cannot land in logs", async () => {
    const worker = defineWorker((c: WorkerContext) => {
      // e.g. a URL carrying an API key — must not be recorded.
      c.log("fetch", { url: "https://x/?api_key=secret", rows: 1 } as Record<string, unknown>);
      return { rowsWritten: 0 };
    });
    await runWorker(worker, ctx(), { now: () => NOW });
    const rows = logRows("app.points");
    const fetch = rows.find((r) => r.event === "fetch")!;
    expect(fetch.rows).toBe(1);
    expect(JSON.stringify(fetch)).not.toContain("secret");
    expect("url" in fetch).toBe(false);
  });
});

describe("error runs", () => {
  it("logs an error row and a failed run_end, and still surfaces the error", async () => {
    const worker = defineWorker(() => {
      throw new Error("upstream down");
    });

    await expect(runWorker(worker, ctx(), { now: () => NOW })).rejects.toThrow("upstream down");

    const rows = logRows("app.points");
    const err = rows.find((r) => r.event === "error")!;
    expect(err).toMatchObject({ level: "error", error: "upstream down" });
    const end = rows.find((r) => r.event === "run_end")!;
    expect(end.level).toBe("error");
    expect(end.error).toBe("upstream down");
  });

  it("never fails the run when writing logs fails (best-effort)", async () => {
    const warnings: unknown[] = [];
    // A client whose logs-table commit throws; the worker append still works.
    const flaky = connect({
      endpoint: endpoint.url,
      token: endpoint.token,
      fetch: async (input, init) => {
        const url = String(input);
        if (url.includes(encodeURIComponent(logsTableName("app.points")))) {
          throw new Error("logs endpoint unreachable");
        }
        return globalThis.fetch(input as string, init);
      },
    });
    (globalThis as { console: Console }).console.warn = (...a: unknown[]) => warnings.push(a);

    const worker = defineWorker(async (c: WorkerContext) => {
      await c.client.table(c.output).append([{ n: 1 }]);
      return { rowsWritten: 1 };
    });
    const result = await runWorker(worker, ctx("app.points", flaky), { now: () => NOW });
    expect(result?.rowsWritten).toBe(1); // run succeeded despite logging failure
    expect(warnings.length).toBeGreaterThan(0); // failure was reported, not thrown
  });
});

describe("batching + idempotency", () => {
  it("writes all log rows for a run in a single commit to <name>_LOGS", async () => {
    const worker = defineWorker(async (c: WorkerContext) => {
      await c.client.table(c.output).append([{ n: 1 }]);
      await c.client.table(c.output).append([{ n: 2 }]);
      return { rowsWritten: 2 };
    });
    await runWorker(worker, ctx(), { now: () => NOW });

    // Two commits + run_start + run_end = 4 rows, but exactly ONE commit to LOGS.
    const logsCommits = endpoint.requests.filter(
      (r) => r.method === "POST" && r.path.includes(encodeURIComponent(logsTableName("app.points"))),
    );
    expect(logsCommits).toHaveLength(1);
    expect(logRows("app.points").length).toBe(4); // run_start, commit, commit, run_end
  });

  it("does not double-log when a run is retried under the same run id", async () => {
    const worker = defineWorker(async (c: WorkerContext) => {
      await c.client.table(c.output).append([{ n: 1 }]);
      return { rowsWritten: 1 };
    });
    await runWorker(worker, ctx(), { now: () => NOW, runId: "run-fixed" });
    await runWorker(worker, ctx(), { now: () => NOW, runId: "run-fixed" }); // retry

    // The logs commit is idempotency-keyed by run id, so the second flush is a
    // no-op: the logs table holds one run's worth of rows, not two.
    const rows = logRows("app.points");
    expect(new Set(rows.map((r) => r.run_id))).toEqual(new Set(["run-fixed"]));
    expect(rows.filter((r) => r.event === "run_start")).toHaveLength(1);
  });
});

describe("deployment name vs output table", () => {
  it("stamps the deployment name in the pipeline column, logs to <output>_LOGS", async () => {
    // A deployment named `alpha` writing to a shared output `app.points`. The logs
    // TABLE follows the output; the `pipeline` COLUMN carries the deployment name.
    const worker = defineWorker({
      name: "alpha",
      handler: async (c: WorkerContext) => {
        await c.client.table(c.output).append([{ n: 1 }]);
        return { rowsWritten: 1 };
      },
    });
    await runWorker(worker, ctx(), { now: () => NOW });

    const targetLogs = logRows("app.points");
    expect(targetLogs.length).toBeGreaterThan(0);
    expect(new Set(targetLogs.map((l) => l.pipeline))).toEqual(new Set(["alpha"]));
    // Nothing was logged to a name-derived table.
    expect(logRows("alpha")).toHaveLength(0);
  });
});

describe("automatic charting", () => {
  it("declares the standard chart spec over <name>_LOGS", () => {
    const charting = logsCharting("app.points");
    expect(charting.source).toBe(logsTableName("app.points"));
    expect(charting.chart.input).toBe(logsTableName("app.points"));
    expect(charting.chart.timeField).toBe("ts");
    expect(charting.chart.dimensions).toEqual(["event", "kind"]);
    const measureNames = charting.chart.measures.map((m) => m.name);
    expect(measureNames).toEqual(["runs", "errors", "rows", "duration_p50", "duration_p95", "duration_p99"]);
    const errors = charting.chart.measures.find((m) => m.name === "errors")!;
    expect(errors).toMatchObject({ agg: "rate", field: "level", match: "error" });
  });

  it("observabilityFor attaches the charting declaration automatically", () => {
    const obs = observabilityFor("app.points");
    expect(obs.pipeline).toBe("app.points");
    expect(obs.logsTable).toBe(logsTableName("app.points"));
    expect(obs.charting.source).toBe(logsTableName("app.points"));
    // Retention is the serving runtime's job now, not the SDK's — no field here.
    expect("retentionDays" in obs).toBe(false);
  });
});
