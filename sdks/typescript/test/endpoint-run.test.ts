// endpoint-run: the fleet commit-endpoint entry. One invocation is one bounded
// worker run for one scheduled interval — it builds the client, the cron trigger
// (from the logical-time env bindings), and the WorkerContext, runs the worker's
// handler through runWorker (idempotency-keyed appends, run logging), and writes
// the result-file contract the fleet harness reads. There is no watermark.

import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { defineWorker, type WorkerContext } from "../src/index";
import { endpointRun, main, type EndpointRunResult } from "../src/subprocess/endpoint-run";

/** One request the fake transport saw. */
interface RecordedRequest {
  method: string;
  path: string;
  idempotencyKey: string | null;
  body?: Record<string, unknown>;
}

/**
 * A fake commit service injected through ConnectOptions' fetch seam. Records
 * every request; implements the commit route with monotonically increasing
 * sequence watermarks. No watermark route — the worker model does not use one.
 */
function fakeEndpoint() {
  const requests: RecordedRequest[] = [];
  let seq = 0;

  const json = (body: unknown, status = 200) =>
    new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });

  const fetchImpl = (async (input: RequestInfo | URL, init?: RequestInit) => {
    const request = new Request(input as RequestInfo, init);
    const url = new URL(request.url);
    const body =
      request.method === "GET"
        ? undefined
        : ((await request.json().catch(() => undefined)) as Record<string, unknown> | undefined);
    requests.push({
      method: request.method,
      path: url.pathname,
      idempotencyKey: request.headers.get("idempotency-key"),
      body,
    });

    if (/^\/v1\/tables\/[^/]+\/commit$/.test(url.pathname)) {
      const rows = (body?.rows as unknown[] | undefined) ?? [];
      seq += rows.length;
      return json({ snapshotId: `snap-${seq}`, rowsCommitted: rows.length, watermark: String(seq) });
    }
    return json({ error: `no route for ${url.pathname}` }, 404);
  }) as typeof fetch;

  return { requests, fetch: fetchImpl };
}

const TOKEN = "tenant-token-s3cr3t";

/** A well-formed env for the run mode (endpoint is fake; nothing leaves the test). */
function baseEnv(extra: Record<string, string> = {}): Record<string, string> {
  return {
    VERGLAS_ENDPOINT: "https://commit.example",
    VERGLAS_TOKEN: TOKEN,
    DEPLOYMENT: "demo-worker",
    TARGET: "app.t",
    ...extra,
  };
}

const quiet = { log: () => {} };

/** Commit requests against the target table (log commits go to app.t_LOGS). */
const targetCommits = (requests: RecordedRequest[]) =>
  requests.filter((r) => r.method === "POST" && r.path === "/v1/tables/app.t/commit");

describe("endpointRun worker semantics", () => {
  it("passes the configured output and the cron trigger interval into ctx", async () => {
    const fake = fakeEndpoint();
    let seenOutput: string | undefined;
    let seenTrigger: unknown;
    const worker = defineWorker(async (ctx: WorkerContext) => {
      seenOutput = ctx.output;
      seenTrigger = ctx.trigger;
      const r = await ctx.client.table(ctx.output).append([{ n: 1 }]);
      return { rowsWritten: r.rowsCommitted };
    });

    const env = baseEnv({
      VERGLAS_INTERVAL_START: "2026-08-01T00:00:00Z",
      VERGLAS_INTERVAL_END: "2026-08-02T00:00:00Z",
    });
    const result = await endpointRun(worker, env, { fetch: fake.fetch, ...quiet });
    expect(result.error).toBeNull();
    expect(seenOutput).toBe("app.t");
    expect(seenTrigger).toEqual({
      type: "cron",
      intervalStart: "2026-08-01T00:00:00Z",
      intervalEnd: "2026-08-02T00:00:00Z",
    });
  });

  it("maps the control plane's VERGLAS_* payload vars onto the cron trigger event", async () => {
    const fake = fakeEndpoint();
    let seenTrigger: unknown;
    const worker = defineWorker((ctx: WorkerContext) => {
      seenTrigger = ctx.trigger;
      return { rowsWritten: 0 };
    });

    const env = baseEnv({
      VERGLAS_TRIGGER: "cron",
      VERGLAS_LOGICAL_DATE: "2026-08-01T13:05:00.000Z",
      VERGLAS_INTERVAL_START: "2026-08-01T13:05:00.000Z",
      VERGLAS_INTERVAL_END: "2026-08-01T13:10:00.000Z",
    });
    const result = await endpointRun(worker, env, { fetch: fake.fetch, ...quiet });
    expect(result.error).toBeNull();
    expect(seenTrigger).toEqual({
      type: "cron",
      logicalDate: "2026-08-01T13:05:00.000Z",
      intervalStart: "2026-08-01T13:05:00.000Z",
      intervalEnd: "2026-08-01T13:10:00.000Z",
    });
  });

  it("ignores unprefixed INTERVAL_* env vars", async () => {
    const fake = fakeEndpoint();
    let seenTrigger: unknown;
    const worker = defineWorker((ctx: WorkerContext) => {
      seenTrigger = ctx.trigger;
    });

    const env = baseEnv({
      INTERVAL_START: "1999-01-01T00:00:00.000Z",
      INTERVAL_END: "2026-08-01T13:10:00.000Z",
    });
    const result = await endpointRun(worker, env, { fetch: fake.fetch, ...quiet });
    expect(result.error).toBeNull();
    expect(seenTrigger).toEqual({ type: "cron" });
  });

  it("no trigger vars at all still runs as a bare cron trigger", async () => {
    const fake = fakeEndpoint();
    let seenTrigger: unknown;
    const worker = defineWorker((ctx: WorkerContext) => {
      seenTrigger = ctx.trigger;
    });

    const result = await endpointRun(worker, baseEnv(), { fetch: fake.fetch, ...quiet });
    expect(result.error).toBeNull();
    expect(seenTrigger).toEqual({ type: "cron" });
  });

  it("refuses a non-cron VERGLAS_TRIGGER without running the worker", async () => {
    const fake = fakeEndpoint();
    let ran = false;
    const worker = defineWorker(() => {
      ran = true;
    });

    const result = await endpointRun(worker, baseEnv({ VERGLAS_TRIGGER: "webhook" }), {
      fetch: fake.fetch,
      ...quiet,
    });
    expect(result.rows).toBe(0);
    expect(result.error).toContain('unsupported VERGLAS_TRIGGER "webhook"');
    expect(ran).toBe(false);
    expect(fake.requests).toHaveLength(0);
  });

  it("idempotency-keyed appends survive to the commit route and report the row count", async () => {
    const fake = fakeEndpoint();
    const worker = defineWorker(async (ctx: WorkerContext) => {
      const r = await ctx.client.table(ctx.output).append([{ n: 1 }, { n: 2 }], { idempotencyKey: "k1" });
      return { rowsWritten: r.rowsCommitted };
    });

    const result = await endpointRun(worker, baseEnv(), { fetch: fake.fetch, ...quiet });
    expect(result).toEqual<EndpointRunResult>({ rows: 2, error: null });

    const commits = targetCommits(fake.requests);
    expect(commits).toHaveLength(1);
    expect(commits[0].idempotencyKey).toBe("k1");
  });

  it("returns the error result when the worker throws", async () => {
    const fake = fakeEndpoint();
    const worker = defineWorker(() => {
      throw new Error("upstream down");
    });

    const result = await endpointRun(worker, baseEnv(), { fetch: fake.fetch, ...quiet });
    expect(result.rows).toBe(0);
    expect(result.error).toContain("upstream down");
  });

  it("reports missing env cleanly without touching the endpoint", async () => {
    const fake = fakeEndpoint();
    const worker = defineWorker(() => ({ rowsWritten: 0 }));

    const result = await endpointRun(worker, { DEPLOYMENT: "demo-worker" }, { fetch: fake.fetch, ...quiet });
    expect(result.rows).toBe(0);
    expect(result.error).toContain("VERGLAS_ENDPOINT");
    expect(result.error).toContain("VERGLAS_TOKEN");
    expect(result.error).toContain("TARGET");
    expect(fake.requests).toHaveLength(0);
  });
});

describe("endpoint-run main: result file and exit code", () => {
  const fixture = join(__dirname, "fixtures", "endpoint-run-worker.ts");
  let dir: string;
  const readResult = (): EndpointRunResult =>
    JSON.parse(readFileSync(join(dir, "result.json"), "utf8")) as EndpointRunResult;

  afterEach(() => {
    rmSync(dir, { recursive: true, force: true });
    delete process.env.ENDPOINT_RUN_THROW;
  });

  function envWithResultPath(extra: Record<string, string> = {}): Record<string, string> {
    dir = mkdtempSync(join(tmpdir(), "endpoint-run-"));
    return baseEnv({ RESULT_PATH: join(dir, "result.json"), ...extra });
  }

  it("writes the success result, exits 0, and never prints the token", async () => {
    const fake = fakeEndpoint();
    const lines: string[] = [];
    const code = await main([fixture], envWithResultPath(), { fetch: fake.fetch, log: (l) => lines.push(l) });
    expect(code).toBe(0);
    expect(readResult()).toEqual({ rows: 2, error: null });
    expect(lines.every((l) => l.startsWith("[endpoint-run]"))).toBe(true);
    expect(lines.join("\n")).not.toContain(TOKEN);
  });

  it("writes the error result and exits 1 when the worker throws", async () => {
    const fake = fakeEndpoint();
    process.env.ENDPOINT_RUN_THROW = "1";
    const code = await main([fixture], envWithResultPath(), { fetch: fake.fetch, ...quiet });
    expect(code).toBe(1);
    const result = readResult();
    expect(result.rows).toBe(0);
    expect(result.error).toContain("boom");
  });

  it("writes a clean error result and exits 1 on missing env", async () => {
    const fake = fakeEndpoint();
    dir = mkdtempSync(join(tmpdir(), "endpoint-run-"));
    const env = { RESULT_PATH: join(dir, "result.json") };
    const code = await main([fixture], env, { fetch: fake.fetch, ...quiet });
    expect(code).toBe(1);
    const result = readResult();
    expect(result).toEqual({ rows: 0, error: expect.stringContaining("missing required env") });
    expect(fake.requests).toHaveLength(0);
  });

  it("exits 1 with a usage error when no module argument is given", async () => {
    const code = await main([], envWithResultPath(), quiet);
    expect(code).toBe(1);
    expect(readResult().error).toContain("usage");
  });

  it("exits 1 when the module does not export a worker", async () => {
    const notAWorker = join(__dirname, "mock-endpoint.ts");
    const code = await main([notAWorker], envWithResultPath(), quiet);
    expect(code).toBe(1);
    expect(readResult().error).toContain("does not default-export a worker");
  });
});
