import { existsSync, readdirSync, readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

const packageRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const sourceRoot = resolve(packageRoot, "src");
const indexSource = readFileSync(resolve(sourceRoot, "index.ts"), "utf8");

function sourceFiles(directory: string): string[] {
  return readdirSync(directory, { withFileTypes: true }).flatMap((entry) => {
    const path = resolve(directory, entry.name);
    return entry.isDirectory() ? sourceFiles(path) : entry.name.endsWith(".ts") ? [path] : [];
  });
}

const sourceText = sourceFiles(sourceRoot).map((path) => readFileSync(path, "utf8")).join("\n");

const packageJson = JSON.parse(readFileSync(resolve(packageRoot, "package.json"), "utf8")) as {
  exports: Record<string, unknown>;
  scripts: Record<string, string>;
};
const readmeSource = readFileSync(resolve(packageRoot, "README.md"), "utf8");
const forbiddenFiles = [
  ["arrow", "ipc.ts"].join("-"),
  ["do", "protocol.ts"].join("-"),
  "durable-objects.ts",
  "control.ts",
  "contracts.ts",
  "subprocess/endpoint-run.ts",
  "examples/index.ts",
  "examples/http-poll-worker.ts",
  "examples/webhook-worker.ts",
  "examples/change-fanout-worker.ts",
  "data.ts",
  "dashboards.ts",
  "feed.ts",
  "graph-handle.ts",
  "namespace.ts",
  "semantic.ts",
  "semantic-types.ts",
];
const forbiddenTests = [
  "control.test.ts",
  "endpoint-run.test.ts",
  "examples.test.ts",
  "feed.test.ts",
  "graph-handle.test.ts",
  "namespace.test.ts",
  "semantic.test.ts",
  "v0-alignment.test.ts",
];
const forbiddenExports = [
  "DurableObject",
  "StorageBridge",
  "createWorkerRuntime",
  "connectScheduler",
  "extractWorkerSource",
  "VerglasSchedulerClient",
  "ControlConnectOptions",
  "WorkerRow",
  "WorkerSpec",
  "defineWorker",
  "runWorker",
  "WorkerContext",
  "WorkerHandler",
  "WorkerDefinition",
  "WorkerResult",
  "RunWorkerOptions",
  "CloudEvent",
  "TriggerSpec",
  "CronTriggerSpec",
  "WebhookTriggerSpec",
  "EventTriggerSpec",
  "S3VectorsClient",
  "VerglasGraphsClient",
  "Graph",
  "graphFromEnv",
  "GraphEdgeInput",
  "GraphFromEnvOptions",
  "GraphNodeInput",
  "SemanticDocument",
  "SigV4Credentials",
  "createDataClient",
  "DataClient",
  "DataClientOptions",
  "IngestResult",
  "DynamicNamespaceRegistry",
  "DynamicNamespaceNode",
  "NamespaceBinding",
  "NamespaceBindings",
  "NamespaceCall",
  "NamespaceJsonSchema",
  "NamespaceManifest",
  "NamespaceMethod",
  "NamespaceMethodManifest",
  "NamespaceMethodMode",
  "NamespaceRegistry",
  "ChangeEvent",
  "ChangeHandler",
  "FeedSubscription",
  "FollowFeedOptions",
  "FollowHandler",
  "FollowRowsOptions",
  "QueryAt",
];
const forbiddenSourceTokens = [
  "DashboardSource",
  "DashboardElement",
  "DashboardSpec",
  "DashboardDataClient",
  "DashboardStateStore",
  "bindDashboardSources",
  "LogsChartAgg",
  "LogsChartMeasure",
  "LogsChartSpec",
  "LogsCharting",
  "standardLogsChartSpec",
  "logsCharting",
  "WorkerObservability",
  "observabilityFor",
];
const indexTokens = new Set(indexSource.split(/[^A-Za-z0-9_$]+/u).filter(Boolean));

const forbiddenProtocolWords = [
  ["REG", "ISTER"].join(""),
  ["QUE", "RY"].join(""),
  ["COM", "MIT"].join(""),
  ["Transaction", "Envelope"].join(""),
];

describe("SDK package surface", () => {
  it("does not retain the deleted custom Durable Object modules", () => {
    for (const file of forbiddenFiles) expect(existsSync(resolve(sourceRoot, file))).toBe(false);
    for (const file of forbiddenTests) expect(existsSync(resolve(packageRoot, "test", file))).toBe(false);
  });

  it("does not publish the retired scheduler or job-worker surface", () => {
    for (const name of forbiddenExports) expect(indexTokens.has(name)).toBe(false);
    expect(packageJson.exports).not.toHaveProperty("./control");
    expect(packageJson.exports).not.toHaveProperty("./examples");
    expect(packageJson.scripts.build).not.toContain("entry.control");
    expect(packageJson.scripts.build).not.toContain("entry.examples");
    for (const text of ["defineWorker", "connectScheduler", "endpoint-run.ts", "/v0/workers"]) {
      expect(readmeSource).not.toContain(text);
    }
  });

  it("does not publish retired semantic, data, reflection, feed, or dashboard surfaces", () => {
    for (const name of forbiddenExports) expect(indexTokens.has(name)).toBe(false);
    for (const name of forbiddenSourceTokens) expect(sourceText).not.toContain(name);
    expect(packageJson.exports).not.toHaveProperty("./data");
    expect(packageJson.scripts.build).not.toContain("entry.data");
    for (const text of [
      "S3VectorsClient",
      "VerglasGraphsClient",
      "graphFromEnv",
      "createDataClient",
      "DataClient",
      "tableWrite",
      "vectorWrite",
      "client.reflect",
      "client.follow",
      "bindDashboardSources",
      "DashboardSpec",
    ]) {
      expect(readmeSource).not.toContain(text);
    }
  });

  it("does not export or mention the deleted custom protocol", () => {
    for (const name of forbiddenExports) expect(indexTokens.has(name)).toBe(false);
    for (const word of forbiddenProtocolWords) expect(sourceText).not.toContain(word);
  });

  it("requires Web Crypto UUIDs instead of time/random fallbacks", () => {
    expect(sourceText).not.toContain("Math.random");
    expect(sourceText).not.toContain("Date.now");
  });

  it("keeps the catalog and Worker management clients on the public root", async () => {
    const sdk = await import("../src/index");
    expect(sdk.connect).toBeTypeOf("function");
    expect(sdk.Table).toBeTypeOf("function");
    expect(sdk.WorkersManagementClient).toBeTypeOf("function");
  });
});
