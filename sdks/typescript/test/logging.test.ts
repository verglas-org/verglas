import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { connect, type VerglasClient } from "../src/index";
import {
  errorMessage,
  inferPlacement,
  isLogsTable,
  logsTableName,
  newRunId,
  RunLogger,
} from "../src/logging";
import { startMockEndpoint, type MockEndpoint } from "./mock-endpoint";

let endpoint: MockEndpoint;
let client: VerglasClient;

beforeEach(async () => {
  endpoint = await startMockEndpoint();
  client = connect({ endpoint: endpoint.url, token: endpoint.token });
});
afterEach(() => endpoint.close());

const NOW = 1_770_000_000_000;

describe("logging primitives", () => {
  it("names and identifies log tables", () => {
    expect(logsTableName("app.points")).toBe("app.points_LOGS");
    expect(isLogsTable("app.points_LOGS")).toBe(true);
    expect(isLogsTable("app.points")).toBe(false);
  });

  it("generates run IDs and classifies endpoint placement", () => {
    expect(newRunId()).toMatch(/^[0-9a-f-]{36}$/);
    expect(inferPlacement("http://localhost")).toBe("local");
    expect(inferPlacement("https://api.example.test")).toBe("remote");
  });

  it("converts thrown values into safe messages", () => {
    expect(errorMessage(new Error("failed"))).toBe("failed");
    expect(errorMessage("failed")).toBe("failed");
    expect(errorMessage({ code: 7 })).toBe('{"code":7}');
  });

  it("buffers standard rows and flushes them through the catalog client", async () => {
    const logger = new RunLogger({
      pipeline: "app.points",
      kind: "worker",
      placement: "local",
      runId: "run-fixed",
      now: () => NOW,
    });
    logger.log("step", { rows: 2, message: "loaded" });
    expect(logger.pending).toBe(1);

    await logger.flush(client);

    expect(logger.pending).toBe(0);
    const rows = endpoint.tableState("app.points_LOGS").rows.map((entry) => entry.row);
    expect(rows).toEqual([
      expect.objectContaining({
        pipeline: "app.points",
        run_id: "run-fixed",
        event: "step",
        rows: 2,
        message: "loaded",
      }),
    ]);
  });

  it("keeps log writes best-effort when the catalog append fails", async () => {
    const warnings: unknown[] = [];
    const logger = new RunLogger({
      pipeline: "app.points",
      kind: "worker",
      placement: "local",
      warn: (...args) => warnings.push(args),
      now: () => NOW,
    });
    logger.log("step");
    const broken = connect({
      endpoint: endpoint.url,
      token: endpoint.token,
      fetch: async () => {
        throw new Error("catalog unavailable");
      },
    });

    await expect(logger.flush(broken)).resolves.toBeUndefined();
    expect(warnings).toHaveLength(1);
  });
});
