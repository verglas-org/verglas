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

const forbiddenFiles = [
  ["arrow", "ipc.ts"].join("-"),
  ["do", "protocol.ts"].join("-"),
  "durable-objects.ts",
];
const forbiddenExports = ["DurableObject", "StorageBridge", "createWorkerRuntime"];
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
  });

  it("does not export or mention the deleted custom protocol", () => {
    for (const name of forbiddenExports) expect(indexTokens.has(name)).toBe(false);
    for (const word of forbiddenProtocolWords) expect(sourceText).not.toContain(word);
  });

  it("keeps the catalog and semantic clients on the public root", async () => {
    const sdk = await import("../src/index");
    expect(sdk.connect).toBeTypeOf("function");
    expect(sdk.S3VectorsClient).toBeTypeOf("function");
    expect(sdk.VerglasGraphsClient).toBeTypeOf("function");
  });
});
