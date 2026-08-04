// Tests for the canonical session-input normalizer (WHITEPAPER §7.6): every
// supported harness transcript becomes one validated trajectory-v1 record
// stream. Real-format Claude Code and Codex fixtures; synthetic malformed input.

import { describe, it, expect } from "vitest";
import { readFileSync } from "node:fs";
import { execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

import {
  normalizeTranscript,
  safeNormalize,
  isHarnessSource,
  HARNESS_SOURCES,
  validateTranscript,
  type NormalizedRecord,
} from "../src/trajectory";

const here = dirname(fileURLToPath(import.meta.url));
const fixture = (name: string) => readFileSync(join(here, "fixtures", name), "utf8");

describe("normalizeTranscript", () => {
  it("normalizes a real Claude Code transcript into trajectory-v1 records", () => {
    const { records, diagnostics } = normalizeTranscript("claude-code", fixture("claude-code.jsonl"));
    expect(diagnostics).toEqual([]);
    // Leading meta record names the source.
    expect(records[0]).toMatchObject({ role: "meta", source: "claude-code" });
    const roles = records.map((r) => r.role);
    expect(roles).toContain("user");
    expect(roles).toContain("assistant");
    expect(roles).toContain("tool");
    // A tool call carries a stable id + stringified args, linked to its result.
    const call = records.find(
      (r): r is Extract<NormalizedRecord, { role: "assistant"; content: null }> =>
        r.role === "assistant" && r.content === null,
    );
    expect(call?.tool_calls?.[0]).toMatchObject({ id: "tool_1", name: "Bash" });
    expect(call?.tool_calls?.[0].args).toContain("duckdb");
    const result = records.find((r) => r.role === "tool");
    expect(result).toMatchObject({ tool_call_id: "tool_1", content: "GOOG" });
    // Thinking blocks surface as reasoning records (the richer structure §7.6
    // gives the extractor), not as assistant prose.
    const reasoning = records.find((r) => r.role === "reasoning");
    expect(reasoning).toMatchObject({ content: "I will inspect the file first." });
    // The normalizer is verbatim: secret scrubbing is a downstream (enqueue) property.
    expect(records.some((r) => "content" in r && typeof r.content === "string" && r.content.includes("wJalrXUtnFEMI"))).toBe(true);
  });

  it("normalizes a real Codex transcript, exposing reasoning and meta context", () => {
    const { records, diagnostics } = normalizeTranscript("codex", fixture("codex.jsonl"));
    expect(diagnostics).toEqual([]);
    expect(records[0]).toMatchObject({
      role: "meta",
      source: "codex",
      cwd: "/repo",
      git_branch: "main",
      model: "gpt-5-codex",
    });
    expect(records.some((r) => r.role === "reasoning")).toBe(true);
  });

  it("produces output that validates against the trajectory-v1 schema", () => {
    for (const src of ["claude-code", "codex"] as const) {
      const { records } = normalizeTranscript(src, fixture(`${src}.jsonl`));
      expect(() => validateTranscript(records)).not.toThrow();
    }
  });

  it("reports malformed lines as diagnostics rather than crashing", () => {
    const good = fixture("claude-code.jsonl").trim();
    const dirty = ["not json at all", good, "{\"partial\":"].join("\n");
    const { records, diagnostics } = normalizeTranscript("claude-code", dirty);
    expect(records.length).toBeGreaterThan(0);
    expect(diagnostics.length).toBeGreaterThan(0);
    expect(diagnostics.some((d) => d.code === "invalid_json_line")).toBe(true);
  });

  it("safeNormalize never throws, even on structurally invalid input", () => {
    // A transcript with no user/assistant turn makes the library throw a
    // NormalizationError; safeNormalize turns it into a reported error instead.
    const r = safeNormalize("claude-code", "{\"type\":\"summary\",\"summary\":\"x\"}");
    expect(r.records).toEqual([]);
    expect(r.error).toBeTruthy();
  });

  it("knows the transcript-based harness sources it supports", () => {
    expect(isHarnessSource("claude-code")).toBe(true);
    expect(isHarnessSource("codex")).toBe(true);
    expect(isHarnessSource("nonsense")).toBe(false);
    expect(HARNESS_SOURCES).toContain("openhands");
  });
});

describe("trajectory-cli", () => {
  const cli = join(here, "..", "src", "trajectory-cli.ts");
  const run = (args: string[], input: string) =>
    execFileSync("bun", ["run", cli, ...args], { input, encoding: "utf8" });

  it("emits trajectory-v1 records as JSONL on stdout", () => {
    const out = run(["--source", "claude-code"], fixture("claude-code.jsonl"));
    const lines = out.trim().split("\n").filter(Boolean);
    const parsed = lines.map((l) => JSON.parse(l) as NormalizedRecord);
    expect(parsed[0]).toMatchObject({ role: "meta", source: "claude-code" });
    expect(() => validateTranscript(parsed)).not.toThrow();
  });

  it("exits nonzero with a structured error on unnormalizable input, not a stack trace", () => {
    let failed = false;
    try {
      execFileSync("bun", ["run", cli, "--source", "claude-code"], {
        input: "{\"type\":\"summary\"}",
        encoding: "utf8",
        stdio: ["pipe", "pipe", "pipe"],
      });
    } catch (e: unknown) {
      failed = true;
      const err = e as { status: number; stderr: string };
      expect(err.status).not.toBe(0);
      const diag = JSON.parse(err.stderr.trim().split("\n").pop() as string);
      expect(diag.error).toBeTruthy();
    }
    expect(failed).toBe(true);
  });
});
