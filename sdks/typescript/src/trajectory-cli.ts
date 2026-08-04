// The capture source's bun-side normalizer. The Rust capture path shells out to
// this: it reads a harness-native transcript (from --transcript <path> or
// stdin), normalizes it to trajectory-v1 records, and prints them as JSONL on
// stdout. Diagnostics and any normalization error are printed as one JSON object
// on stderr. It never throws to the process: a structurally invalid transcript
// exits with a nonzero code and a structured stderr line, so the Rust caller can
// fail open (enqueue nothing) rather than crash the host session.
//
// Usage:
//   bun run src/trajectory-cli.ts --source <harness> [--transcript <path>]
//   (transcript on stdin when --transcript is absent)

import { readFileSync } from "node:fs";

import { HARNESS_SOURCES, safeNormalize } from "./trajectory";

/** Parses `--key value` and `--key=value` pairs into a flat map. */
function parseArgs(argv: string[]): Record<string, string> {
  const out: Record<string, string> = {};
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (!arg.startsWith("--")) continue;
    const eq = arg.indexOf("=");
    if (eq !== -1) {
      out[arg.slice(2, eq)] = arg.slice(eq + 1);
    } else {
      out[arg.slice(2)] = argv[i + 1] ?? "";
      i += 1;
    }
  }
  return out;
}

function fail(message: string): never {
  process.stderr.write(`${JSON.stringify({ error: message })}\n`);
  process.exit(2);
}

function main(): void {
  const args = parseArgs(process.argv.slice(2));
  const source = args.source;
  if (!source) {
    fail(`--source is required (one of ${HARNESS_SOURCES.join(", ")})`);
  }
  const transcript = args.transcript
    ? readFileSync(args.transcript, "utf8")
    : readFileSync(0, "utf8");

  const { records, diagnostics, error } = safeNormalize(source, transcript);
  if (error) {
    fail(error);
  }
  // Records first, one per line; then diagnostics as the last stderr line so the
  // caller can log recoverable cleanup without parsing stdout.
  const stdout = records.map((r) => JSON.stringify(r)).join("\n");
  if (stdout) process.stdout.write(`${stdout}\n`);
  process.stderr.write(`${JSON.stringify({ diagnostics })}\n`);
}

main();
