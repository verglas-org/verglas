import assert from "node:assert/strict";
import { readFile, readdir } from "node:fs/promises";
import path from "node:path";
import test from "node:test";

const root = new URL("../", import.meta.url);

// The real verglas CLI 0.1.1 top-level commands. This is the repo-internal
// source of truth for this test; it is not read from any installed CLI or
// external file. Update it deliberately when the CLI's command surface
// changes, not to silence a failing assertion.
const KNOWN_COMMANDS = new Set([
  "drain",
  "status",
  "table",
  "dashboard",
  "workers",
  "lakehouse",
  "secret",
  "token",
  "graph",
  "vector",
  "help",
]);

// `skill`/`skills` document a forward-looking distribution contract (e.g.
// `verglas skill install rime`) that is not part of the CLI 0.1.1 command
// surface yet. Exempt them rather than flagging every mention.
const EXEMPT_COMMANDS = new Set(["skill", "skills"]);

/** Recursively collects absolute paths of every `.md` file under `dir`. */
async function findMarkdownFiles(dir) {
  let entries;
  try {
    entries = await readdir(dir, { withFileTypes: true });
  } catch {
    return [];
  }
  const files = [];
  for (const entry of entries) {
    const entryPath = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      files.push(...(await findMarkdownFiles(entryPath)));
    } else if (entry.isFile() && entry.name.endsWith(".md")) {
      files.push(entryPath);
    }
  }
  return files;
}

// Matches fenced code blocks (```...```) or single-backtick inline code
// spans (`...`). `verglas <command>` references outside of code formatting
// are prose, not CLI invocations, and are intentionally not scanned.
const CODE_SPAN_PATTERN = /```[\s\S]*?```|`[^`\n]+`/g;

/**
 * Extracts every `verglas <command>` reference from a code span's text.
 * Flag tokens (leading `-`) are skipped so `verglas --json graph` resolves
 * to command `graph`. Returns the list of top-level command tokens found.
 */
function extractCommands(spanText) {
  const commands = [];
  for (const rawLine of spanText.split("\n")) {
    const tokens = rawLine.trim().split(/\s+/).filter(Boolean);
    for (let i = 0; i < tokens.length; i += 1) {
      if (tokens[i] !== "verglas") continue;
      let j = i + 1;
      while (j < tokens.length && tokens[j].startsWith("-")) j += 1;
      if (j < tokens.length && !tokens[j].startsWith("<")) {
        // A leading `<` marks a placeholder (e.g. `verglas <group> <verb>
        // --help`) documenting the CLI's shape generically, not a literal
        // command reference; nothing concrete to check there.
        const command = tokens[j].replace(/[^a-zA-Z0-9_-]+$/, "");
        if (command) commands.push(command);
      }
    }
  }
  return commands;
}

/** Finds every `verglas <command>` reference inside a markdown file's code spans. */
function extractCommandsFromMarkdown(text) {
  const commands = [];
  const spans = text.match(CODE_SPAN_PATTERN) ?? [];
  for (const span of spans) {
    const inner = span.startsWith("```")
      ? span.replace(/^```[^\n]*\n?/, "").replace(/```$/, "")
      : span.slice(1, -1);
    commands.push(...extractCommands(inner));
  }
  return commands;
}

test("every `verglas <command>` reference under skills/ and agents/ names a real CLI 0.1.1 command", async () => {
  const skillsDir = new URL("skills/", root);
  const agentsDir = new URL("agents/", root);
  const files = [
    ...(await findMarkdownFiles(skillsDir.pathname)),
    ...(await findMarkdownFiles(agentsDir.pathname)),
  ];
  assert.ok(
    files.length >= 4,
    "expected to scan at least four markdown files across skills/ and agents/",
  );

  const offenses = [];
  for (const file of files) {
    const text = await readFile(file, "utf8");
    for (const command of extractCommandsFromMarkdown(text)) {
      if (KNOWN_COMMANDS.has(command) || EXEMPT_COMMANDS.has(command)) {
        continue;
      }
      offenses.push(`${file}: unknown verglas command "${command}"`);
    }
  }

  assert.deepEqual(
    offenses,
    [],
    `found unknown verglas command references:\n${offenses.join("\n")}`,
  );
});
