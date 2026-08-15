import assert from "node:assert/strict";
import { readFile, readdir } from "node:fs/promises";
import test from "node:test";

const root = new URL("../", import.meta.url);

/** Returns every skill's frontmatter + body text keyed by directory name. */
async function readSkills() {
  const skillsDir = new URL("skills/", root);
  const entries = await readdir(skillsDir, { withFileTypes: true });
  const skills = {};
  for (const entry of entries) {
    if (!entry.isDirectory()) continue;
    const path = new URL(`${entry.name}/SKILL.md`, skillsDir);
    try {
      skills[entry.name] = await readFile(path, "utf8");
    } catch {
      // no SKILL.md in this directory; ignore
    }
  }
  return skills;
}

// Verbs and JSON shapes confirmed present in the frozen `verglas --help`
// snapshots. Anything outside this set is an invented CLI surface.
const forbiddenVerbs = [
  /\bverglas\s+source\b/,
  /\bverglas\s+mv\b/,
  /\bverglas\s+sink\b/,
  /--connector\b/,
  /error_filter\b/,
];

test("every skill declares YAML frontmatter with name and description", async () => {
  const skills = await readSkills();
  assert.ok(Object.keys(skills).length >= 3, "expected at least three skills");
  for (const [dir, text] of Object.entries(skills)) {
    assert.match(text, /^---\n/, `${dir}/SKILL.md must open with frontmatter`);
    const frontmatter = text.split("---\n")[1] ?? "";
    assert.match(frontmatter, /^name:\s*\S+/m, `${dir}/SKILL.md missing name:`);
    assert.match(
      frontmatter,
      /^description:/m,
      `${dir}/SKILL.md missing description:`,
    );
  }
});

test("a lakehouse skill covers table, graph, and vector verbs from the shipped CLI surface", async () => {
  const skills = await readSkills();
  const match = Object.values(skills).find(
    (text) =>
      /verglas table/.test(text) &&
      /verglas graph/.test(text) &&
      /verglas vector/.test(text),
  );
  assert.ok(match, "expected one skill documenting table+graph+vector");

  // Only verbs present in the shipped CLI.
  assert.match(match, /\btable (create|append|list|show|history|delete)\b/);
  assert.match(match, /\bgraph (create|add-node|add-edge|neighbors|k-hop|paths|index|show|list|delete)\b/);
  assert.match(match, /\bvector (create-bucket|create-index|put|query|list|get|delete)\b/);
  assert.match(match, /--json/);
  assert.doesNotMatch(match, /\bverglas (index|query|kv|db|vessel|queue)\b/);

  for (const pattern of forbiddenVerbs) {
    assert.doesNotMatch(match, pattern);
  }
});

test("an evidence/observability skill covers workers follow, SQL analysis, and graph provenance", async () => {
  const skills = await readSkills();
  const match = Object.values(skills).find(
    (text) =>
      /workers follow/.test(text) &&
      /(DuckDB|Iceberg SQL client|Iceberg client)/.test(text) &&
      /(add-node|add-edge)/.test(text),
  );
  assert.ok(match, "expected one skill documenting evidence capture via workers follow");
  assert.match(match, /provenance/i);
  assert.match(match, /confidence/i);
  assert.match(match, /query-reproducible|reproduc/i);
  // Evidence pins to the state it measured: git SHA when the project is a git
  // repo, snapshot id otherwise (#148 evolution record).
  assert.match(match, /git rev-parse\s+HEAD/i);
  assert.match(match, /not.*git|without git|snapshot id/i);

  for (const pattern of forbiddenVerbs) {
    assert.doesNotMatch(match, pattern);
  }
});

test("no plugin manifest or skill references the deleted rime:rime-worker dispatch string", async () => {
  const manifests = await Promise.all(
    [".claude-plugin/plugin.json", ".codex-plugin/plugin.json", ".cursor-plugin/plugin.json"].map(
      (p) => readFile(new URL(p, root), "utf8"),
    ),
  );
  for (const [i, manifest] of manifests.entries()) {
    const parsed = JSON.parse(manifest);
    assert.equal(parsed.name, "verglas", `manifest ${i} should be named verglas`);
  }

  const skill = await readFile(new URL("skills/rime/SKILL.md", root), "utf8");
  assert.match(skill, /verglas:rime-worker/);
  assert.doesNotMatch(skill, /[^:]rime:rime-worker/);
});

test("the RIME skill wires workers to attach evaluator evidence to their candidate's graph node", async () => {
  const skill = await readFile(new URL("skills/rime/SKILL.md", root), "utf8");
  assert.match(skill, /evidence/i);
  assert.match(skill, /graph node/i);
});

test("the RIME worker agent records evidence rather than unsupported claims", async () => {
  const agent = await readFile(new URL("agents/rime-worker.md", root), "utf8");
  assert.match(agent, /unsupported claim/i);
  assert.match(agent, /graph node/i);
});
