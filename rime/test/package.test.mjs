import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

const root = new URL("../", import.meta.url);

test("package exposes one portable skill to Pi and plugin hosts", async () => {
  const packageJson = JSON.parse(
    await readFile(new URL("package.json", root), "utf8"),
  );
  const codex = JSON.parse(
    await readFile(new URL(".codex-plugin/plugin.json", root), "utf8"),
  );
  const claude = JSON.parse(
    await readFile(new URL(".claude-plugin/plugin.json", root), "utf8"),
  );

  assert.deepEqual(packageJson.pi.skills, ["./skills"]);
  assert.deepEqual(packageJson.pi.extensions, ["./extensions"]);
  assert.match(packageJson.description, /low-cost/i);
  assert.equal(codex.name, "verglas");
  assert.equal(codex.skills, "./skills/");
  assert.match(codex.interface.longDescription, /low-cost/i);
  assert.equal(claude.name, "verglas");
});

test("package supplies executable and native worker integrations", async () => {
  const piExtension = await readFile(
    new URL("extensions/rime.ts", root),
    "utf8",
  );
  const piWorker = await readFile(
    new URL("host/pi/rime-worker.md", root),
    "utf8",
  );
  const codexWorker = await readFile(
    new URL("host/codex/rime_worker.toml", root),
    "utf8",
  );
  const claudeWorker = await readFile(
    new URL("agents/rime-worker.md", root),
    "utf8",
  );
  const cursorWorker = await readFile(
    new URL("host/cursor/rime-worker.md", root),
    "utf8",
  );
  const cursorPlugin = JSON.parse(
    await readFile(new URL(".cursor-plugin/plugin.json", root), "utf8"),
  );

  assert.match(piExtension, /registerTool/);
  assert.match(piExtension, /registerCommand\("rime-status"/);
  assert.match(piExtension, /rime_parallel/);
  assert.match(piExtension, /Promise\.all/);
  assert.match(piWorker, /low-cost/i);
  assert.match(codexWorker, /name = "rime_worker"/);
  assert.match(codexWorker, /model = "gpt-5\.6-luna"/);
  assert.match(codexWorker, /low-cost/i);
  assert.match(claudeWorker, /model: sonnet/);
  assert.match(claudeWorker, /low-cost/i);
  assert.match(claudeWorker, /isolation: worktree/);
  assert.equal(cursorPlugin.name, "verglas");
  assert.match(cursorWorker, /name: rime-worker/);
  assert.match(cursorWorker, /model: composer-2\.5-fast/);
  assert.match(cursorWorker, /low-cost/i);
  assert.match(cursorWorker, /isolated worktree|assigned workspace/i);
});

test("skill prefers evaluated exploration without forcing it", async () => {
  const skill = await readFile(new URL("skills/rime/SKILL.md", root), "utf8");

  assert.match(skill, /Prefer RIME/i);
  assert.match(skill, /work directly/i);
  assert.match(skill, /reproduc/i);
  assert.match(skill, /parallel/i);
  assert.match(skill, /do not force/i);
  assert.match(skill, /dead code/i);
  assert.match(skill, /fallback/i);
  assert.match(skill, /architectural/i);
  assert.match(skill, /parser|parsing/i);
  assert.match(skill, /type/i);
  assert.match(skill, /open one wave/i);
  assert.match(skill, /graph-state\.md/i);
  assert.match(skill, /incompatible raw units/i);
  assert.match(skill, /engineering-objective\.md/i);
  assert.match(skill, /remove workspaces/i);
  assert.match(skill, /workspace-lifecycle\.md/i);
  assert.match(skill, /verglas skill install rime/i);
  assert.match(skill, /verglas --json/i);
  assert.match(skill, /low-cost/i);
  assert.match(skill, /open-source|local/i);
  assert.doesNotMatch(skill, /host has no RIME integration/i);
  assert.match(skill, /spawn_agent/);
  assert.match(skill, /rime_parallel/);
  assert.match(skill, /rime-worker/);
  assert.match(skill, /verglas:rime-worker/);
  assert.match(skill, /\*\*Cursor:\*\*/);
  assert.match(skill, /best-of-n-runner|rime-worker/);
  assert.match(skill, /rime_<project>|project graph/i);
  assert.match(skill, /sessionStart/);
  assert.match(skill, /beforeSubmitPrompt/);
  assert.match(skill, /gpt-5\.6-luna/);
  assert.match(skill, /composer-2\.5-fast/);
  assert.match(skill, /fork_turns.*none/);
  assert.match(skill, /do not pass a model override/i);
  assert.match(skill, /lineage/i);
  assert.match(skill, /bandit/i);
  assert.match(skill, /operator-specific/i);
  assert.match(skill, /held-out/i);
  assert.match(skill, /fixed.*cost/i);
});
