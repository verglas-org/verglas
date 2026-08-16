import assert from "node:assert/strict";
import { mkdtemp, readFile, writeFile, chmod, mkdir } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawn } from "node:child_process";
import test from "node:test";
import { fileURLToPath } from "node:url";

const root = new URL("../", import.meta.url);
const hooksDir = fileURLToPath(new URL("host/cursor/hooks/", root));

/** Runs a hook script with JSON stdin and a fake `verglas` on PATH. */
function runHook(script, input, env = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn("bash", [join(hooksDir, script)], {
      env: { ...process.env, ...env },
      stdio: ["pipe", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";
    child.stdout.on("data", (chunk) => {
      stdout += chunk.toString();
    });
    child.stderr.on("data", (chunk) => {
      stderr += chunk.toString();
    });
    child.on("error", reject);
    child.on("close", (code) => resolve({ code, stdout, stderr }));
    child.stdin.end(JSON.stringify(input));
  });
}

/** Writes a fake `verglas` that records argv and prints a graph show payload. */
async function fakeVerglas(behavior) {
  const dir = await mkdtemp(join(tmpdir(), "rime-verglas-"));
  const bin = join(dir, "verglas");
  const failFast = behavior === "fail" ? "exit 1" : "";
  await writeFile(
    bin,
    `#!/bin/bash
set -euo pipefail
log="${dir}/calls.log"
printf '%s\\n' "$*" >> "$log"
${failFast}
if [[ "\${1:-}" == "--json" && "\${2:-}" == "graph" && "\${3:-}" == "create" ]]; then
  echo '{"namespace":"'"\${4}"'","nodesTable":"'"\${4}"'.nodes","edgesTable":"'"\${4}"'.edges"}'
  exit 0
fi
if [[ "\${1:-}" == "--json" && "\${2:-}" == "graph" && "\${3:-}" == "show" ]]; then
  echo '{"namespace":"'"\${4}"'","nodesTable":"'"\${4}"'.nodes","edgesTable":"'"\${4}"'.edges","nodeCount":2,"edgeCount":1,"indexed":false,"snapshotId":7}'
  exit 0
fi
exit 0
`,
    { mode: 0o755 },
  );
  await chmod(bin, 0o755);
  return { dir, bin, log: join(dir, "calls.log") };
}

test("Cursor sessionStart hook creates the project graph and injects it", async () => {
  const fake = await fakeVerglas("ok");
  const { code, stdout } = await runHook(
    "rime-session-start.sh",
    { cwd: "/Users/jfbrown/code/verglas" },
    { PATH: `${fake.dir}:${process.env.PATH}` },
  );
  assert.equal(code, 0);
  const payload = JSON.parse(stdout);
  assert.match(payload.additional_context, /rime_verglas/);
  const calls = await readFile(fake.log, "utf8");
  assert.match(calls, /graph create rime_verglas/);
  assert.match(calls, /graph show rime_verglas/);
});

test("Cursor beforeSubmitPrompt hook injects the same project graph", async () => {
  const fake = await fakeVerglas("ok");
  const { code, stdout } = await runHook(
    "rime-prompt.sh",
    { workspace_roots: ["/Users/jfbrown/code/rlean"], prompt: "fix the evaluator" },
    { PATH: `${fake.dir}:${process.env.PATH}` },
  );
  assert.equal(code, 0);
  const payload = JSON.parse(stdout);
  assert.match(payload.additional_context, /rime_rlean/);
  assert.doesNotMatch(payload.additional_context, /"namespace":"agent_memory"/);
});

test("Cursor hooks fail open when Verglas is unavailable", async () => {
  const fake = await fakeVerglas("fail");
  const { code, stdout } = await runHook(
    "rime-session-start.sh",
    { cwd: "/tmp/app" },
    { PATH: `${fake.dir}:${process.env.PATH}` },
  );
  assert.equal(code, 0);
  assert.equal(stdout.trim(), "");
});

test("Cursor worker-loop hook reports a finished rime-worker without writing the graph", async () => {
  const fake = await fakeVerglas("ok");
  const { code, stdout } = await runHook(
    "rime-subagent-stop.sh",
    {
      subagent_type: "rime-worker",
      status: "completed",
      cwd: "/Users/jfbrown/code/verglas",
      workspace: "/tmp/rime-attempt-1",
    },
    { PATH: `${fake.dir}:${process.env.PATH}` },
  );
  assert.equal(code, 0);
  const payload = JSON.parse(stdout);
  assert.match(payload.followup_message, /rime-worker/);
  assert.match(payload.followup_message, /rime_verglas/);
  const calls = await readFile(fake.log, "utf8").catch(() => "");
  assert.doesNotMatch(calls, /add-node|add-edge|insert/);
});

test("Cursor install fragment wires both loops", async () => {
  const fragment = JSON.parse(
    await readFile(new URL("host/cursor/hooks.json", root), "utf8"),
  );
  assert.equal(fragment.version, 1);
  assert.ok(fragment.hooks.sessionStart.some((hook) => hook.command.includes("rime-session-start")));
  assert.ok(
    fragment.hooks.beforeSubmitPrompt.some((hook) => hook.command.includes("rime-prompt")),
  );
  assert.ok(
    fragment.hooks.subagentStop.some((hook) => hook.command.includes("rime-subagent-stop")),
  );
});
