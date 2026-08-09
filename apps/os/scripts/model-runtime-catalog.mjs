import { execFile, spawn } from "node:child_process";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

const CLAUDE_MODELS = [
  {
    id: "opus",
    name: "Claude Opus",
    description: "Claude Code's current highest-capability model alias.",
    contextWindow: 1_000_000,
  },
  {
    id: "sonnet",
    name: "Claude Sonnet",
    description: "Claude Code's current balanced model alias.",
    isDefault: true,
    contextWindow: 1_000_000,
  },
  {
    id: "haiku",
    name: "Claude Haiku",
    description: "Claude Code's current Haiku model.",
    contextWindow: 200_000,
  },
];

export function parseCursorModels(output) {
  return output.split(/\r?\n/u).flatMap(line => {
    const match = line.match(/^([^\s]+)\s+-\s+(.+)$/u);
    if (!match) return [];
    const [, id, rawName] = match;
    const isDefault = /\s+\(default\)$/u.test(rawName);
    const name = rawName.replace(/\s+\(default\)$/u, "");
    return [{ id, name, ...(isDefault ? { isDefault: true } : {}) }];
  });
}

async function listCursorModels(providerApiKey) {
  const { stdout, stderr } = await execFileAsync("cursor-agent", ["models"], {
    timeout: 30_000,
    maxBuffer: 2 * 1024 * 1024,
    env: providerApiKey ? { ...process.env, CURSOR_API_KEY: providerApiKey } : process.env,
  });
  const models = parseCursorModels(`${stdout}${stderr}`);
  if (models.length === 0) throw new Error("Cursor returned an empty model catalog.");
  return models;
}

async function listCodexModels() {
  return await new Promise((resolve, reject) => {
    const child = spawn("codex", ["app-server", "--stdio"], {
      stdio: ["pipe", "pipe", "pipe"],
    });
    let settled = false;
    let stdout = "";
    let stderr = "";
    const finish = (error, models) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      child.kill();
      if (error) reject(error);
      else resolve(models);
    };
    const timer = setTimeout(
      () => finish(new Error("Codex model discovery timed out.")),
      30_000,
    );
    child.on("error", error => finish(error));
    child.on("exit", code => {
      if (!settled) finish(new Error(
        stderr.trim() || `Codex model discovery exited with code ${code}.`,
      ));
    });
    child.stderr.on("data", chunk => {
      stderr = `${stderr}${chunk}`.slice(-16_384);
    });
    child.stdout.on("data", chunk => {
      stdout += chunk;
      const lines = stdout.split("\n");
      stdout = lines.pop() || "";
      for (const line of lines) {
        if (!line.trim()) continue;
        let message;
        try { message = JSON.parse(line); } catch { continue; }
        if (message.id === 1 && message.result) {
          child.stdin.write(`${JSON.stringify({ method: "initialized" })}\n`);
          child.stdin.write(`${JSON.stringify({
            id: 2,
            method: "model/list",
            params: { limit: 100, cursor: null, includeHidden: false },
          })}\n`);
        } else if (message.id === 2) {
          if (message.error) {
            finish(new Error(message.error.message || "Codex model discovery failed."));
            return;
          }
          const models = Array.isArray(message.result?.data)
            ? message.result.data.flatMap(entry => {
              const id = typeof entry.model === "string" ? entry.model.trim() : "";
              if (!id || entry.hidden) return [];
              return [{
                id,
                name: typeof entry.displayName === "string" && entry.displayName.trim()
                  ? entry.displayName.trim()
                  : id,
                ...(typeof entry.description === "string" && entry.description.trim()
                  ? { description: entry.description.trim() }
                  : {}),
                ...(entry.isDefault === true ? { isDefault: true } : {}),
              }];
            })
            : [];
          if (models.length === 0) {
            finish(new Error("Codex returned an empty model catalog."));
          } else {
            finish(undefined, models);
          }
          return;
        }
      }
    });
    child.stdin.write(`${JSON.stringify({
      id: 1,
      method: "initialize",
      params: {
        clientInfo: { name: "verglas", title: "Verglas", version: "0.1.0" },
        capabilities: { experimentalApi: true },
      },
    })}\n`);
  });
}

export async function discoverRuntimeModels(runtimeId, providerApiKey) {
  if (runtimeId === "codex") return await listCodexModels();
  if (runtimeId === "claude-code") return CLAUDE_MODELS;
  if (runtimeId === "cursor") return await listCursorModels(providerApiKey);
  throw new Error(`Unknown local model runtime: ${runtimeId}`);
}
