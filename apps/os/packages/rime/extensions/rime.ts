/** Pi fleet adapter for one parallel RIME policy wave. */

import { spawn } from "node:child_process";
import { existsSync } from "node:fs";
import { basename } from "node:path";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";

type Task = { prompt: string; workspace: string };
type Attempt = {
  workspace: string;
  exitCode: number;
  output: string;
  stderr: string;
  model?: string;
};

const WORKER_CONTRACT = [
  "Implement exactly one RIME candidate inside this assigned workspace.",
  "Do not change the frozen evaluator or edit outside the workspace.",
  "Make one coherent architectural change; leave no dead code, compatibility path, unrequested fallback, or guessed parsing or typing.",
  "Run the supplied evaluator and return a concise summary, its result, and your actual model identity.",
].join(" ");

/** Resolves the running Pi executable without selecting a model provider. */
function piInvocation(args: string[]): { command: string; args: string[] } {
  const script = process.argv[1];
  if (script && !script.startsWith("/$bunfs/root/") && existsSync(script)) {
    return { command: process.execPath, args: [script, ...args] };
  }
  if (!/^(node|bun)(\.exe)?$/.test(basename(process.execPath).toLowerCase())) {
    return { command: process.execPath, args };
  }
  return { command: "pi", args };
}

/** Extracts final assistant output and the observed model from Pi JSON events. */
function consumeEvents(stdout: string): { output: string; model?: string } {
  let output = "";
  let model: string | undefined;
  for (const line of stdout.split("\n")) {
    if (!line.trim()) continue;
    let event: Record<string, any>;
    try {
      event = JSON.parse(line);
    } catch {
      continue;
    }
    if (event.type !== "message_end" || event.message?.role !== "assistant") continue;
    model = event.message.model ?? model;
    for (const part of event.message.content ?? []) {
      if (part.type === "text") output = part.text;
    }
  }
  return { output, model };
}

/** Runs one candidate in the workspace allocated by the RIME coordinator. */
async function runAttempt(
  task: Task,
  model: string | undefined,
  signal: AbortSignal | undefined,
): Promise<Attempt> {
  const args = ["--mode", "json", "--no-session", "-p"];
  if (model) args.push("--model", model);
  args.push(`${WORKER_CONTRACT}\n\n${task.prompt}`);
  const invocation = piInvocation(args);
  return new Promise((resolve, reject) => {
    const child = spawn(invocation.command, invocation.args, {
      cwd: task.workspace,
      shell: false,
      stdio: ["ignore", "pipe", "pipe"],
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
    child.on("close", (code) => {
      const observed = consumeEvents(stdout);
      resolve({
        workspace: task.workspace,
        exitCode: code ?? 1,
        output: observed.output,
        stderr,
        ...(observed.model ? { model: observed.model } : {}),
      });
    });
    const abort = () => child.kill("SIGTERM");
    if (signal?.aborted) abort();
    else signal?.addEventListener("abort", abort, { once: true });
  });
}

/** Registers Pi's executable RIME wave tool. */
export default function rimeExtension(pi: ExtensionAPI): void {
  pi.registerCommand("rime-status", {
    description: "Confirm that the RIME extension and 100-worker wave boundary are loaded",
    handler: async (_args, ctx) => {
      ctx.ui.notify("RIME is loaded; waves support up to 100 workers.", "info");
    },
  });
  pi.registerTool({
    name: "rime_parallel",
    label: "RIME parallel wave",
    description: "Run one RIME policy wave in independently allocated workspaces. A model override is optional so a missing low-cost model never blocks execution.",
    promptSnippet: "Run isolated candidate attempts for one RIME policy wave",
    promptGuidelines: [
      "Use rime_parallel only after freezing an evaluator and allocating one isolated workspace per candidate.",
      "Evaluate every returned workspace independently and close the complete wave before selecting again.",
    ],
    parameters: Type.Object({
      tasks: Type.Array(
        Type.Object({ prompt: Type.String(), workspace: Type.String() }),
        { minItems: 1, maxItems: 100 },
      ),
      model: Type.Optional(Type.String({ description: "Available lower-cost Pi model to prefer; omit when unavailable." })),
    }),
    async execute(_id, params, signal) {
      const attempts = await Promise.all(
        params.tasks.map((task) => runAttempt(task, params.model, signal)),
      );
      return {
        content: [{ type: "text", text: JSON.stringify({ attempts }) }],
        details: { attempts },
        ...(attempts.every((attempt) => attempt.exitCode !== 0) ? { isError: true } : {}),
      };
    },
  });
}
