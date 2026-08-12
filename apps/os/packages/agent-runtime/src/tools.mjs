import { spawn } from "node:child_process";
import { mkdir, readFile, stat, writeFile } from "node:fs/promises";
import { dirname, relative, resolve, sep } from "node:path";

const MAX_FILE_BYTES = 1024 * 1024;
const MAX_OUTPUT_BYTES = 128 * 1024;
const MAX_EXECUTION_MS = 120_000;
const MAX_FETCH_MS = 30_000;

function schema(properties, required = Object.keys(properties)) {
  return { type: "object", properties, required, additionalProperties: false };
}

export const toolDefinitions = [
  {
    type: "function",
    function: {
      name: "describeEnvironment",
      description:
        "Describe the isolated workspace, installed Verglas SDK, endpoint variables, and available runtime capabilities without revealing credentials.",
      parameters: schema({}, []),
    },
  },
  {
    type: "function",
    function: {
      name: "execute",
      description:
        "Execute a program directly inside the isolated workspace. No shell is implied; invoke sh explicitly when shell syntax is required.",
      parameters: schema(
        {
          command: { type: "string" },
          args: { type: "array", items: { type: "string" } },
          cwd: { type: "string" },
          timeoutMs: { type: "integer", minimum: 1, maximum: MAX_EXECUTION_MS },
        },
        ["command"],
      ),
    },
  },
  {
    type: "function",
    function: {
      name: "readFile",
      description: "Read a UTF-8 file from the isolated workspace.",
      parameters: schema({ path: { type: "string" } }),
    },
  },
  {
    type: "function",
    function: {
      name: "writeFile",
      description:
        "Create or replace a UTF-8 file inside the isolated workspace, creating parent directories as needed.",
      parameters: schema({
        path: { type: "string" },
        content: { type: "string" },
      }),
    },
  },
  {
    type: "function",
    function: {
      name: "editFile",
      description:
        "Replace one exact, unique text occurrence in a UTF-8 workspace file.",
      parameters: schema({
        path: { type: "string" },
        oldText: { type: "string" },
        newText: { type: "string" },
      }),
    },
  },
  {
    type: "function",
    function: {
      name: "webFetch",
      description:
        "Fetch an HTTPS resource. Responses are bounded and returned as text.",
      parameters: schema(
        {
          url: { type: "string" },
          method: {
            type: "string",
            enum: ["GET", "HEAD", "POST", "PUT", "PATCH", "DELETE"],
          },
          headers: { type: "object", additionalProperties: { type: "string" } },
          body: { type: "string" },
          timeoutMs: { type: "integer", minimum: 1, maximum: MAX_FETCH_MS },
        },
        ["url"],
      ),
    },
  },
  {
    type: "function",
    function: {
      name: "requestPermission",
      description:
        "Ask the user to delegate missing Verglas access to this agent. End the turn after calling this tool.",
      parameters: schema({
        resourceId: { type: "string" },
        actions: { type: "array", items: { type: "string" } },
        reason: { type: "string" },
      }),
    },
  },
];

function workspacePath(workspace, requestedPath) {
  if (typeof requestedPath !== "string" || !requestedPath.trim()) {
    throw new Error("A workspace path is required.");
  }
  const absolute = resolve(workspace, requestedPath);
  if (absolute !== workspace && !absolute.startsWith(`${workspace}${sep}`)) {
    throw new Error("Paths must remain inside the agent workspace.");
  }
  return absolute;
}

function boundedText(value, limit = MAX_OUTPUT_BYTES) {
  const bytes = Buffer.from(value);
  if (bytes.length <= limit) return { text: value, truncated: false };
  return { text: bytes.subarray(0, limit).toString("utf8"), truncated: true };
}

function redact(value, secrets) {
  let result = value;
  for (const secret of secrets) {
    if (secret) result = result.split(secret).join("[REDACTED]");
  }
  return result;
}

async function executeProgram(
  { command, args = [], timeoutMs },
  env,
  cwd,
  secrets,
) {
  if (typeof command !== "string" || !command.trim()) {
    throw new Error("execute requires a command.");
  }
  if (
    !Array.isArray(args) ||
    args.some((argument) => typeof argument !== "string")
  ) {
    throw new Error("execute args must be strings.");
  }
  const duration = Math.min(
    Math.max(Number(timeoutMs) || 30_000, 1),
    MAX_EXECUTION_MS,
  );
  return await new Promise((resolveResult, reject) => {
    const child = spawn(command, args, {
      cwd,
      env,
      shell: false,
      stdio: ["ignore", "pipe", "pipe"],
    });
    let stdout = Buffer.alloc(0);
    let stderr = Buffer.alloc(0);
    let truncated = false;
    const append = (current, chunk) => {
      if (current.length >= MAX_OUTPUT_BYTES) {
        truncated = true;
        return current;
      }
      const remaining = MAX_OUTPUT_BYTES - current.length;
      if (chunk.length > remaining) truncated = true;
      return Buffer.concat([current, chunk.subarray(0, remaining)]);
    };
    child.stdout.on("data", (chunk) => {
      stdout = append(stdout, chunk);
    });
    child.stderr.on("data", (chunk) => {
      stderr = append(stderr, chunk);
    });
    let timedOut = false;
    const timer = setTimeout(() => {
      timedOut = true;
      child.kill("SIGKILL");
    }, duration);
    child.once("error", (error) => {
      clearTimeout(timer);
      reject(error);
    });
    child.once("close", (exitCode, signal) => {
      clearTimeout(timer);
      resolveResult({
        exitCode,
        signal,
        stdout: redact(stdout.toString("utf8"), secrets),
        stderr: redact(stderr.toString("utf8"), secrets),
        timedOut,
        truncated,
      });
    });
  });
}

function validatePermissionRequest(args) {
  const allowedActions = new Set([
    "discover",
    "describe",
    "query",
    "append",
    "modify",
    "create_child",
    "execute",
    "use_secret",
    "deploy",
    "pass_grants",
    "manage_grants",
    "own",
  ]);
  const resourceId = String(args.resourceId ?? "");
  const actions = [...new Set(Array.isArray(args.actions) ? args.actions : [])];
  const hasControlCharacter = [...resourceId].some((character) => {
    const point = character.codePointAt(0);
    return point <= 0x1f || point === 0x7f;
  });
  const validResourceId =
    new TextEncoder().encode(resourceId).length <= 256 &&
    resourceId.length > 0 &&
    !hasControlCharacter &&
    !resourceId.includes(":");
  if (
    !validResourceId ||
    !args.reason?.trim() ||
    actions.length === 0 ||
    actions.some((action) => !allowedActions.has(action))
  ) {
    throw new Error(
      "A permission request requires a valid resource identifier, valid actions, and a reason.",
    );
  }
  return { resourceId, actions, reason: args.reason.trim() };
}

export function createToolExecutor(env, emit) {
  const workspace = resolve(env.VERGLAS_AGENT_WORKSPACE || "/workspace");
  const scopedToken = env.VERGLAS_TOKEN;
  const principalId = env.VERGLAS_AGENT_PRINCIPAL_ID;
  if (
    !env.VERGLAS_DATA_ENDPOINT ||
    !env.VERGLAS_CONTAINER_RUNTIME_URL ||
    !env.VERGLAS_ACCESS_URI ||
    !scopedToken ||
    !principalId
  ) {
    throw new Error(
      "Agent runtime is missing its scoped Verglas capabilities.",
    );
  }
  const secrets = [scopedToken, env.LOCAL_MODEL_RUNTIME_TOKEN].filter(Boolean);
  const subprocessEnvironment = { ...env };
  delete subprocessEnvironment.LOCAL_MODEL_RUNTIME_TOKEN;
  delete subprocessEnvironment.LOCAL_MODEL_RUNTIME_URL;
  delete subprocessEnvironment.VERGLAS_AGENT_CONTROLLER_URL;

  return async (name, args = {}) => {
    switch (name) {
      case "describeEnvironment":
        return {
          workspace,
          runtime: { bun: true, nodeCompatible: true },
          sdk: {
            package: "@verglas/sdk",
            dataEndpoint: "VERGLAS_DATA_ENDPOINT",
            accessEndpoint: "VERGLAS_ACCESS_URI",
            runtimeEndpoint: "VERGLAS_CONTAINER_RUNTIME_URL",
            credential: "VERGLAS_TOKEN",
          },
          permissions: {
            principalId,
            requestTool: "requestPermission",
          },
          network: { https: true },
        };
      case "execute": {
        const cwd = args.cwd ? workspacePath(workspace, args.cwd) : workspace;
        return await executeProgram(args, subprocessEnvironment, cwd, secrets);
      }
      case "readFile": {
        const absolute = workspacePath(workspace, args.path);
        const metadata = await stat(absolute);
        if (!metadata.isFile())
          throw new Error("readFile requires a regular file.");
        const data = await readFile(absolute);
        const result = boundedText(data.toString("utf8"), MAX_FILE_BYTES);
        return {
          path: relative(workspace, absolute),
          content: result.text,
          truncated: result.truncated,
        };
      }
      case "writeFile": {
        if (typeof args.content !== "string")
          throw new Error("writeFile requires content.");
        if (Buffer.byteLength(args.content) > MAX_FILE_BYTES) {
          throw new Error("writeFile content exceeds 1 MiB.");
        }
        const absolute = workspacePath(workspace, args.path);
        await mkdir(dirname(absolute), { recursive: true });
        await writeFile(absolute, args.content, "utf8");
        return {
          path: relative(workspace, absolute),
          bytes: Buffer.byteLength(args.content),
        };
      }
      case "editFile": {
        if (typeof args.oldText !== "string" || !args.oldText) {
          throw new Error("editFile requires non-empty oldText.");
        }
        if (typeof args.newText !== "string")
          throw new Error("editFile requires newText.");
        const absolute = workspacePath(workspace, args.path);
        const original = await readFile(absolute, "utf8");
        const first = original.indexOf(args.oldText);
        if (first < 0) throw new Error("editFile oldText was not found.");
        if (original.indexOf(args.oldText, first + args.oldText.length) >= 0) {
          throw new Error("editFile oldText must occur exactly once.");
        }
        const content = `${original.slice(0, first)}${args.newText}${original.slice(first + args.oldText.length)}`;
        if (Buffer.byteLength(content) > MAX_FILE_BYTES) {
          throw new Error("Edited file exceeds 1 MiB.");
        }
        await writeFile(absolute, content, "utf8");
        return { path: relative(workspace, absolute), replacements: 1 };
      }
      case "webFetch": {
        const url = new URL(args.url);
        if (url.protocol !== "https:")
          throw new Error("webFetch requires an HTTPS URL.");
        const timeout = Math.min(
          Math.max(Number(args.timeoutMs) || 30_000, 1),
          MAX_FETCH_MS,
        );
        const response = await fetch(url, {
          method: args.method || "GET",
          headers: args.headers,
          body: args.body,
          redirect: "follow",
          signal: AbortSignal.timeout(timeout),
        });
        const result = boundedText(await response.text());
        return {
          url: response.url || url.toString(),
          status: response.status,
          contentType: response.headers.get("content-type"),
          body: result.text,
          truncated: result.truncated,
        };
      }
      case "requestPermission": {
        const permission = validatePermissionRequest(args);
        const requestId = `${env.VERGLAS_AGENT_CHAT_ID}:${crypto.randomUUID()}`;
        await emit({
          type: "permissionRequest",
          requestId,
          principalId,
          resourceId: permission.resourceId,
          actions: permission.actions,
          reason: permission.reason,
          state: "pending",
        });
        return { permissionRequested: true, requestId };
      }
      default:
        throw new Error(`Unknown agent tool ${name}.`);
    }
  };
}
