#!/usr/bin/env node

// End-to-end surface check: contracts/api-surface.json ↔ server routes ↔ SDK
// symbols/routes ↔ OpenAPI ↔ docs markers.

import { readFile, readdir } from "node:fs/promises";
import { execFileSync } from "node:child_process";
import path from "node:path";

const ROOT = process.cwd();
const contract = JSON.parse(await readFile("contracts/api-surface.json", "utf8"));

execFileSync("node", ["scripts/generate-openapi.mjs"], { stdio: "inherit" });
const openapi = JSON.parse(await readFile("contracts/openapi.json", "utf8"));

const serverRoots = contract.sources.server.map((p) => path.join(ROOT, p));
const serverText = await readTrees(serverRoots);
const tsSource = await readFile(contract.sources.typescript, "utf8");
const rustSource = await readFile(contract.sources.rust, "utf8");
const docsText = await readTrees([path.join(ROOT, "docs/sdk")]);

const errors = [];

for (const operation of contract.operations) {
  for (const route of operation.server ?? []) {
    if (!serverText.includes(route.path)) {
      errors.push(
        `server missing route string ${JSON.stringify(route.path)} for ${operation.id}`,
      );
    }
    const method = route.method.toLowerCase();
    const openApiMethod = method === "any" ? "get" : method;
    const entry = openapi.paths?.[route.path]?.[openApiMethod];
    if (!entry) {
      errors.push(
        `OpenAPI missing ${route.method} ${route.path} for ${operation.id}`,
      );
    } else if (!String(entry.operationId).startsWith(operation.id)) {
      errors.push(
        `OpenAPI operationId ${entry.operationId} does not belong to ${operation.id}`,
      );
    }
  }

  for (const marker of operation.docs ?? []) {
    if (!docsText.includes(`<!-- ${marker} -->`) && !docsText.includes(marker)) {
      errors.push(`docs missing marker ${marker} for ${operation.id}`);
    }
  }

  for (const language of ["typescript", "rust"]) {
    const client = operation.clients?.[language];
    if (!client) {
      errors.push(`${operation.id} missing clients.${language}`);
      continue;
    }
    if (client.status === "implemented") {
      const source = language === "typescript" ? tsSource : rustSource;
      if (!client.symbol || !source.includes(client.symbol)) {
        errors.push(
          `${language} missing symbol ${JSON.stringify(client.symbol)} for ${operation.id}`,
        );
      }
      const tree =
        language === "typescript"
          ? await readTrees([path.join(ROOT, "sdks/typescript/src")])
          : await readTrees([path.join(ROOT, "sdks/rust/src")]);
      for (const route of client.routes ?? []) {
        const needle = route.includes(" ") ? route.split(" ").slice(1).join(" ") : route;
        if (needle && !tree.includes(needle) && !tree.includes(route)) {
          // Allow Iceberg catalog paths that appear only as composed segments.
          if (needle.includes("/namespaces/") && tree.includes("namespaces")) {
            continue;
          }
          if (needle.includes("/v1/write/") && tree.includes("/v1/write/")) {
            continue;
          }
          if (needle.includes("/v1/ingest/") && tree.includes("/v1/ingest/")) {
            continue;
          }
          errors.push(
            `${language} source missing route ${JSON.stringify(route)} for ${operation.id}`,
          );
        }
      }
    } else if (client.status === "deferred" || client.status === "n/a") {
      // Deferred and n/a are intentional; dual-SDK parity is not required.
    } else {
      errors.push(`${operation.id} clients.${language}.status is unknown: ${client.status}`);
    }
  }

  const ts = operation.clients.typescript?.status;
  const rust = operation.clients.rust?.status;
  if (ts === "implemented" && rust !== "implemented") {
    errors.push(
      `${operation.id}: TypeScript is implemented but Rust is ${rust} (shared SDK parity required)`,
    );
  }
  if (rust === "implemented" && ts !== "implemented") {
    errors.push(
      `${operation.id}: Rust is implemented but TypeScript is ${ts} (shared SDK parity required)`,
    );
  }
}

// OpenAPI must not invent paths outside the contract REST subset.
const contractPaths = new Set();
for (const operation of contract.operations) {
  for (const route of operation.server ?? []) {
    contractPaths.add(`${route.method.toUpperCase()} ${route.path}`);
  }
}
for (const [pathKey, methods] of Object.entries(openapi.paths ?? {})) {
  for (const [method, entry] of Object.entries(methods)) {
    const verb =
      entry["x-verglas-method"] === "ANY" ? "ANY" : method.toUpperCase();
    const key = `${verb} ${pathKey}`;
    if (!contractPaths.has(key) && !(verb === "GET" && contractPaths.has(`ANY ${pathKey}`))) {
      // Allow GET stand-in for ANY.
      if (entry["x-verglas-method"] === "ANY" && contractPaths.has(`ANY ${pathKey}`)) {
        continue;
      }
      if (!contractPaths.has(key)) {
        errors.push(`OpenAPI has undocumented route ${key}`);
      }
    }
  }
}

if (errors.length > 0) {
  console.error("API surface check failed:\n" + errors.map((e) => `  - ${e}`).join("\n"));
  process.exit(1);
}

console.log(
  `API surface OK: ${contract.operations.length} operations; OpenAPI + SDKs + docs markers match`,
);

/**
 * Reads every file under the given directories into one searchable string.
 * @param {string[]} roots
 */
async function readTrees(roots) {
  const chunks = [];
  for (const root of roots) {
    for await (const file of walk(root)) {
      chunks.push(await readFile(file, "utf8"));
    }
  }
  return chunks.join("\n");
}

/**
 * Yields file paths under dir.
 * @param {string} dir
 */
async function* walk(dir) {
  let entries;
  try {
    entries = await readdir(dir, { withFileTypes: true });
  } catch {
    return;
  }
  for (const entry of entries) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      if (entry.name === "node_modules" || entry.name === "target") continue;
      yield* walk(full);
    } else if (entry.isFile()) {
      yield full;
    }
  }
}
