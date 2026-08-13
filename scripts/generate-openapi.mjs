#!/usr/bin/env node

// Emits contracts/openapi.json from contracts/api-surface.json for REST visibility.
// Schemas are intentionally minimal: path inventory for humans and CI, not SDK codegen.

import { readFile, writeFile } from "node:fs/promises";

const contract = JSON.parse(await readFile("contracts/api-surface.json", "utf8"));
const paths = {};

for (const operation of contract.operations) {
  const routes = operation.server ?? [];
  for (const route of routes) {
    const path = route.path;
    const method = route.method.toLowerCase();
    const openApiMethod = method === "any" ? "get" : method;
    paths[path] ??= {};
    if (paths[path][openApiMethod]) {
      throw new Error(
        `Duplicate OpenAPI mapping for ${route.method} ${path}: ` +
          `${paths[path][openApiMethod].operationId} vs ${operation.id}`,
      );
    }
    const operationId =
      routes.length === 1 ? operation.id : `${operation.id}_${method}_${slug(path)}`;
    paths[path][openApiMethod] = {
      operationId,
      tags: [operation.family],
      summary: operation.id,
      ...(method === "any" ? { "x-verglas-method": "ANY" } : {}),
      responses: {
        "200": { description: "Success" },
        default: { description: "Error" },
      },
    };
  }
}

const doc = {
  openapi: "3.1.0",
  info: {
    title: "Verglas /v1 surface",
    version: String(contract.version),
    description:
      "Generated from contracts/api-surface.json. High-level SDKs remain hand-written; this document is for REST visibility and CI.",
  },
  servers: [{ url: "/", description: "Admin / query listener" }],
  paths,
};

await writeFile("contracts/openapi.json", `${JSON.stringify(doc, null, 2)}\n`);
console.log(`Wrote contracts/openapi.json (${Object.keys(paths).length} paths)`);

/**
 * Builds a short stable slug from a path template.
 * @param {string} path
 */
function slug(path) {
  return path.replaceAll(/[{}/*]/g, "").replaceAll(/_+/g, "_");
}
