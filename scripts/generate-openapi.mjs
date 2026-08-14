#!/usr/bin/env node

// Emits the cache listener's semantic OpenAPI document from checked-in service models.

import { readFile, writeFile } from "node:fs/promises";

const semanticModels = [
  ["s3vectors-service-2.json", "AWS S3 Vectors"],
  ["verglas-graphs-service-1.json", "Verglas Graph"],
];
const semanticPaths = {};
for (const [file, tag] of semanticModels) {
  const model = JSON.parse(await readFile("contracts/" + file, "utf8"));
  for (const [operationId, operation] of Object.entries(model.operations)) {
    const path = operation.http.requestUri;
    const method = operation.http.method.toLowerCase();
    semanticPaths[path] ??= {};
    semanticPaths[path][method] = {
      operationId,
      tags: [tag],
      responses: { [String(operation.http.responseCode)]: { description: "Semantic operation response" } },
    };
  }
}
const semanticDoc = {
  openapi: "3.1.0",
  info: { title: "Verglas S3 listener API", version: "0.1.0" },
  paths: semanticPaths,
};
await writeFile("contracts/s3-openapi.json", JSON.stringify(semanticDoc, null, 2) + "\n");
