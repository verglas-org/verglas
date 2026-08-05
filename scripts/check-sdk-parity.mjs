#!/usr/bin/env node

// Compares the semantic data-plane inventory of the TypeScript and Rust SDKs.
// Each manifest also names a source symbol; checking that marker prevents a
// manifest from claiming an operation that its implementation no longer has.

import { readFile } from "node:fs/promises";

const specifications = [
  {
    name: "TypeScript",
    manifest: "sdks/typescript/sdk-parity.json",
    source: "sdks/typescript/src/client.ts",
  },
  {
    name: "Rust",
    manifest: "sdks/rust/sdk-parity.json",
    source: "sdks/rust/src/client.rs",
  },
];

const loaded = await Promise.all(
  specifications.map(async (specification) => {
    const manifest = JSON.parse(await readFile(specification.manifest, "utf8"));
    const source = await readFile(specification.source, "utf8");
    for (const capability of manifest.capabilities) {
      if (!source.includes(capability.symbol)) {
        throw new Error(
          `${specification.name} parity manifest claims ${capability.operation}, ` +
            `but ${specification.source} does not contain ${JSON.stringify(capability.symbol)}`,
        );
      }
    }
    return { specification, manifest };
  }),
);

const semantic = (manifest) => ({
  version: manifest.version,
  capabilities: manifest.capabilities
    .map(({ symbol: _symbol, ...capability }) => capability)
    .sort((left, right) => left.operation.localeCompare(right.operation)),
});

const expected = JSON.stringify(semantic(loaded[0].manifest));
for (const { specification, manifest } of loaded.slice(1)) {
  const actual = JSON.stringify(semantic(manifest));
  if (actual !== expected) {
    throw new Error(
      `${specification.name} SDK semantic surface differs from ${loaded[0].specification.name}\n` +
        `expected: ${expected}\nactual:   ${actual}`,
    );
  }
}

console.log(
  "SDK parity: core data operations and typed dashboard create/list/show/delete/refresh match",
);
