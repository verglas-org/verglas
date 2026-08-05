import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

import { parseManifest } from "./index.js";

const fixtureUrl = new URL("../tests/fixtures/valid.yaml", import.meta.url);
const valid = await readFile(fixtureUrl, "utf8");

test("parses the generated MicroVMStack contract", () => {
  const stack = parseManifest(valid);
  assert.equal(stack.apiVersion, "verglas.io/v1alpha1");
  assert.equal(stack.components[0].cluster.members, 3);
});

test("rejects semantic dependency violations", () => {
  assert.throws(
    () => parseManifest(valid.replace("dependsOn: [postgres]", "dependsOn: [missing]")),
    /missing component/,
  );
});

test("rejects unknown fields through the generated schema", () => {
  assert.throws(
    () => parseManifest(valid.replace("kind: MicroVMStack", "kind: MicroVMStack\nlegacy: true")),
    /must NOT have additional properties/,
  );
});
