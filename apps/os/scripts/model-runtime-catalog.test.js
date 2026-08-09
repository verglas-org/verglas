import assert from "node:assert/strict";
import test from "node:test";
import { parseCursorModels } from "./model-runtime-catalog.mjs";

test("parseCursorModels preserves provider model IDs and default metadata", () => {
  assert.deepEqual(parseCursorModels(`Available models

auto - Auto (default)
gpt-5.6-sol-high - GPT-5.6 Sol 1M High
`), [
    { id: "auto", name: "Auto", isDefault: true },
    { id: "gpt-5.6-sol-high", name: "GPT-5.6 Sol 1M High" },
  ]);
});
