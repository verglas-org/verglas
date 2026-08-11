import { describe, expect, it } from "vitest";
import { groupModels, modelRuntime } from "./modelRuntimePresentation";

describe("model runtime presentation", () => {
  it("groups selectable models by their linked runtime", () => {
    const groups = groupModels([
      { type: "agent", id: "runtime:codex:gpt-5.6-sol", name: "GPT-5.6-Sol" },
      {
        type: "agent",
        id: "runtime:codex:gpt-5.6-terra",
        name: "GPT-5.6-Terra",
      },
      {
        type: "agent",
        id: "runtime:claude-code:sonnet",
        name: "Claude Sonnet",
      },
    ]);

    expect(groups.map((group) => [group.label, group.models.length])).toEqual([
      ["Codex", 2],
      ["Claude", 1],
    ]);
  });

  it("does not mistake direct API model IDs for runtime connections", () => {
    expect(modelRuntime("custom-model")).toBeNull();
  });
});
