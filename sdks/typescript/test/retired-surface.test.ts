import { describe, expect, it } from "vitest";
import { VerglasClient } from "../src/index";

describe("retired platform surfaces", () => {
  it("does not expose query or the old graph handle", () => {
    expect("query" in VerglasClient.prototype).toBe(false);
    expect("graph" in VerglasClient.prototype).toBe(false);
  });
});
