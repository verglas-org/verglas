import { describe, expect, it } from "vitest";
import {
  applicationLifecycleAvailable,
  nextApplicationLifecycleState,
} from "./applicationLifecycle";

describe("applicationLifecycleAvailable", () => {
  it("allows container controls when the deployment exposes a local container runtime", () => {
    expect(applicationLifecycleAvailable(true)).toBe(true);
  });

  it("hides container controls when the deployment does not expose one", () => {
    expect(applicationLifecycleAvailable(false)).toBe(false);
    expect(applicationLifecycleAvailable(undefined)).toBe(false);
  });
});

describe("nextApplicationLifecycleState", () => {
  it("uses persisted desired state rather than a possibly stale Docker observation", () => {
    expect(nextApplicationLifecycleState(false)).toBe("running");
    expect(nextApplicationLifecycleState(true)).toBe("stopped");
    expect(nextApplicationLifecycleState(undefined)).toBe("stopped");
  });
});
