import { describe, expect, it } from "vitest";
import { chatWidgetStatus } from "./WorkspaceChatWidgets";

describe("chatWidgetStatus", () => {
  it("gives approvals clear lifecycle labels", () => {
    expect(chatWidgetStatus("pending")).toEqual({
      label: "Pending approval",
      tone: "pending",
    });
    expect(chatWidgetStatus("approved")).toEqual({
      label: "Approved",
      tone: "success",
    });
    expect(chatWidgetStatus("denied")).toEqual({
      label: "Denied",
      tone: "danger",
    });
  });

  it("covers the lifecycle of created chat resources", () => {
    expect(chatWidgetStatus("needs_configuration").label).toBe("Needs setup");
    expect(chatWidgetStatus("deploying").label).toBe("Deploying");
    expect(chatWidgetStatus("ready").label).toBe("Ready");
    expect(chatWidgetStatus("error").label).toBe("Failed");
  });
});
