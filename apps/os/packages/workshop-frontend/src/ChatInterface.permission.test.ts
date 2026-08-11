import { describe, expect, it } from "vitest";
import type { AiChatMessage } from "@verglas/workshop-shared/api";
import { applyPermissionDecisionToMessages } from "./ChatInterface";

describe("permission request decisions", () => {
  it("replaces the pending cached message with its approved state", () => {
    const pending = {
      type: "permissionRequest",
      requestId: "0:request-1",
      state: "pending",
    } as AiChatMessage;
    const messages = [pending];

    const updated = applyPermissionDecisionToMessages(
      messages,
      "0:request-1",
      "approved",
    );

    expect(updated).not.toBe(messages);
    expect(updated[0]).toMatchObject({
      requestId: "0:request-1",
      state: "approved",
    });
  });

  it("preserves the cache when the request is absent", () => {
    const messages = [{ type: "message", message: "hello" }] as AiChatMessage[];
    expect(
      applyPermissionDecisionToMessages(messages, "0:missing", "denied"),
    ).toBe(messages);
  });
});
