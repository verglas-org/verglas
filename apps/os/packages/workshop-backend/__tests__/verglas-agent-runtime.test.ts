import {describe, expect, it} from "vitest";
import {VerglasAgentRuntimeClient} from "../src/verglas-agent-runtime.js";

describe("VerglasAgentRuntimeClient", () => {
  it("calls the Workers fetch primitive with the global receiver", async () => {
    const receivers: unknown[] = [];
    const fetcher = function(this: unknown) {
      receivers.push(this);
      return Promise.resolve(Response.json({
        id: "workspace-1",
        owner_id: "user@example.com",
        title: "Untitled Workspace",
        pinned: false,
        created_at: "2026-08-11T00:00:00Z",
        updated_at: "2026-08-11T00:00:00Z",
      }, {status: 201}));
    } as typeof fetch;
    const client = new VerglasAgentRuntimeClient({
      endpoint: "http://agent-runtime.test",
      token: "runtime-secret",
      tenantId: "tenant-a",
    }, fetcher);

    await client.createWorkspace("workspace-1", "user@example.com", "Untitled Workspace");

    expect(receivers).toEqual([globalThis]);
  });
});
