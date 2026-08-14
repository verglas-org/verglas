// Standalone queue-container contract: append, exclusive lease, fenced ack.

import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { connect, type VerglasClient } from "../src/index";
import { startMockEndpoint, type MockEndpoint } from "./mock-endpoint";

let endpoint: MockEndpoint;
let client: VerglasClient;

beforeEach(async () => {
  endpoint = await startMockEndpoint();
  client = connect({ endpoint: endpoint.url, token: endpoint.token });
});
afterEach(() => endpoint.close());

describe("queue verb", () => {
  it("enqueues, exclusively polls, and acknowledges a fenced receipt", async () => {
    const q = client.queue("events");
    const appended = await q.enqueue([
      {id: "event-1", topic: "orders", payload: {n: 1}},
      {id: "event-2", topic: "orders", payload: {n: 2}},
    ]);
    expect(appended.positions).toEqual([0, 1]);

    const first = await q.poll("workers", {
      owner: "consumer-a",
      topics: ["orders"],
      max: 1,
      leaseSeconds: 30,
    });
    expect(first.deliveries).toHaveLength(1);
    expect(first.deliveries[0].payload.n).toBe(1);

    const competing = await q.poll("workers", {
      owner: "consumer-b",
      topics: ["orders"],
      max: 1,
      leaseSeconds: 30,
    });
    expect(competing.deliveries[0].position).toBe(1);

    await q.ack("workers", first.deliveries[0].receipt);
  });

  it("requires group, owner, and a fenced receipt", () => {
    const q = client.queue("events");
    expect(() => q.poll("", {owner: "consumer", topics: ["orders"], leaseSeconds: 30})).toThrow(/group is required/);
    expect(() => q.poll("workers", {owner: "", topics: ["orders"], leaseSeconds: 30})).toThrow(/owner is required/);
    expect(() => q.ack("", {position: 1, owner: "consumer", generation: 1})).toThrow(/group is required/);
  });
});
