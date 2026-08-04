// The queue verb (#327): enqueue, poll by consumer group, ack. Exercises the
// at-least-once + consumer-idempotency contract against the mock endpoint.

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
  it("enqueues, polls by group, and acks", async () => {
    const q = client.queue("events");
    const res = await q.enqueue([{ n: 1 }, { n: 2 }, { n: 3 }]);
    expect(res.enqueued).toBe(3);
    expect(res.endPosition).toBe(3);

    // Poll two records for group "g".
    const first = await q.poll("g", { max: 2 });
    expect(first.watermark).toBe(0);
    expect(first.records.map((r) => r.row.n)).toEqual([1, 2]);
    expect(first.records.map((r) => r.position)).toEqual([0, 1]);

    // Ack through position 2; the next poll returns only the tail.
    await q.ack("g", 2);
    const rest = await q.poll("g");
    expect(rest.watermark).toBe(2);
    expect(rest.records.map((r) => r.row.n)).toEqual([3]);
  });

  it("re-serves un-acked records (at-least-once)", async () => {
    const q = client.queue("events");
    await q.enqueue([{ n: 1 }, { n: 2 }]);

    // A poll without an ack (a crash) re-serves the same records.
    const a = await q.poll("g");
    const b = await q.poll("g");
    expect(a.records).toEqual(b.records);
    expect(a.records.map((r) => r.position)).toEqual([0, 1]);
  });

  it("ack is monotone: a regressing position is ignored", async () => {
    const q = client.queue("events");
    await q.enqueue([{ n: 1 }, { n: 2 }, { n: 3 }]);
    await q.ack("g", 2);
    const back = await q.ack("g", 1);
    expect(back.watermark).toBe(2);
  });

  it("consumer groups are independent", async () => {
    const q = client.queue("events");
    await q.enqueue([{ n: 1 }, { n: 2 }]);
    await q.ack("g1", 2);
    const g2 = await q.poll("g2");
    expect(g2.watermark).toBe(0);
    expect(g2.records.map((r) => r.row.n)).toEqual([1, 2]);
  });

  it("requires a group name", () => {
    const q = client.queue("events");
    expect(() => q.poll("")).toThrow(/group is required/);
    expect(() => q.ack("", 1)).toThrow(/group is required/);
  });
});
