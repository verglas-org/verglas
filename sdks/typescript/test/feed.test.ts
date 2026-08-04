import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { connect, type VerglasClient, type ChangeEvent } from "../src/index";
import { backoffDelay, feedUrl } from "../src/feed";
import { startMockFeed, until, type MockFeed } from "./mock-feed";

let feed: MockFeed;
let client: VerglasClient;

beforeEach(async () => {
  feed = await startMockFeed();
  client = connect({ endpoint: feed.url, token: feed.token });
});
afterEach(() => feed.close());

describe("backoffDelay", () => {
  it("ramps exponentially and caps near 60s", () => {
    const noJitter = () => 0.5; // rand=0.5 -> zero jitter term
    expect(backoffDelay(0, noJitter)).toBe(250);
    expect(backoffDelay(1, noJitter)).toBe(500);
    expect(backoffDelay(2, noJitter)).toBe(1000);
    expect(backoffDelay(3, noJitter)).toBe(2000);
    // Deep attempts saturate at the 60s cap.
    expect(backoffDelay(20, noJitter)).toBe(60_000);
    expect(backoffDelay(50, noJitter)).toBe(60_000);
  });

  it("keeps jitter within +/-25%", () => {
    for (let a = 0; a < 8; a++) {
      const lo = backoffDelay(a, () => 0); // -25%
      const hi = backoffDelay(a, () => 1); // +25%
      const mid = Math.min(250 * 2 ** a, 60_000);
      expect(lo).toBeGreaterThanOrEqual(Math.round(mid * 0.75) - 1);
      expect(hi).toBeLessThanOrEqual(Math.round(mid * 1.25) + 1);
    }
  });
});

describe("feedUrl", () => {
  it("maps the endpoint origin to a wss feed path", () => {
    expect(feedUrl("https://t.verglas.cloud")).toBe("wss://t.verglas.cloud/v1/catalog/feed");
    expect(feedUrl("http://127.0.0.1:8334")).toBe("ws://127.0.0.1:8334/v1/catalog/feed");
  });
  it("drops any path, query, and fragment on the endpoint", () => {
    expect(feedUrl("https://h/base?x=1#f")).toBe("wss://h/v1/catalog/feed");
  });
});

describe("client.follow", () => {
  it("requires at least one non-empty table name", () => {
    expect(() => client.follow("", () => {})).toThrow(/table name is required/);
    expect(() => client.follow([], () => {})).toThrow(/table name is required/);
  });

  it("attaches, sends a live-only subscribe, and delivers matching changes", async () => {
    const seen: ChangeEvent[] = [];
    const sub = client.follow("ns.a", (c) => seen.push(c));

    await feed.waitForConns(1);
    const conn = feed.latest();
    await until(() => conn.subscribeCursors.length >= 1);
    expect(conn.subscribeCursors[0]).toBeNull(); // no cursor => live-only

    conn.sendChange({ seq: 1, table: "ns.a", snapshotId: "snap-9", committedAt: "2026-08-01T00:00:00Z" });
    await until(() => seen.length >= 1);
    expect(seen[0]).toEqual({
      seq: 1,
      table: "ns.a",
      snapshotId: "snap-9",
      committedAt: "2026-08-01T00:00:00Z",
    });

    sub.close();
    await sub.closed;
  });

  it("filters changes to the followed table client-side", async () => {
    const seen: string[] = [];
    client.follow("ns.a", (c) => seen.push(`${c.table}#${c.seq}`));
    await feed.waitForConns(1);
    const conn = feed.latest();
    await until(() => conn.subscribeCursors.length >= 1);

    conn.sendChange({ seq: 1, table: "ns.b" }); // other table
    conn.sendChange({ seq: 2, table: "ns.a" });
    conn.sendChange({ seq: 3, table: "ns.c" }); // other table
    await until(() => seen.length >= 1);
    await new Promise((r) => setTimeout(r, 30));
    expect(seen).toEqual(["ns.a#2"]);
  });

  it("replays from an int cursor and drops seqs at or below it", async () => {
    feed.helloCursor = 0;
    const seen: number[] = [];
    client.follow("ns.a", (c) => seen.push(c.seq), { cursor: 3 });
    await feed.waitForConns(1);
    const conn = feed.latest();
    await until(() => conn.subscribeCursors.length >= 1);
    expect(conn.subscribeCursors[0]).toBe(3);

    conn.sendChange({ seq: 2, table: "ns.a" }); // <= cursor: dropped
    conn.sendChange({ seq: 3, table: "ns.a" }); // == cursor: dropped
    conn.sendChange({ seq: 4, table: "ns.a" }); // > cursor: delivered
    await until(() => seen.length >= 1);
    await new Promise((r) => setTimeout(r, 30));
    expect(seen).toEqual([4]);
  });

  it("multiplexes several follows over one socket", async () => {
    const a: number[] = [];
    const b: number[] = [];
    client.follow("ns.a", (c) => a.push(c.seq));
    client.follow("ns.b", (c) => b.push(c.seq));
    await feed.waitForConns(1);
    const conn = feed.latest();
    await until(() => conn.subscribeCursors.length >= 1);

    conn.sendChange({ seq: 1, table: "ns.a" });
    conn.sendChange({ seq: 2, table: "ns.b" });
    await until(() => a.length >= 1 && b.length >= 1);
    expect(a).toEqual([1]);
    expect(b).toEqual([2]);
    expect(feed.conns.length).toBe(1); // one socket for both follows
  });

  it("re-subscribes from a lower floor when a replay follow joins live", async () => {
    client.follow("ns.a", () => {}); // live-only
    await feed.waitForConns(1);
    const conn = feed.latest();
    await until(() => conn.subscribeCursors.length >= 1);
    expect(conn.subscribeCursors[0]).toBeNull();

    // A second follow that wants replay from seq 2 lowers the floor.
    client.follow("ns.b", () => {}, { cursor: 2 });
    await until(() => conn.subscribeCursors.length >= 2);
    expect(conn.subscribeCursors[1]).toBe(2);
    expect(feed.conns.length).toBe(1);
  });

  it("surfaces resync, jumps to live, and re-subscribes", async () => {
    feed.helloCursor = 10;
    const seen: number[] = [];
    const resyncs: { reason: string }[] = [];
    client.follow("ns.a", (c) => seen.push(c.seq), { cursor: 5, onResync: (i) => resyncs.push(i) });
    await feed.waitForConns(1);
    const conn = feed.latest();
    await until(() => conn.subscribeCursors.length >= 1);
    expect(conn.subscribeCursors[0]).toBe(5);

    conn.sendResync();
    await until(() => resyncs.length >= 1);
    expect(resyncs[0]).toEqual({ reason: "cursor-too-old" });
    await until(() => conn.subscribeCursors.length >= 2);
    expect(conn.subscribeCursors[1]).toBeNull(); // re-subscribed live

    conn.sendChange({ seq: 6, table: "ns.a" }); // stale: below the live floor
    conn.sendChange({ seq: 11, table: "ns.a" }); // past the floor: delivered
    await until(() => seen.length >= 1);
    await new Promise((r) => setTimeout(r, 30));
    expect(seen).toEqual([11]);
  });

  it("reconnects after a drop and resumes from the last seen seq", async () => {
    const seen: number[] = [];
    client.follow("ns.a", (c) => seen.push(c.seq));
    await feed.waitForConns(1);
    const first = feed.latest();
    await until(() => first.subscribeCursors.length >= 1);

    first.sendChange({ seq: 1, table: "ns.a" });
    first.sendChange({ seq: 2, table: "ns.a" });
    await until(() => seen.length >= 2);

    first.drop();
    await feed.waitForConns(2); // client reconnected with backoff
    const second = feed.latest();
    await until(() => second.subscribeCursors.length >= 1);
    expect(second.subscribeCursors[0]).toBe(2); // resume from last seen

    second.sendChange({ seq: 3, table: "ns.a" });
    await until(() => seen.length >= 3);
    expect(seen).toEqual([1, 2, 3]); // no gap, no repeat
  });

  it("stops delivery after close and tears down the last socket", async () => {
    const seen: number[] = [];
    const sub = client.follow("ns.a", (c) => seen.push(c.seq));
    await feed.waitForConns(1);
    const conn = feed.latest();
    await until(() => conn.subscribeCursors.length >= 1);

    sub.close();
    await sub.closed;

    conn.sendChange({ seq: 1, table: "ns.a" });
    await new Promise((r) => setTimeout(r, 40));
    expect(seen).toEqual([]); // nothing delivered after close
  });

  it("ends a subscription when its signal aborts", async () => {
    const ctrl = new AbortController();
    const seen: number[] = [];
    const sub = client.follow("ns.a", (c) => seen.push(c.seq), { signal: ctrl.signal });
    await feed.waitForConns(1);
    const conn = feed.latest();
    await until(() => conn.subscribeCursors.length >= 1);

    ctrl.abort();
    await sub.closed;

    conn.sendChange({ seq: 1, table: "ns.a" });
    await new Promise((r) => setTimeout(r, 40));
    expect(seen).toEqual([]);
  });
});

describe("change-feed auth", () => {
  it("attaches with the bearer token and rejects a bad one", async () => {
    // The good client (from beforeEach) attaches cleanly.
    const good = client.follow("ns.a", () => {});
    await feed.waitForConns(1);
    good.close();

    const bad = connect({ endpoint: feed.url, token: "wrong" });
    const badSub = bad.follow("ns.a", () => {});
    await until(() => feed.rejectedAuth >= 1);
    expect(feed.rejectedAuth).toBeGreaterThanOrEqual(1);
    badSub.close(); // stop the reconnect loop
  });
});
