// The catalog change feed: one websocket per client, carrying table-commit
// notifications from the platform's edge. The tenant backend scales to zero, so
// the long-lived socket is held by the edge Durable Object — the SDK never opens
// a long-lived connection to a backend. `VerglasClient.follow` registers a
// subscription here; many follows multiplex over the single socket, and each is
// filtered to its own table(s) client-side.
//
// Wire protocol (must match the edge exactly):
//   - server → client  {"type":"hello","cursor":<int>}                 on attach
//   - client → server  {"type":"subscribe","cursor":<int|null>}        null = live
//   - server → client  {"type":"change","seq":<int>,"table":"ns.t",
//                        "snapshot_id":"<str>","committed_at":"<ISO>"}
//   - server → client  {"type":"resync","reason":"cursor-too-old"}
//
// The socket may drop at any time; we reconnect with capped exponential backoff
// and resume from the lowest seq any live subscription still needs.

import type { ChangeEvent, ChangeHandler, FeedSubscription, FollowFeedOptions } from "./types";

/** Reconnect backoff: base delay doubles per attempt, capped, with jitter. */
const BACKOFF_BASE_MS = 250;
const BACKOFF_CAP_MS = 60_000;

/**
 * The delay before reconnect attempt `attempt` (0-based): an exponential ramp
 * `base * 2^attempt` capped at ~60s, then up to ±25% jitter so a fleet of
 * clients does not reconnect in lockstep. Pure and deterministic given `rand`.
 */
export function backoffDelay(attempt: number, rand: () => number = Math.random): number {
  const raw = BACKOFF_BASE_MS * 2 ** Math.max(0, attempt);
  const capped = Math.min(raw, BACKOFF_CAP_MS);
  const jitter = capped * 0.25 * (rand() * 2 - 1);
  return Math.round(capped + jitter);
}

/**
 * Builds the change-feed websocket URL from a client endpoint: the endpoint's
 * origin with `https`→`wss` and `http`→`ws`, at `/v1/catalog/feed`. Any path,
 * query, or fragment on the endpoint is dropped — the feed lives at the origin.
 */
export function feedUrl(endpoint: string): string {
  const u = new URL(endpoint);
  u.protocol = u.protocol === "https:" ? "wss:" : "ws:";
  u.pathname = "/v1/catalog/feed";
  u.search = "";
  u.hash = "";
  return u.toString();
}

/** The subset of the WebSocket global the feed uses (typed to allow the
 *  non-standard `headers` init the runtimes accept for bearer auth). */
interface FeedSocket {
  readyState: number;
  send(data: string): void;
  close(): void;
  addEventListener(type: "open", cb: () => void): void;
  addEventListener(type: "message", cb: (ev: { data: unknown }) => void): void;
  addEventListener(type: "close", cb: () => void): void;
  addEventListener(type: "error", cb: () => void): void;
}
type FeedSocketCtor = new (url: string, init?: { headers?: Record<string, string> }) => FeedSocket;
const OPEN = 1;

/** One registered follow: which tables it wants, its handler, and the seq
 *  boundary past which changes are still undelivered. */
interface Sub {
  readonly tables: Set<string>;
  readonly handler: ChangeHandler;
  readonly onResync?: (info: { reason: string }) => void;
  /**
   * Deliver only changes with `seq > since`. An int cursor starts it at that
   * cursor; a live-only (`null`) start leaves it `null` until the next `hello`
   * resolves it to the connection's current seq. Advances to the highest seq the
   * subscription has witnessed, so a reconnect resumes without gap or repeat.
   */
  since: number | null;
  /**
   * Whether this subscription demands the edge replay history from `since`.
   * A replay follow (int cursor) sets it; a live-only follow does not — until a
   * socket drop, after which every subscription must resume from `since` to not
   * miss the gap. This is what keeps a live-only follow's `subscribe` cursor
   * `null` on first attach yet an int seq on reconnect.
   */
  resume: boolean;
  readonly resolveClosed: () => void;
  onAbort?: () => void;
  readonly signal?: AbortSignal;
}

/**
 * The per-client change feed. Holds at most one websocket; opens it on the first
 * subscription and tears it down when the last one closes. All message-handling
 * is a small state machine over hello/change/resync, with capped-backoff
 * reconnect. Injectable clock and socket constructor keep it unit-testable; the
 * public client wires in the real globals.
 */
export class CatalogFeed {
  private ws?: FeedSocket;
  private readonly subs = new Set<Sub>();
  /** Highest seq witnessed on the current wire (from hello or any change). */
  private lastSeq = 0;
  /** The cursor last sent on `subscribe`, to avoid redundant re-subscribes. */
  private subscribedCursor: number | null | undefined;
  private helloSeen = false;
  private reconnectAttempt = 0;
  private reconnectTimer?: ReturnType<typeof setTimeout>;
  private connecting = false;

  constructor(
    private readonly url: string,
    private readonly token: string,
    private readonly WebSocketImpl: FeedSocketCtor,
    private readonly setTimer: (cb: () => void, ms: number) => ReturnType<typeof setTimeout> = setTimeout,
    private readonly clearTimer: (t: ReturnType<typeof setTimeout>) => void = clearTimeout,
  ) {}

  /** Registers a follow and returns its subscription handle. Opens the socket
   *  if this is the first follow. */
  follow(tables: string[], handler: ChangeHandler, opts?: FollowFeedOptions): FeedSubscription {
    let resolveClosed!: () => void;
    const closed = new Promise<void>((r) => (resolveClosed = r));
    const cursor = opts?.cursor;
    const sub: Sub = {
      tables: new Set(tables),
      handler,
      onResync: opts?.onResync,
      // Live-only until a hello resolves it; an int cursor starts there.
      since: typeof cursor === "number" ? cursor : null,
      // An explicit cursor demands replay; a live-only follow does not (yet).
      resume: typeof cursor === "number",
      resolveClosed,
      signal: opts?.signal,
    };

    // A subscription joining a live socket that has already said hello starts
    // live from the current seq (unless it asked to replay from an int cursor).
    if (this.helloSeen && sub.since === null) sub.since = this.lastSeq;

    this.subs.add(sub);

    if (opts?.signal) {
      if (opts.signal.aborted) {
        // Aborted before we even attached: close immediately, next tick.
        queueMicrotask(() => this.remove(sub));
      } else {
        sub.onAbort = () => this.remove(sub);
        opts.signal.addEventListener("abort", sub.onAbort, { once: true });
      }
    }

    this.ensureConnected();
    // If already live, a new int-cursor follow may need replay from below the
    // current subscribe cursor; re-subscribe when the floor drops.
    if (this.helloSeen) this.maybeResubscribe();

    return { close: () => this.remove(sub), closed };
  }

  /** Opens the socket if none is live and there is work to do. */
  private ensureConnected(): void {
    if (this.ws || this.connecting || this.subs.size === 0) return;
    this.connect();
  }

  /** Opens a fresh websocket and wires its lifecycle. */
  private connect(): void {
    this.connecting = true;
    this.helloSeen = false;
    this.subscribedCursor = undefined;
    let ws: FeedSocket;
    try {
      ws = new this.WebSocketImpl(this.url, { headers: { authorization: `Bearer ${this.token}` } });
    } catch {
      this.connecting = false;
      this.scheduleReconnect();
      return;
    }
    this.ws = ws;
    ws.addEventListener("open", () => {
      this.connecting = false;
    });
    ws.addEventListener("message", (ev) => this.onMessage(ev.data));
    ws.addEventListener("close", () => this.onDrop(ws));
    ws.addEventListener("error", () => this.onDrop(ws));
  }

  /** Handles a wire message: hello, change, or resync. Unknown/garbled frames
   *  are ignored — the feed never crashes on input. */
  private onMessage(data: unknown): void {
    if (typeof data !== "string") return;
    let msg: Record<string, unknown>;
    try {
      msg = JSON.parse(data) as Record<string, unknown>;
    } catch {
      return;
    }
    switch (msg.type) {
      case "hello":
        this.onHello(typeof msg.cursor === "number" ? msg.cursor : 0);
        break;
      case "change":
        this.onChange(msg);
        break;
      case "resync":
        this.onResync(typeof msg.reason === "string" ? msg.reason : "cursor-too-old");
        break;
    }
  }

  /** hello: mark the connection live, resolve any live-only subs to the current
   *  seq, and send our subscribe. A clean hello resets the backoff. */
  private onHello(cursor: number): void {
    this.lastSeq = Math.max(this.lastSeq, cursor);
    this.helloSeen = true;
    this.reconnectAttempt = 0;
    for (const sub of this.subs) if (sub.since === null) sub.since = this.lastSeq;
    this.sendSubscribe();
  }

  /** change: dispatch to every subscription that wants the table and has not
   *  seen this seq, then advance each subscription's seq boundary. */
  private onChange(msg: Record<string, unknown>): void {
    const seq = typeof msg.seq === "number" ? msg.seq : undefined;
    const table = typeof msg.table === "string" ? msg.table : undefined;
    if (seq === undefined || table === undefined) return;
    this.lastSeq = Math.max(this.lastSeq, seq);
    const event: ChangeEvent = {
      seq,
      table,
      snapshotId: String(msg.snapshot_id ?? ""),
      committedAt: String(msg.committed_at ?? ""),
    };
    for (const sub of this.subs) {
      if (sub.since !== null && seq > sub.since) {
        if (sub.tables.has(table)) sub.handler(event);
        // Advance past this seq whether or not it matched, so the subscription
        // resumes from here and never re-processes it after a reconnect.
        sub.since = seq;
      }
    }
  }

  /** resync: the edge dropped the replay we asked for. Tell every subscription,
   *  jump them all to live, and re-subscribe live. */
  private onResync(reason: string): void {
    for (const sub of this.subs) {
      sub.since = this.lastSeq;
      sub.resume = false; // history is gone; go live
      sub.onResync?.({ reason });
    }
    this.sendSubscribe(); // computes null: every subscription is live now
  }

  /** The lowest seq any subscription still demands replayed. `null` when none
   *  demand replay — a live-only subscribe. */
  private computeCursor(): number | null {
    let min: number | null = null;
    for (const sub of this.subs) {
      if (!sub.resume || sub.since === null) continue;
      if (min === null || sub.since < min) min = sub.since;
    }
    return min;
  }

  /** Sends subscribe with the computed floor cursor. */
  private sendSubscribe(): void {
    const cursor = this.computeCursor();
    this.subscribedCursor = cursor;
    this.rawSend({ type: "subscribe", cursor });
  }

  /** Re-subscribes only when the needed floor dropped below what we last sent
   *  (a new replay follow joined a live socket). */
  private maybeResubscribe(): void {
    const cursor = this.computeCursor();
    const prev = this.subscribedCursor;
    const dropped = cursor !== null && (prev === null || prev === undefined || cursor < prev);
    if (dropped) this.sendSubscribe();
  }

  /** Serializes and sends a control frame if the socket is open. */
  private rawSend(obj: unknown): void {
    if (this.ws && this.ws.readyState === OPEN) this.ws.send(JSON.stringify(obj));
  }

  /** A drop from the socket we still hold triggers a backoff reconnect. Ignores
   *  drops from a superseded socket. */
  private onDrop(ws: FeedSocket): void {
    if (ws !== this.ws) return;
    this.ws = undefined;
    this.connecting = false;
    this.helloSeen = false;
    // Every surviving subscription must now resume from where it left off, so
    // the reconnect subscribe carries a floor cursor and misses no commit.
    for (const sub of this.subs) sub.resume = true;
    if (this.subs.size > 0) this.scheduleReconnect();
  }

  /** Schedules the next reconnect with capped exponential backoff. */
  private scheduleReconnect(): void {
    if (this.reconnectTimer !== undefined) return;
    const delay = backoffDelay(this.reconnectAttempt);
    this.reconnectAttempt++;
    this.reconnectTimer = this.setTimer(() => {
      this.reconnectTimer = undefined;
      if (this.subs.size > 0) this.connect();
    }, delay);
  }

  /** Removes a subscription, resolving its `closed`. Tears the socket down when
   *  the last follow goes away. Idempotent per subscription. */
  private remove(sub: Sub): void {
    if (!this.subs.delete(sub)) return;
    if (sub.onAbort && sub.signal) sub.signal.removeEventListener("abort", sub.onAbort);
    sub.resolveClosed();
    if (this.subs.size === 0) this.teardown();
  }

  /** Closes the socket and cancels any pending reconnect. A later `follow`
   *  reconnects from scratch. */
  private teardown(): void {
    if (this.reconnectTimer !== undefined) {
      this.clearTimer(this.reconnectTimer);
      this.reconnectTimer = undefined;
    }
    this.reconnectAttempt = 0;
    const ws = this.ws;
    this.ws = undefined;
    this.helloSeen = false;
    this.connecting = false;
    if (ws) {
      try {
        ws.close();
      } catch {
        // closing an already-closing socket is fine
      }
    }
  }
}

/** Resolves the global WebSocket constructor, typed for the header init the
 *  runtimes accept. Throws if the runtime has no WebSocket — the feed needs the
 *  standard global (Bun, Node ≥ 22, Workers all provide it). */
export function globalWebSocket(): FeedSocketCtor {
  const ws = (globalThis as { WebSocket?: unknown }).WebSocket;
  if (typeof ws !== "function") {
    throw new Error(
      "client.follow: no global WebSocket. Run on Bun, Node >= 22, or a Workers runtime, " +
        "or provide one on globalThis.WebSocket.",
    );
  }
  return ws as FeedSocketCtor;
}
