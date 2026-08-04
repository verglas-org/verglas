// A minimal, scriptable change-feed websocket server for tests: a real RFC 6455
// endpoint over node:http, hand-rolled so the SDK's feed can be driven end to
// end without a `ws` dependency. node:http and the frame codec live ONLY here in
// tests — the SDK itself uses the global WebSocket and stays runtime-neutral.
//
// It speaks the feed wire protocol: sends `hello` on attach, records the
// `subscribe` cursors it receives, and lets a test push `change`/`resync` frames
// or drop sockets to exercise reconnect.

import { createServer, type IncomingMessage, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import type { Socket } from "node:net";
import { createHash } from "node:crypto";

const GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

/** Encodes a text payload as a single unmasked server frame. */
function encodeText(payload: string): Buffer {
  const body = Buffer.from(payload, "utf8");
  const len = body.length;
  let header: Buffer;
  if (len < 126) {
    header = Buffer.from([0x81, len]);
  } else if (len < 65536) {
    header = Buffer.from([0x81, 126, (len >> 8) & 0xff, len & 0xff]);
  } else {
    header = Buffer.alloc(10);
    header[0] = 0x81;
    header[1] = 127;
    header.writeBigUInt64BE(BigInt(len), 2);
  }
  return Buffer.concat([header, body]);
}

/** One live connection to the mock feed. */
export interface FeedConn {
  /** Cursors this connection has sent on `subscribe`, in order (null = live). */
  readonly subscribeCursors: (number | null)[];
  /** Sends a `change` frame to this connection. */
  sendChange(change: { seq: number; table: string; snapshotId?: string; committedAt?: string }): void;
  /** Sends a `resync` frame to this connection. */
  sendResync(reason?: string): void;
  /** Abruptly closes the underlying socket (to exercise reconnect). */
  drop(): void;
}

class Conn implements FeedConn {
  readonly subscribeCursors: (number | null)[] = [];
  private buf = Buffer.alloc(0);

  constructor(
    private readonly socket: Socket,
    private readonly onSubscribe: () => void,
  ) {
    socket.on("data", (chunk) => this.onData(chunk));
    socket.on("error", () => void 0);
  }

  /** Sends the initial hello with a starting cursor. */
  hello(cursor: number): void {
    this.socket.write(encodeText(JSON.stringify({ type: "hello", cursor })));
  }

  sendChange(change: { seq: number; table: string; snapshotId?: string; committedAt?: string }): void {
    this.socket.write(
      encodeText(
        JSON.stringify({
          type: "change",
          seq: change.seq,
          table: change.table,
          snapshot_id: change.snapshotId ?? `snap-${change.seq}`,
          committed_at: change.committedAt ?? new Date().toISOString(),
        }),
      ),
    );
  }

  sendResync(reason = "cursor-too-old"): void {
    this.socket.write(encodeText(JSON.stringify({ type: "resync", reason })));
  }

  drop(): void {
    this.socket.destroy();
  }

  /** Buffers input and parses every complete client frame it contains. */
  private onData(chunk: Buffer): void {
    this.buf = Buffer.concat([this.buf, chunk]);
    for (;;) {
      const frame = this.takeFrame();
      if (!frame) return;
      const { opcode, payload } = frame;
      if (opcode === 0x8) {
        this.socket.end();
        return;
      }
      if (opcode === 0x1) {
        try {
          const msg = JSON.parse(payload.toString("utf8")) as { type?: string; cursor?: number | null };
          if (msg.type === "subscribe") {
            this.subscribeCursors.push(msg.cursor ?? null);
            this.onSubscribe();
          }
        } catch {
          // ignore garbled client frame
        }
      }
    }
  }

  /** Removes and returns one complete masked client frame from the buffer, or
   *  null when more bytes are needed. */
  private takeFrame(): { opcode: number; payload: Buffer } | null {
    if (this.buf.length < 2) return null;
    const b0 = this.buf[0];
    const b1 = this.buf[1];
    const opcode = b0 & 0x0f;
    const masked = (b1 & 0x80) !== 0;
    let len = b1 & 0x7f;
    let offset = 2;
    if (len === 126) {
      if (this.buf.length < offset + 2) return null;
      len = this.buf.readUInt16BE(offset);
      offset += 2;
    } else if (len === 127) {
      if (this.buf.length < offset + 8) return null;
      len = Number(this.buf.readBigUInt64BE(offset));
      offset += 8;
    }
    let mask: Buffer | undefined;
    if (masked) {
      if (this.buf.length < offset + 4) return null;
      mask = this.buf.subarray(offset, offset + 4);
      offset += 4;
    }
    if (this.buf.length < offset + len) return null;
    const raw = this.buf.subarray(offset, offset + len);
    const payload = Buffer.alloc(len);
    for (let i = 0; i < len; i++) payload[i] = mask ? raw[i] ^ mask[i % 4] : raw[i];
    this.buf = this.buf.subarray(offset + len);
    return { opcode, payload };
  }
}

/** The running mock feed and its controls. */
export interface MockFeed {
  /** HTTP base URL; the SDK turns this into `ws://…/v1/catalog/feed`. */
  url: string;
  token: string;
  /** Every connection accepted so far, oldest first. */
  readonly conns: Conn[];
  /** Number of upgrade attempts that presented a bad/absent token. */
  readonly rejectedAuth: number;
  /** The starting cursor sent in `hello` (mutable between connects). */
  helloCursor: number;
  /** Resolves once at least `n` connections have been accepted. */
  waitForConns(n: number): Promise<void>;
  /** The most recently accepted connection. */
  latest(): Conn;
  close(): Promise<void>;
}

/** Starts the mock feed and resolves once it is listening. */
export async function startMockFeed(token = "test-token"): Promise<MockFeed> {
  const conns: Conn[] = [];
  let rejectedAuth = 0;
  const state = { helloCursor: 0 };
  let connWaiters: (() => void)[] = [];

  const server: Server = createServer((_req, res) => {
    res.writeHead(426, { "content-type": "text/plain" });
    res.end("upgrade required");
  });

  server.on("upgrade", (req: IncomingMessage, socket: Socket) => {
    if (req.url !== "/v1/catalog/feed" || req.headers.authorization !== `Bearer ${token}`) {
      rejectedAuth++;
      socket.write("HTTP/1.1 401 Unauthorized\r\nConnection: close\r\n\r\n");
      socket.destroy();
      return;
    }
    const key = req.headers["sec-websocket-key"] ?? "";
    const accept = createHash("sha1").update(key + GUID).digest("base64");
    socket.write(
      "HTTP/1.1 101 Switching Protocols\r\n" +
        "Upgrade: websocket\r\n" +
        "Connection: Upgrade\r\n" +
        `Sec-WebSocket-Accept: ${accept}\r\n\r\n`,
    );
    const notify = () => {
      const waiters = connWaiters;
      connWaiters = [];
      for (const w of waiters) w();
    };
    const conn = new Conn(socket, () => void 0);
    conns.push(conn);
    conn.hello(state.helloCursor);
    notify();
  });

  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const { port } = server.address() as AddressInfo;

  return {
    url: `http://127.0.0.1:${port}`,
    token,
    conns,
    get rejectedAuth() {
      return rejectedAuth;
    },
    get helloCursor() {
      return state.helloCursor;
    },
    set helloCursor(v: number) {
      state.helloCursor = v;
    },
    waitForConns(n: number): Promise<void> {
      if (conns.length >= n) return Promise.resolve();
      return new Promise<void>((resolve) => {
        const check = () => {
          if (conns.length >= n) resolve();
          else connWaiters.push(check);
        };
        connWaiters.push(check);
      });
    },
    latest: () => conns[conns.length - 1],
    close: () =>
      new Promise<void>((resolve) => {
        for (const c of conns) c.drop();
        server.close(() => resolve());
      }),
  };
}

/** Waits until `pred` holds, polling briefly. Rejects after `timeoutMs`. */
export async function until(pred: () => boolean, timeoutMs = 2000): Promise<void> {
  const start = Date.now();
  while (!pred()) {
    if (Date.now() - start > timeoutMs) throw new Error("until: timed out");
    await new Promise((r) => setTimeout(r, 5));
  }
}
