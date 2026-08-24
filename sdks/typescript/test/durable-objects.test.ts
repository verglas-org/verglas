import { describe, expect, it } from "vitest";
import { encodeArrowStream, encodeHex } from "../src/arrow-ipc";
import {
  DurableObject,
  DurableObjectId,
  DurableObjectNamespace,
  createWorkerRuntime,
  type DurableObjectTransport,
} from "../src/index";

class ScriptedTransport implements DurableObjectTransport {
  readonly commands: string[] = [];

  send(command: string): Promise<string> {
    this.commands.push(command);
    if (command.startsWith("COMMIT ")) return Promise.resolve("OK 1");
    if (command.startsWith("QUERY kv ")) {
      return Promise.resolve(
        `OK ${encodeHex(encodeArrowStream([{ name: "value", type: "int64", nullable: false }], [{ value: 7 }]))}`,
      );
    }
    return Promise.resolve("OK");
  }
}

type TestEnv = { COUNTER: DurableObjectNamespace<Counter> };

class Counter extends DurableObject<TestEnv> {
  private value = 0;

  constructor(ctx: ConstructorParameters<typeof DurableObject<TestEnv>>[0], env: TestEnv) {
    super(ctx, env);
    void ctx.blockConcurrencyWhile(async () => {
      this.value = (await ctx.storage.get<number>("value")) ?? 0;
    });
  }

  async fetch(request: Request): Promise<Response> {
    if (request.method === "POST") return Response.json({ value: await this.increment(1) });
    return Response.json({ value: this.value });
  }

  async increment(amount = 1): Promise<number> {
    this.value += amount;
    await this.ctx.storage.put("value", this.value);
    return this.value;
  }

  async query(): Promise<unknown[]> {
    const cursor = await this.ctx.storage.sql.exec("SELECT value FROM kv");
    return cursor.toArray();
  }

  async transactional(): Promise<number> {
    await this.ctx.storage.transaction(async (txn) => {
      await txn.put("value", 9);
    });
    return 1;
  }
}

class Ordered extends DurableObject {
  readonly events: string[] = [];

  constructor(ctx: ConstructorParameters<typeof DurableObject>[0], env: unknown) {
    super(ctx, env);
    void ctx.blockConcurrencyWhile(async () => {
      this.events.push("begin");
      await new Promise((resolve) => setTimeout(resolve, 5));
      this.events.push("end");
    });
  }

  fetch(): Response {
    this.events.push("fetch");
    return new Response(this.events.join(","));
  }
}

class AlarmObject extends DurableObject {
  fired = 0;

  async schedule(delay: number): Promise<void> {
    await this.ctx.storage.setAlarm(Date.now() + delay);
  }

  async alarm(): Promise<void> {
    this.fired += 1;
  }

  fetch(): Response {
    return new Response(String(this.fired));
  }
}

describe("Cloudflare Durable Objects API", () => {
  it("provides stable IDs and namespace lookup", () => {
    const namespace = new DurableObjectNamespace(Counter);
    const first = namespace.idFromName("alice");
    const second = namespace.idFromName("alice");

    expect(first).toBeInstanceOf(DurableObjectId);
    expect(first.equals(second)).toBe(true);
    expect(first.name).toBe("alice");
    expect(namespace.idFromString(first.toString()).toString()).toBe(first.toString());
    expect(namespace.newUniqueId().toString()).toMatch(/^[0-9a-f]{64}$/);
  });

  it("serves an authored Durable Object through a worker namespace", async () => {
    const transport = new ScriptedTransport();
    const runtime = createWorkerRuntime<TestEnv>({
      module: {
        default: {
          fetch(request, env) {
            return env.COUNTER.get(env.COUNTER.idFromName("alice")).fetch(request);
          },
        },
      },
      transport,
      durableObjects: { COUNTER: Counter },
    });

    const response = await runtime.fetch(new Request("https://worker.test/"));
    expect(await response.json()).toEqual({ value: 0 });
    expect(transport.commands.every((command) => !/^(BEGIN|STATEMENT)\b/.test(command))).toBe(true);
  });

  it("proxies public RPC methods through a stub and commits mutations", async () => {
    const transport = new ScriptedTransport();
    const namespace = new DurableObjectNamespace(Counter, { transport });
    const stub = namespace.get(namespace.idFromName("rpc"));

    await expect(stub.increment(2)).resolves.toBe(2);
    await expect(stub.increment(3)).resolves.toBe(5);
    expect(transport.commands.filter((command) => command.startsWith("COMMIT "))).toHaveLength(2);
  });

  it("waits for blockConcurrencyWhile before dispatching fetch", async () => {
    const namespace = new DurableObjectNamespace(Ordered, { transport: new ScriptedTransport() });
    const stub = namespace.get(namespace.idFromName("ordered"));

    const response = await stub.fetch("https://worker.test/");
    expect(await response.text()).toBe("begin,end,fetch");
  });

  it("bridges SQL through the scripted line transport", async () => {
    const transport = new ScriptedTransport();
    const namespace = new DurableObjectNamespace(Counter, { transport });
    const stub = namespace.get(namespace.idFromName("sql"));

    await expect(stub.query()).resolves.toEqual([{ value: 7 }]);
    expect(transport.commands.at(-1)).toBe(
      "QUERY kv 53454c4543542076616c75652046524f4d206b76",
    );
    expect(transport.commands.every((command) => !/^(BEGIN|STATEMENT)\b/.test(command))).toBe(true);
  });

  it("buffers storage transactions into one canonical COMMIT envelope", async () => {
    const transport = new ScriptedTransport();
    const namespace = new DurableObjectNamespace(Counter, { transport });
    const stub = namespace.get(namespace.idFromName("transaction"));

    await stub.transactional();
    expect(transport.commands.every((command) => !/^(BEGIN|STATEMENT)\b/.test(command))).toBe(true);
    expect(transport.commands.filter((command) => command.startsWith("COMMIT "))).toHaveLength(1);
    expect(transport.commands.at(-1)).toMatch(/^COMMIT [0-9a-f]+$/);
  });

  it("dispatches local alarms scheduled through storage", async () => {
    const namespace = new DurableObjectNamespace(AlarmObject, { transport: new ScriptedTransport() });
    const id = namespace.idFromName("alarm");
    const stub = namespace.get(id);

    await stub.schedule(1);
    await new Promise((resolve) => setTimeout(resolve, 15));
    await expect(stub.fetch("https://worker.test/")).resolves.toMatchObject({});
    expect(await (await stub.fetch("https://worker.test/")).text()).toBe("1");
    expect(await namespace.get(id).fetch("https://worker.test/")).toBeInstanceOf(Response);
  });

  it("rejects storage without an engine endpoint or injected transport", async () => {
    const namespace = new DurableObjectNamespace(Counter);
    const stub = namespace.get(namespace.idFromName("missing"));

    await expect(stub.increment()).rejects.toThrow(/engine endpoint|test transport|in-memory fallback|canonical TransactionEnvelope codec/i);
  });
});
