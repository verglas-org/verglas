import { describe, expect, it, vi } from "vitest";
import {
  connect,
  type NamespaceMethod,
} from "../src/index";

type ExampleNamespaces = {
  crm: {
    contacts: {
      get: NamespaceMethod<{ id: string }, { id: string; name: string }>;
      watch: NamespaceMethod<{ team: string }, { id: string }, "stream">;
    };
  };
};

const manifest = {
  namespace: "crm",
  title: "Example CRM",
  description: "A reflected test integration.",
  methods: {
    "contacts.get": {
      description: "Gets one contact.",
      mode: "read",
      input: { type: "object" },
      output: { type: "object" },
    },
    "contacts.watch": {
      description: "Streams changed contacts.",
      mode: "stream",
      input: { type: "object" },
      output: { type: "object" },
    },
  },
} as const;

describe("reflected Integration namespaces", () => {
  it("discovers namespace manifests through the Verglas endpoint", async () => {
    const fetcher = vi.fn<typeof fetch>().mockResolvedValue(Response.json([manifest]));
    const client = connect({ endpoint: "http://verglas.test", token: "scoped", fetch: fetcher });

    await expect(client.reflect()).resolves.toEqual([manifest]);
    expect(fetcher).toHaveBeenCalledWith(
      "http://verglas.test/v1/namespaces",
      expect.objectContaining({ headers: expect.objectContaining({ authorization: "Bearer scoped" }) }),
    );
  });

  it("invokes a reflected method through a composable typed namespace", async () => {
    const fetcher = vi.fn<typeof fetch>()
      .mockResolvedValueOnce(Response.json(manifest))
      .mockResolvedValueOnce(Response.json({ id: "c-1", name: "Ada" }));
    const client = connect<ExampleNamespaces>({
      endpoint: "http://verglas.test/",
      token: "scoped",
      fetch: fetcher,
    });

    const contact: { id: string; name: string } = await client.namespace.crm.contacts.get({ id: "c-1" });

    expect(contact).toEqual({ id: "c-1", name: "Ada" });
    expect(fetcher.mock.calls[1]?.[0]).toBe(
      "http://verglas.test/v1/namespaces/crm/invoke/contacts.get",
    );
    expect(JSON.parse(String(fetcher.mock.calls[1]?.[1]?.body))).toEqual({ id: "c-1" });
  });

  it("rejects methods absent from reflection before sending an invocation", async () => {
    const fetcher = vi.fn<typeof fetch>().mockResolvedValue(Response.json(manifest));
    const client = connect({ endpoint: "http://verglas.test", token: "scoped", fetch: fetcher });

    await expect(client.namespace.crm.contacts.remove({ id: "c-1" })).rejects.toThrow(
      "namespace crm does not declare method contacts.remove",
    );
    expect(fetcher).toHaveBeenCalledTimes(1);
  });

  it("streams newline-delimited results from a reflected stream method", async () => {
    const fetcher = vi.fn<typeof fetch>()
      .mockResolvedValueOnce(Response.json(manifest))
      .mockResolvedValueOnce(new Response('{"id":"c-1"}\n{"id":"c-2"}\n', {
        headers: { "content-type": "application/x-ndjson" },
      }));
    const client = connect<ExampleNamespaces>({
      endpoint: "http://verglas.test",
      token: "scoped",
      fetch: fetcher,
    });

    const rows: Array<{ id: string }> = [];
    for await (const row of client.namespace.crm.contacts.watch({ team: "platform" })) rows.push(row);

    expect(rows).toEqual([{ id: "c-1" }, { id: "c-2" }]);
    expect(fetcher.mock.calls[1]?.[1]).toEqual(expect.objectContaining({
      method: "POST",
      body: JSON.stringify({ team: "platform" }),
    }));
  });

  it("caches one namespace manifest across calls", async () => {
    const fetcher = vi.fn<typeof fetch>()
      .mockResolvedValueOnce(Response.json(manifest))
      .mockResolvedValueOnce(Response.json({ id: "c-1", name: "Ada" }))
      .mockResolvedValueOnce(Response.json({ id: "c-2", name: "Grace" }));
    const client = connect<ExampleNamespaces>({
      endpoint: "http://verglas.test",
      token: "scoped",
      fetch: fetcher,
    });

    await client.namespace.crm.contacts.get({ id: "c-1" });
    await client.namespace.crm.contacts.get({ id: "c-2" });

    expect(fetcher).toHaveBeenCalledTimes(3);
  });
});
