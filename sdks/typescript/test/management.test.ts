import { describe, expect, it } from "vitest";
import {
  WorkersManagementClient,
  WorkersManagementError,
  type WorkerScript,
} from "../src/management";

interface CapturedCall {
  input: RequestInfo | URL;
  init?: RequestInit;
}

function captureFetch(result: unknown = {}, status = 200): {
  calls: CapturedCall[];
  fetch: typeof fetch;
  next: (body: unknown, status?: number) => void;
} {
  const calls: CapturedCall[] = [];
  let responseBody = result;
  let responseStatus = status;
  const fetchImpl = (async (input: RequestInfo | URL, init?: RequestInit) => {
    calls.push({ input, init });
    return new Response(
      JSON.stringify({
        success: responseStatus >= 200 && responseStatus < 300,
        errors: responseStatus >= 300 ? [{ code: responseStatus, message: "nope" }] : [],
        messages: [],
        result: responseBody,
      }),
      { status: responseStatus, headers: { "content-type": "application/json" } },
    );
  }) as typeof fetch;
  return {
    calls,
    fetch: fetchImpl,
    next(body, status = 200) {
      responseBody = body;
      responseStatus = status;
    },
  };
}

function pathOf(call: CapturedCall): string {
  return new URL(String(call.input)).pathname;
}

describe("Workers management client", () => {
  it("uploads module-syntax multipart metadata and module parts", async () => {
    const captured = captureFetch({ id: "demo", name: "demo" } satisfies Partial<WorkerScript>);
    const client = new WorkersManagementClient("https://cell.test/", captured.fetch);

    await client.uploadScript("demo", {
      main_module: "worker.js",
      bindings: [{ name: "OBJECTS", type: "durable_object_namespace", class_name: "Counter" }],
      modules: { "worker.js": "export default {}" },
    });

    expect(captured.calls).toHaveLength(1);
    expect(captured.calls[0].init?.method).toBe("PUT");
    expect(pathOf(captured.calls[0])).toBe("/workers/scripts/demo");
    const form = captured.calls[0].init?.body as FormData;
    expect(form).toBeInstanceOf(FormData);
    const metadata = form.get("metadata");
    expect(metadata).toBeInstanceOf(Blob);
    expect(await (metadata as Blob).text()).toBe(
      JSON.stringify({
        main_module: "worker.js",
        bindings: [{ name: "OBJECTS", type: "durable_object_namespace", class_name: "Counter" }],
      }),
    );
    const module = form.get("worker.js");
    expect(module).toBeInstanceOf(File);
    expect((module as File).name).toBe("worker.js");
    expect(await (module as File).text()).toBe("export default {}");
  });

  it("unwraps envelopes and uses account-prefix-free management routes", async () => {
    const captured = captureFetch({ id: "x" });
    const client = new WorkersManagementClient("https://cell.test", captured.fetch);

    await client.listScripts();
    await client.getScript("demo");
    await client.deleteScript("demo");

    expect(captured.calls.map((call) => `${call.init?.method ?? "GET"} ${pathOf(call)}`)).toEqual([
      "GET /workers/scripts",
      "GET /workers/scripts/demo",
      "DELETE /workers/scripts/demo",
    ]);
    expect(await new Response(JSON.stringify({ result: "unused" })).json()).toEqual({ result: "unused" });
  });

  it("throws a typed error carrying the CF errors array", async () => {
    const captured = captureFetch({}, 400);
    const client = new WorkersManagementClient("https://cell.test", captured.fetch);

    await expect(client.getScript("missing")).rejects.toBeInstanceOf(WorkersManagementError);
    await expect(client.getScript("missing")).rejects.toMatchObject({
      errors: [{ code: 400, message: "nope" }],
      status: 400,
    });
  });
});
