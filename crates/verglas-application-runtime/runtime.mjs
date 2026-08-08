import {connect} from "./sdk/client.ts";
import {invokeApplication} from "./contract.mjs";

const port = Number.parseInt(process.env.VERGLAS_APPLICATION_PORT || "8380", 10);
const name = required("VERGLAS_APPLICATION_NAME");
const endpoint = required("VERGLAS_DATA_ENDPOINT").replace(/\/+$/, "");
const token = required("VERGLAS_DATA_TOKEN");
const source = Buffer.from(required("VERGLAS_APPLICATION_MODULE"), "base64").toString("utf8");
const verglas = connect({endpoint, token});
for (const key of ["VERGLAS_DATA_TOKEN", "VERGLAS_APPLICATION_MODULE"]) delete process.env[key];

const generated = await import(`data:text/javascript;base64,${Buffer.from(source).toString("base64")}`);
const application = generated.default;
const runtime = Object.freeze({name, verglas});

function required(key) {
  const value = process.env[key]?.trim();
  if (!value) throw new Error(`${key} is required`);
  return value;
}

Bun.serve({
  port,
  async fetch(request) {
    try {
      return await invokeApplication(application, request, runtime);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      return Response.json({error: message.slice(0, 512)}, {status: 500});
    }
  },
});
