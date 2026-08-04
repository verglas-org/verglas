// Reference worker: a webhook worker that lands each inbound request's JSON body
// as a row in the deployment-configured output table. Stands in for any "receive
// an event, record it" ingress — a vendor webhook, a form post, an app callback.
//
// The trigger is a webhook: the platform routes matching requests to the worker
// and returns whatever the worker writes back. The long-lived listener is the
// platform's; the worker is invoked per request and holds nothing between calls.

import { defineWorker, type Row, type WorkerContext } from "../index";

export interface WebhookEnv {
  /** Optional shared secret the sender must present in `x-webhook-token`. */
  WEBHOOK_TOKEN?: string;
}

/** A webhook worker recording each request body as a row. */
export const webhookWorker = defineWorker<WebhookEnv>({
  name: "webhook-ingest",
  triggers: [{ type: "webhook", path: "/ingest" }],
  secrets: ["WEBHOOK_TOKEN"],
  async handler(ctx: WorkerContext<WebhookEnv>) {
    if (ctx.trigger.type !== "webhook") throw new Error("webhook-worker: expected a webhook trigger");
    const request = ctx.trigger.request;

    if (ctx.env.WEBHOOK_TOKEN && request.headers.get("x-webhook-token") !== ctx.env.WEBHOOK_TOKEN) {
      throw new Error("webhook-worker: bad or missing x-webhook-token");
    }

    const body = (await request.json().catch(() => null)) as Row | null;
    if (!body || typeof body !== "object") throw new Error("webhook-worker: body was not a JSON object");

    const row: Row = { ...body, received_at: new Date().toISOString() };
    const result = await ctx.client.table(ctx.output).append([row]);
    ctx.log("webhook-worker: recorded", { rows: result.rowsCommitted });
    return { rowsWritten: result.rowsCommitted };
  },
});

export default webhookWorker;
