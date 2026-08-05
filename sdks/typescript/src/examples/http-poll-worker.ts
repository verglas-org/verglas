// Reference worker: a cron worker that polls a JSON HTTP endpoint and appends the
// rows it returns to the deployment-configured output table. Domain-neutral — an
// agent adapts the URL, the row mapping, and the timestamp field for a concrete
// upstream.
//
// It ranges its query over the cron trigger's LOGICAL INTERVAL (the Airflow
// model): only rows whose timestamp falls in `[intervalStart, intervalEnd)` are
// kept, so each scheduled run — including a backfill run for a past interval —
// pulls exactly its own slice. There is no watermark: the interval IS the
// progress signal. When the trigger carries no interval (a manual dispatch), the
// worker keeps everything the endpoint returns.

import { defineWorker, type Row, type WorkerContext } from "../index";

export interface HttpPollEnv {
  /** URL returning a JSON array of records, or `{ [itemsKey]: [...] }`. */
  POLL_URL: string;
  /** Key holding the array when the body is an object (default: whole body). */
  POLL_ITEMS_KEY?: string;
  /** Row field carrying an ISO-8601 timestamp, used to range by the interval. */
  POLL_TIME_FIELD?: string;
  /** Optional bearer key the deployment injects as a secret binding. */
  POLL_KEY?: string;
}

/** Pulls the record array out of the response body. */
function extractItems(body: unknown, itemsKey?: string): Row[] {
  if (Array.isArray(body)) return body as Row[];
  if (itemsKey && body && typeof body === "object") {
    const arr = (body as Record<string, unknown>)[itemsKey];
    if (Array.isArray(arr)) return arr as Row[];
  }
  throw new Error("http-poll-worker: response was not an array; set POLL_ITEMS_KEY");
}

/** A cron worker polling `POLL_URL`, ranged by the trigger's logical interval. */
export const httpPollWorker = defineWorker<HttpPollEnv>({
  name: "http-poll",
  triggers: [{ type: "cron", schedule: "*/5 * * * *" }],
  secrets: ["POLL_KEY"],
  async handler(ctx: WorkerContext<HttpPollEnv>) {
    const headers = ctx.env.POLL_KEY ? { authorization: `Bearer ${ctx.env.POLL_KEY}` } : undefined;
    const resp = await fetch(ctx.env.POLL_URL, { headers, signal: ctx.signal });
    if (!resp.ok) throw new Error(`http-poll-worker: ${ctx.env.POLL_URL} returned HTTP ${resp.status}`);
    const items = extractItems(await resp.json(), ctx.env.POLL_ITEMS_KEY);

    // Range by the logical interval when the cron trigger carries one.
    const tf = ctx.env.POLL_TIME_FIELD;
    const cron = ctx.trigger.type === "org.verglas.schedule.tick"
      ? ctx.trigger.data as { intervalStart?: string; intervalEnd?: string } | undefined
      : undefined;
    const inRange = (r: Row): boolean => {
      if (!tf || !cron) return true;
      const t = String(r[tf] ?? "");
      if (cron.intervalStart && t < cron.intervalStart) return false;
      if (cron.intervalEnd && t >= cron.intervalEnd) return false;
      return true;
    };
    const fresh = items.filter(inRange);
    if (fresh.length === 0) {
      ctx.log("http-poll-worker: nothing in interval", { url: ctx.env.POLL_URL });
      return { rowsWritten: 0 };
    }

    const result = await ctx.client.table(ctx.output).append(fresh);
    ctx.log("http-poll-worker: appended", { rows: result.rowsCommitted, snapshot: result.snapshotId });
    return { rowsWritten: result.rowsCommitted };
  },
});

export default httpPollWorker;
