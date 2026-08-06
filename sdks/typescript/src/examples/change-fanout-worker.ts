// Reference worker that reacts to Iceberg snapshot CloudEvents on an input table
// and writes derived rows to the deployment-configured output table. Stands in
// for any "on new data, transform it" pipeline — the streaming transform that
// used to be a materialized view, now a worker whose trigger is a table commit.
//
// An Iceberg snapshot event invokes the worker with the commit data. The worker
// delta-reads the newly committed rows
// (a short request that wakes the sleeping backend), transforms them, and appends
// the results. Dedupe across a replayed commit is the idempotency key: the same
// commit produces the same output under the same key, so a re-delivery is a free
// replay, not a double write.

import { defineWorker, type Row, type WorkerContext } from "../index";

export interface FanoutEnv {
  /** The input table whose commits invoke this worker. */
  INPUT_TABLE: string;
  /** Watermark to delta-read from, when the platform threads one in. Optional. */
  SINCE_WATERMARK?: string;
}

/** An Iceberg-event worker that doubles a `v` column and appends the result. */
export const changeFanoutWorker = defineWorker<FanoutEnv>({
  name: "change-fanout",
  triggers: [{
    type: "event",
    eventType: "org.apache.iceberg.snapshot.committed",
    subject: "app.input",
  }],
  async handler(ctx: WorkerContext<FanoutEnv>) {
    if (ctx.trigger.type !== "org.apache.iceberg.snapshot.committed") {
      throw new Error("change-fanout: expected an Iceberg snapshot CloudEvent");
    }
    const changed = ctx.trigger.data as { snapshotId?: string } | undefined;
    if (!changed?.snapshotId) throw new Error("change-fanout: snapshotId is required");

    // Read the rows this commit added. `SINCE_WATERMARK` (deployment config) marks
    // the last processed position; absent, we read the whole current snapshot.
    const input = ctx.verglas.table(ctx.env.INPUT_TABLE);
    const since = ctx.env.SINCE_WATERMARK;
    const rows = since ? (await input.delta(since)).rows : (await input.scan()).rows;

    const out: Row[] = rows
      .filter((r) => r.v !== null && r.v !== undefined)
      .map((r) => ({ v: Number(r.v) * 2, from_snapshot: changed.snapshotId }));
    if (out.length === 0) return { rowsWritten: 0 };

    // Key the commit by the input snapshot: a replayed CloudEvent re-commits the
    // same derived rows under the same key, so the output is never doubled.
    const result = await ctx.verglas
      .table(ctx.output)
      .append(out, { idempotencyKey: `${ctx.env.INPUT_TABLE}@${changed.snapshotId}` });
    ctx.log("change-fanout: appended", { rows: result.rowsCommitted, idempotent: result.idempotent });
    return { rowsWritten: result.rowsCommitted };
  },
});

export default changeFanoutWorker;
