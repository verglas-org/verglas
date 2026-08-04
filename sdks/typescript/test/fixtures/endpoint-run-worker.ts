// A minimal fleet entry module for the endpoint-run tests: default-exports a
// worker, the same convention a real entry uses. It reads ENDPOINT_RUN_THROW
// from the process environment the way a real entry reads its own secrets, and
// writes to the deployment-configured output table (`ctx.output`).

import { defineWorker, type WorkerContext } from "../../src/contracts";

export default defineWorker(async (ctx: WorkerContext) => {
  if (process.env.ENDPOINT_RUN_THROW) throw new Error("boom");
  const r = await ctx.client.table(ctx.output).append([{ n: 1 }, { n: 2 }]);
  ctx.log("fetched", { rows: r.rowsCommitted });
  return { rowsWritten: r.rowsCommitted };
});
