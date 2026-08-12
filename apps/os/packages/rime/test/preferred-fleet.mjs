import { RimeSearch } from "../src/index.mjs";

/** Wraps a test proposal operator in the same low-cost preference as a host adapter. */
export function preferredFleet(
  execute,
  { agent = "test", model = "fast" } = {},
) {
  return {
    assign: async () => ({ agent, model }),
    execute: async (request) => ({
      ...(await execute(request)),
      execution: request.assignment,
    }),
  };
}

/** Preserves concise search fixtures while exercising the governed fleet boundary. */
export function createSearch(options) {
  const { propose, ...config } = options;
  return new RimeSearch({ ...config, fleet: preferredFleet(propose) });
}
