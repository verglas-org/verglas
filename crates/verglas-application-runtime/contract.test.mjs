import assert from "node:assert/strict";
import test from "node:test";

import {invokeApplication} from "./contract.mjs";

test("injects the connected SDK as this.verglas", async () => {
  const application = {
    async fetch(request) {
      return {path: new URL(request.url).pathname, endpoint: this.verglas.endpoint};
    },
  };
  const response = await invokeApplication(
    application,
    new Request("http://application.test/dashboard"),
    {verglas: {endpoint: "http://verglas.test"}},
  );
  assert.deepEqual(await response.json(), {
    path: "/dashboard",
    endpoint: "http://verglas.test",
  });
});
