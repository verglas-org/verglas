import assert from "node:assert/strict";
import test from "node:test";

import {describeIntegration, invokeIntegration} from "./contract.mjs";

const api = {
  namespace: "crm",
  title: "CRM",
  description: "Customer records.",
  methods: {
    "contacts.get": {
      description: "Gets a contact.",
      mode: "read",
      input: {type: "object"},
      output: {type: "object"},
      async handler(input) {
        return {id: input.id, endpoint: this.verglas.endpoint};
      },
    },
  },
};

test("publishes handlers as a data-only reflection manifest", () => {
  assert.deepEqual(describeIntegration({api}, "crm"), {
    namespace: "crm",
    title: "CRM",
    description: "Customer records.",
    methods: {
      "contacts.get": {
        description: "Gets a contact.",
        mode: "read",
        input: {type: "object"},
        output: {type: "object"},
      },
    },
  });
});

test("invokes a declared method with the SDK on this.verglas", async () => {
  const verglas = {endpoint: "http://verglas.test"};
  const result = await invokeIntegration({api}, "contacts.get", {id: "c-1"}, {verglas});
  assert.deepEqual(result, {id: "c-1", endpoint: "http://verglas.test"});
});

test("rejects undeclared methods", async () => {
  await assert.rejects(
    invokeIntegration({api}, "contacts.remove", {}, {verglas: {}}),
    /does not declare method contacts\.remove/,
  );
});
