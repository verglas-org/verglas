import assert from "node:assert/strict";
import test from "node:test";
import { parseRuntimeOutput } from "./model-runtime-response.mjs";

test("parseRuntimeOutput rejects responses that would silently finish a chat", () => {
  assert.equal(parseRuntimeOutput(""), null);
  assert.equal(parseRuntimeOutput('{"content":null,"tool_calls":[]}'), null);
  assert.equal(parseRuntimeOutput('{"content":"","tool_calls":[]}'), null);
});

test("parseRuntimeOutput retains valid text and tool calls", () => {
  assert.deepEqual(parseRuntimeOutput('{"content":" ready ","tool_calls":[]}'), {
    content: "ready",
    tool_calls: [],
  });
  assert.deepEqual(parseRuntimeOutput(
    '{"content":null,"tool_calls":[{"name":"query","arguments":{"sql":"select 1"}}]}',
  ), {
    content: null,
    tool_calls: [{ name: "query", arguments: { sql: "select 1" } }],
  });
});

test("parseRuntimeOutput parses structured CLI tool arguments", () => {
  assert.deepEqual(parseRuntimeOutput(JSON.stringify({
    result: {
      content: null,
      tool_calls: [{
        name: "writeFile",
        argumentsJson: '{"filename":"client.ts","content":"const x = \\\"quoted\\\";\\n"}',
      }],
    },
  })), {
    content: null,
    tool_calls: [{
      name: "writeFile",
      arguments: { filename: "client.ts", content: 'const x = "quoted";\n' },
    }],
  });
});

test("parseRuntimeOutput extracts Claude structured output from event arrays", () => {
  assert.deepEqual(parseRuntimeOutput(JSON.stringify([
    { type: "system", subtype: "init" },
    {
      type: "result",
      result: "ignored text wrapper",
      structured_output: {
        content: null,
        tool_calls: [{ name: "echo", argumentsJson: '{"value":"READY"}' }],
      },
    },
  ])), {
    content: null,
    tool_calls: [{ name: "echo", arguments: { value: "READY" } }],
  });
});

test("parseRuntimeOutput recovers Cursor result strings with trailing junk", () => {
  const inner = JSON.stringify({
    content: null,
    tool_calls: [{ name: "createVessel", arguments: { name: "tracker" } }],
  });
  assert.deepEqual(parseRuntimeOutput(JSON.stringify({
    type: "result",
    subtype: "success",
    result: `${inner}]}`,
  })), {
    content: null,
    tool_calls: [{ name: "createVessel", arguments: { name: "tracker" } }],
  });
});

test("parseRuntimeOutput rejects truncated structured payloads instead of finishing as text", () => {
  assert.equal(
    parseRuntimeOutput('{"content":null,"tool_calls":[{"name":"createVessel"'),
    null,
  );
});

test("parseRuntimeOutput still accepts legacy kind/calls envelopes", () => {
  assert.deepEqual(parseRuntimeOutput(
    '{"kind":"tool_calls","calls":[{"name":"query","arguments":{"sql":"select 1"}}]}',
  ), {
    content: null,
    tool_calls: [{ name: "query", arguments: { sql: "select 1" } }],
  });
});
