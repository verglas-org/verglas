import assert from "node:assert/strict";
import test from "node:test";

import {
  compactGraphContext,
  projectGraphNamespace,
  projectRootFromHookInput,
} from "../src/project-graph.mjs";

test("project graphs are namespaced from the workspace root, not a global memory graph", () => {
  assert.equal(projectGraphNamespace("/Users/jfbrown/code/verglas"), "rime_verglas");
  assert.equal(projectGraphNamespace("/Users/jfbrown/code/rlean"), "rime_rlean");
  assert.equal(projectGraphNamespace("/tmp/My-App"), "rime_my_app");
  assert.equal(projectGraphNamespace("verglas-client"), "rime_verglas_client");
  assert.notEqual(projectGraphNamespace("/code/verglas"), "agent_memory");
});

test("project graph names stay Iceberg-safe", () => {
  assert.equal(projectGraphNamespace(""), "rime_project");
  assert.equal(projectGraphNamespace("/"), "rime_project");
  assert.equal(projectGraphNamespace("___"), "rime_project");
  assert.match(projectGraphNamespace("A".repeat(80)), /^rime_[a-z0-9_]{1,58}$/);
  assert.ok(projectGraphNamespace("A".repeat(80)).length <= 63);
});

test("hook input prefers the first workspace root over cwd", () => {
  assert.equal(
    projectRootFromHookInput({
      workspace_roots: ["/Users/jfbrown/code/verglas", "/tmp/other"],
      cwd: "/tmp/cwd",
    }),
    "/Users/jfbrown/code/verglas",
  );
  assert.equal(
    projectRootFromHookInput({
      workspaceRoots: ["/Users/jfbrown/code/rlean"],
    }),
    "/Users/jfbrown/code/rlean",
  );
  assert.equal(projectRootFromHookInput({ cwd: "/tmp/My-App" }), "/tmp/My-App");
  assert.equal(projectRootFromHookInput({}), undefined);
});

test("compact graph context is metadata only", () => {
  const context = compactGraphContext({
    namespace: "rime_verglas",
    show: {
      namespace: "rime_verglas",
      nodesTable: "rime_verglas.nodes",
      edgesTable: "rime_verglas.edges",
      nodeCount: 4,
      edgeCount: 3,
      indexed: false,
      snapshotId: 99,
    },
  });
  assert.match(context, /rime_verglas/);
  assert.match(context, /nodeCount":4/);
  assert.doesNotMatch(context, /prompt body|diff|evaluator output/i);
});
