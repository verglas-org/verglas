import assert from "node:assert/strict";
import test from "node:test";

import { CODING_GATE_DEFINITIONS, RimeStateStore } from "../src/index.mjs";
import { createSearch } from "./preferred-fleet.mjs";

const evidence = (kind, name) => ({
  kind,
  artifact: `artifact:${name}`,
  summary: `${name} independently verified`,
});
const gates = Object.fromEntries(
  Object.entries(CODING_GATE_DEFINITIONS).map(([name, definition]) => [
    name,
    {
      passed: true,
      evaluator: `${name}-evaluator`,
      evidence: [evidence(definition.evidenceKinds[0], name)],
    },
  ]),
);
const measurement = (value, kind, name) => ({
  value,
  evaluator: `${name}-evaluator`,
  evidence: [evidence(kind, name)],
});
const valid = (task) => ({
  status: "valid",
  gates,
  metrics: {
    task: measurement(task, "test", "task"),
    architecture: measurement(1, "architecture-review", "architecture"),
    changeSurface: measurement(0, "diff-analysis", "change-surface"),
  },
});
const buggy = (error = "failed") => ({ status: "buggy", error });

function pool(events, overrides = {}) {
  return {
    create: async ({ attemptId }) => {
      const workspace = { id: `workspace-${attemptId}` };
      events.push(["create", workspace.id]);
      return workspace;
    },
    promote: async ({ workspace, candidate }) => {
      events.push(["promote", workspace.id, candidate.id]);
      return { commit: `promoted-${candidate.id}` };
    },
    remove: async ({ workspace, reason }) => {
      events.push(["remove", workspace.id, reason]);
    },
    ...overrides,
  };
}

test("prunes losing workspaces, promotes the winner, then removes it", async () => {
  const events = [];
  const scores = [0.2, 0.9];
  const stateStore = new RimeStateStore({ runId: "workspace-cleanup" });
  const engine = createSearch({
    initialDrafts: 2,
    concurrency: 2,
    candidateBudget: 2,
    maxDebugDepth: 1,
    debugProbability: 0,
    workspacePool: pool(events),
    stateStore,
    propose: async ({ ordinal, workspace }) => ({
      solution: `solution-${ordinal}`,
      summary: `candidate ${ordinal}`,
      runtime: { workspaceObserved: workspace.id },
    }),
    evaluate: async () => valid(scores.shift()),
  });

  const result = await engine.run({
    solution: "baseline",
    summary: "baseline",
    evaluation: buggy("reproduced"),
  });

  assert.deepEqual(events, [
    ["create", "workspace-attempt-0-0"],
    ["create", "workspace-attempt-0-1"],
    ["remove", "workspace-attempt-0-0", "pruned"],
    ["promote", "workspace-attempt-0-1", "candidate-2"],
    ["remove", "workspace-attempt-0-1", "promoted"],
  ]);
  assert.deepEqual(result.promotion, { commit: "promoted-candidate-2" });
  assert.deepEqual(
    result.state.attemptMatrix.map(({ workspaceState }) => workspaceState),
    ["removed", "removed"],
  );
});

test("keeps a buggy workspace until its debug children have forked", async () => {
  const events = [];
  const evaluations = [buggy("draft failed"), valid(1)];
  const engine = createSearch({
    initialDrafts: 1,
    concurrency: 1,
    candidateBudget: 2,
    maxDebugDepth: 1,
    debugProbability: 1,
    random: () => 0,
    workspacePool: pool(events),
    propose: async ({ operation, workspace }) => ({
      solution: `${operation}-${workspace.id}`,
      summary: operation,
    }),
    evaluate: async () => evaluations.shift(),
  });

  await engine.run({
    solution: "baseline",
    summary: "baseline",
    evaluation: buggy("reproduced"),
  });

  assert.ok(
    events.findIndex(
      (event) => event[0] === "create" && event[1] === "workspace-attempt-1-0",
    ) <
      events.findIndex(
        (event) =>
          event[0] === "remove" && event[1] === "workspace-attempt-0-0",
      ),
  );
});

test("search failure attempts cleanup for every allocated workspace", async () => {
  const events = [];
  const workspacePool = pool(events, {
    remove: async ({ workspace, reason }) => {
      events.push(["remove", workspace.id, reason]);
      if (workspace.id.endsWith("0")) throw new Error("remove zero failed");
    },
  });
  const engine = createSearch({
    initialDrafts: 2,
    concurrency: 2,
    candidateBudget: 2,
    maxDebugDepth: 1,
    debugProbability: 0,
    workspacePool,
    propose: async ({ attemptId }) => {
      if (attemptId.endsWith("1")) throw new Error("agent crashed");
      return { solution: "candidate", summary: "candidate" };
    },
    evaluate: async () => valid(1),
  });

  await assert.rejects(
    engine.run({
      solution: "baseline",
      summary: "baseline",
      evaluation: buggy("reproduced"),
    }),
    (error) => {
      assert.equal(error instanceof AggregateError, true);
      assert.match(error.message, /search and workspace cleanup failed/i);
      assert.equal(error.errors.length, 2);
      return true;
    },
  );
  assert.deepEqual(
    events.filter(([operation]) => operation === "remove"),
    [
      ["remove", "workspace-attempt-0-0", "search-failed"],
      ["remove", "workspace-attempt-0-1", "search-failed"],
    ],
  );
});

test("promotion failure retains only the winning workspace for recovery", async () => {
  const events = [];
  const workspacePool = pool(events, {
    promote: async ({ workspace }) => {
      events.push(["promote", workspace.id]);
      throw new Error("promotion failed");
    },
  });
  const engine = createSearch({
    initialDrafts: 1,
    concurrency: 1,
    candidateBudget: 1,
    maxDebugDepth: 1,
    debugProbability: 0,
    workspacePool,
    propose: async () => ({ solution: "winner", summary: "winner" }),
    evaluate: async () => valid(1),
  });

  await assert.rejects(
    engine.run({
      solution: "baseline",
      summary: "baseline",
      evaluation: buggy("reproduced"),
    }),
    (error) => {
      assert.equal(error.retainedWorkspaceId, "workspace-attempt-0-0");
      return true;
    },
  );
  assert.deepEqual(events, [
    ["create", "workspace-attempt-0-0"],
    ["promote", "workspace-attempt-0-0"],
  ]);
});

test("cleans every speculative workspace when the baseline remains best", async () => {
  const events = [];
  const engine = createSearch({
    initialDrafts: 1,
    concurrency: 1,
    candidateBudget: 1,
    maxDebugDepth: 1,
    debugProbability: 0,
    workspacePool: pool(events),
    propose: async () => ({ solution: "worse", summary: "worse" }),
    evaluate: async () => valid(0),
  });

  const result = await engine.run({
    solution: "baseline",
    summary: "baseline",
    evaluation: valid(1),
  });

  assert.equal(result.best.operation, "root");
  assert.equal(result.promotion, null);
  assert.deepEqual(events, [
    ["create", "workspace-attempt-0-0"],
    ["remove", "workspace-attempt-0-0", "pruned"],
  ]);
});

test("cancellation after allocation cleans the attempt workspace", async () => {
  const events = [];
  const controller = new AbortController();
  const engine = createSearch({
    initialDrafts: 1,
    concurrency: 1,
    candidateBudget: 1,
    maxDebugDepth: 1,
    debugProbability: 0,
    signal: controller.signal,
    workspacePool: pool(events),
    propose: async () => {
      controller.abort(new Error("cancelled by caller"));
      return { solution: "unused", summary: "unused" };
    },
    evaluate: async () => valid(1),
  });

  await assert.rejects(
    engine.run({
      solution: "baseline",
      summary: "baseline",
      evaluation: buggy("reproduced"),
    }),
    /cancelled by caller/,
  );
  assert.deepEqual(events, [
    ["create", "workspace-attempt-0-0"],
    ["remove", "workspace-attempt-0-0", "search-failed"],
  ]);
});
