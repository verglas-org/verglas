import assert from "node:assert/strict";
import test from "node:test";

import { RimeSearch, RimeStateStore } from "../src/index.mjs";

const evidence = (kind, name) => ({
  kind,
  artifact: `artifact:${name}`,
  summary: `${name} independently verified`,
});
const valid = (task) => ({
  status: "valid",
  gates: {
    taskCorrect: {
      passed: true,
      evaluator: "acceptance-harness",
      evidence: [evidence("test", "task")],
    },
    noDeadCode: {
      passed: true,
      evaluator: "diff-analyzer",
      evidence: [evidence("diff-analysis", "dead-code")],
    },
    noFallbacks: {
      passed: true,
      evaluator: "diff-analyzer",
      evidence: [evidence("diff-analysis", "fallback")],
    },
    architecturalFit: {
      passed: true,
      evaluator: "architecture-review",
      evidence: [evidence("architecture-review", "architecture")],
    },
    semanticRigor: {
      passed: true,
      evaluator: "semantic-check",
      evidence: [evidence("typecheck", "semantics")],
    },
  },
  metrics: {
    task: {
      value: task,
      evaluator: "acceptance-harness",
      evidence: [evidence("test", "task")],
    },
    architecture: {
      value: 1,
      evaluator: "architecture-review",
      evidence: [evidence("architecture-review", "architecture")],
    },
    changeSurface: {
      value: 0,
      evaluator: "diff-analyzer",
      evidence: [evidence("diff-analysis", "change-surface")],
    },
  },
});
const baseline = {
  solution: "baseline",
  summary: "baseline",
  evaluation: { status: "buggy", error: "reproduced" },
};

function config(overrides = {}) {
  return {
    initialDrafts: 2,
    concurrency: 2,
    candidateBudget: 2,
    maxDebugDepth: 1,
    debugProbability: 0,
    evaluate: async () => valid(1),
    ...overrides,
  };
}

test("prefers low-cost workers and records hosted and open-source identities", async () => {
  const requests = [];
  const store = new RimeStateStore({ runId: "low-cost-workers" });
  const identities = [
    { agent: "claude", model: "sonnet" },
    { agent: "vllm", model: "qwen3-coder-30b-a3b-q4" },
  ];
  const fleet = {
    assign: async (request) => {
      requests.push(request);
      return identities[request.ordinal];
    },
    execute: async ({ assignment, ordinal }) => ({
      solution: `candidate-${ordinal}`,
      summary: `candidate ${ordinal}`,
      execution: assignment,
      runtime: { startedAt: 10, finishedAt: 20, tokens: 40, cost: 0.01 },
    }),
  };

  const result = await new RimeSearch(config({ fleet, stateStore: store })).run(
    baseline,
  );

  assert.deepEqual(
    requests.map(({ preferences }) => preferences),
    [["low-cost"], ["low-cost"]],
  );
  assert.deepEqual(
    result.state.attemptMatrix.map((attempt) => ({
      preferences: attempt.preferences,
      agent: attempt.agent,
      model: attempt.model,
      executedAgent: attempt.executedAgent,
      executedModel: attempt.executedModel,
      assignmentMatched: attempt.assignmentMatched,
    })),
    identities.map(({ agent, model }) => ({
      preferences: ["low-cost"],
      agent,
      model,
      executedAgent: agent,
      executedModel: model,
      assignmentMatched: true,
    })),
  );
});

test("accepts an expensive assignment without blocking workspace allocation", async () => {
  let allocations = 0;
  const fleet = {
    assign: async () => ({ agent: "host", model: "frontier-expensive" }),
    execute: async ({ assignment }) => ({
      solution: "candidate",
      summary: "candidate",
      execution: assignment,
    }),
  };
  const engine = new RimeSearch(
    config({
      candidateBudget: 1,
      concurrency: 1,
      initialDrafts: 1,
      fleet,
      workspacePool: {
        create: async () => {
          allocations += 1;
          return { id: "workspace-one" };
        },
        promote: async () => ({ promoted: true }),
        remove: async () => {},
      },
    }),
  );

  const result = await engine.run(baseline);
  assert.equal(allocations, 1);
  assert.equal(result.best.solution, "candidate");
});

test("records an execution identity mismatch without rejecting the candidate", async () => {
  const store = new RimeStateStore({ runId: "model-mismatch" });
  const engine = new RimeSearch(
    config({
      candidateBudget: 1,
      concurrency: 1,
      initialDrafts: 1,
      stateStore: store,
      fleet: {
        assign: async () => ({ agent: "router", model: "low-cost-auto" }),
        execute: async () => ({
          solution: "candidate",
          summary: "candidate",
          execution: { agent: "ollama", model: "llama-3.3-70b-q4" },
        }),
      },
    }),
  );

  const result = await engine.run(baseline);
  assert.equal(result.best.solution, "candidate");
  assert.deepEqual(
    {
      executedAgent: result.state.attemptMatrix[0].executedAgent,
      executedModel: result.state.attemptMatrix[0].executedModel,
      assignmentMatched: result.state.attemptMatrix[0].assignmentMatched,
    },
    {
      executedAgent: "ollama",
      executedModel: "llama-3.3-70b-q4",
      assignmentMatched: false,
    },
  );
});

test("requires a fleet boundary but does not prescribe its model taxonomy", () => {
  assert.throws(
    () =>
      new RimeSearch(
        config({
          propose: async () => ({ solution: "x", summary: "x" }),
        }),
      ),
    /fleet/i,
  );
});
