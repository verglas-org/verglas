import assert from "node:assert/strict";
import test from "node:test";

import {
  CODING_GATE_DEFINITIONS,
  createCodingObjective,
  createEngineeringObjective,
} from "../src/index.mjs";
import { createSearch } from "./preferred-fleet.mjs";

const artifact = (kind, name) => ({
  kind,
  artifact: `artifact:${name}`,
  summary: `${name} measured independently`,
});

const codingGates = Object.fromEntries(
  Object.entries(CODING_GATE_DEFINITIONS).map(([name, definition]) => [
    name,
    {
      passed: true,
      evaluator: `${name}-evaluator`,
      evidence: [artifact(definition.evidenceKinds[0], name)],
    },
  ]),
);

const measurement = (value, kind, name) => ({
  value,
  evaluator: `${name}-evaluator`,
  evidence: [artifact(kind, name)],
});

test("combines minimized and maximized performance metrics across units", () => {
  const objective = createEngineeringObjective({
    metrics: [
      {
        name: "p95Latency",
        direction: "minimize",
        unit: "ms",
        weight: 0.5,
        scale: { type: "linear", worst: 200, best: 50 },
        evidenceKinds: ["benchmark"],
      },
      {
        name: "throughput",
        direction: "maximize",
        unit: "queries/s",
        weight: 0.3,
        scale: { type: "linear", worst: 100, best: 400 },
        evidenceKinds: ["benchmark"],
      },
      {
        name: "peakMemory",
        direction: "minimize",
        unit: "MiB",
        weight: 0.2,
        scale: { type: "linear", worst: 1024, best: 256 },
        evidenceKinds: ["benchmark"],
      },
    ],
  });

  const result = objective.normalize({
    status: "valid",
    gates: codingGates,
    metrics: {
      p95Latency: measurement(100, "benchmark", "latency"),
      throughput: measurement(250, "benchmark", "throughput"),
      peakMemory: measurement(512, "benchmark", "memory"),
    },
  });

  assert.equal(result.status, "valid");
  assert.equal(result.score, 0.616666666667);
  assert.deepEqual(result.metrics.p95Latency, {
    value: 100,
    unit: "ms",
    utility: 0.666666666667,
    evaluator: "latency-evaluator",
    evidence: [artifact("benchmark", "latency")],
  });
});

test("supports benchmark quality with hard latency and cost constraints", () => {
  const objective = createEngineeringObjective({
    gates: {
      heldOutQuality: {
        evidenceKinds: ["benchmark"],
      },
    },
    metrics: [
      {
        name: "retrievalRecallAt10",
        direction: "maximize",
        unit: "ratio",
        weight: 0.6,
        scale: { type: "linear", worst: 0.5, best: 1 },
        evidenceKinds: ["benchmark"],
      },
      {
        name: "answerFaithfulness",
        direction: "maximize",
        unit: "ratio",
        weight: 0.4,
        scale: { type: "linear", worst: 0.5, best: 1 },
        evidenceKinds: ["benchmark"],
      },
      {
        name: "p95Latency",
        direction: "minimize",
        unit: "ms",
        weight: 0,
        scale: { type: "linear", worst: 1000, best: 100 },
        evidenceKinds: ["benchmark"],
      },
      {
        name: "costPerQuery",
        direction: "minimize",
        unit: "USD",
        weight: 0,
        scale: { type: "linear", worst: 0.1, best: 0.001 },
        evidenceKinds: ["benchmark"],
      },
    ],
    constraints: [
      { metric: "p95Latency", operator: "<=", threshold: 500 },
      { metric: "costPerQuery", operator: "<=", threshold: 0.02 },
    ],
  });
  const gates = {
    ...codingGates,
    heldOutQuality: {
      passed: true,
      evaluator: "held-out-suite",
      evidence: [artifact("benchmark", "held-out-quality")],
    },
  };
  const metrics = {
    retrievalRecallAt10: measurement(0.9, "benchmark", "recall"),
    answerFaithfulness: measurement(0.8, "benchmark", "faithfulness"),
    p95Latency: measurement(550, "benchmark", "latency"),
    costPerQuery: measurement(0.01, "benchmark", "cost"),
  };

  const result = objective.normalize({ status: "valid", gates, metrics });

  assert.equal(result.status, "buggy");
  assert.deepEqual(result.failedConstraints, [
    { metric: "p95Latency", operator: "<=", threshold: 500, actual: 550 },
  ]);
  assert.equal(result.metrics.retrievalRecallAt10.value, 0.9);
});

test("clamps metric utility while retaining the raw measurement", () => {
  const objective = createEngineeringObjective({
    metrics: [
      {
        name: "latency",
        direction: "minimize",
        unit: "ms",
        weight: 1,
        scale: { type: "linear", worst: 200, best: 100 },
        evidenceKinds: ["benchmark"],
      },
    ],
  });
  const result = objective.normalize({
    status: "valid",
    gates: codingGates,
    metrics: { latency: measurement(50, "benchmark", "latency") },
  });

  assert.equal(result.metrics.latency.value, 50);
  assert.equal(result.metrics.latency.utility, 1);
  assert.equal(result.score, 1);
});

test("supports logarithmic scales for order-of-magnitude performance ranges", () => {
  const objective = createEngineeringObjective({
    metrics: [
      {
        name: "costPerMillion",
        direction: "minimize",
        unit: "USD",
        weight: 1,
        scale: { type: "log", worst: 100, best: 1 },
        evidenceKinds: ["benchmark"],
      },
    ],
  });
  const result = objective.normalize({
    status: "valid",
    gates: codingGates,
    metrics: {
      costPerMillion: measurement(10, "benchmark", "cost"),
    },
  });

  assert.equal(result.metrics.costPerMillion.utility, 0.5);
  assert.equal(result.score, 0.5);
});

test("rejects ambiguous metric definitions and unsupported measurements", () => {
  assert.throws(
    () =>
      createEngineeringObjective({
        metrics: [
          {
            name: "latency",
            direction: "maximize",
            unit: "ms",
            weight: 1,
            scale: { type: "linear", worst: 200, best: 100 },
            evidenceKinds: ["benchmark"],
          },
        ],
      }),
    /direction.*scale/,
  );
  assert.throws(
    () =>
      createEngineeringObjective({
        metrics: [
          {
            name: "latency",
            direction: "minimize",
            unit: "ms",
            weight: 1,
            scale: { type: "linear", worst: 200, best: 100 },
            evidenceKinds: ["benchmark"],
          },
          {
            name: "latency",
            direction: "minimize",
            unit: "ms",
            weight: 1,
            scale: { type: "linear", worst: 200, best: 100 },
            evidenceKinds: ["benchmark"],
          },
        ],
      }),
    /Duplicate metric/,
  );

  const objective = createEngineeringObjective({
    metrics: [
      {
        name: "latency",
        direction: "minimize",
        unit: "ms",
        weight: 1,
        scale: { type: "linear", worst: 200, best: 100 },
        evidenceKinds: ["benchmark"],
      },
    ],
  });
  assert.throws(
    () =>
      objective.normalize({
        status: "valid",
        gates: codingGates,
        metrics: {
          latency: measurement(Number.NaN, "benchmark", "latency"),
        },
      }),
    /latency.*finite/,
  );
  assert.throws(
    () =>
      objective.normalize({
        status: "valid",
        gates: codingGates,
        metrics: { latency: measurement(120, "test", "latency") },
      }),
    /latency.*test/,
  );
  assert.throws(
    () =>
      objective.normalize({
        status: "valid",
        gates: codingGates,
        metrics: {},
      }),
    /latency.*measurement/,
  );
  assert.throws(
    () =>
      objective.normalize({
        status: "valid",
        gates: codingGates,
        metrics: {
          latency: measurement(120, "benchmark", "latency"),
          guessedMetric: measurement(1, "benchmark", "guessed"),
        },
      }),
    /Unknown metric.*guessedMetric/,
  );
});

test("expresses the default coding reward through the general objective", () => {
  const objective = createCodingObjective();
  const result = objective.normalize({
    status: "valid",
    gates: codingGates,
    metrics: {
      task: measurement(0.8, "test", "task"),
      architecture: measurement(1, "architecture-review", "architecture"),
      changeSurface: measurement(0, "diff-analysis", "change-surface"),
    },
  });

  assert.equal(result.status, "valid");
  assert.equal(result.score, 0.872727272727);
  assert.deepEqual(Object.keys(result.metrics), [
    "task",
    "architecture",
    "changeSurface",
  ]);
});

test("RIME selects candidates with an injected performance objective", async () => {
  const objective = createEngineeringObjective({
    metrics: [
      {
        name: "p95Latency",
        direction: "minimize",
        unit: "ms",
        weight: 1,
        scale: { type: "linear", worst: 200, best: 50 },
        evidenceKinds: ["benchmark"],
      },
    ],
  });
  const latencies = [140, 80];
  const engine = createSearch({
    initialDrafts: 2,
    concurrency: 2,
    candidateBudget: 2,
    maxDebugDepth: 0,
    debugProbability: 0,
    objective,
    propose: async ({ ordinal }) => ({
      solution: `query-plan-${ordinal}`,
      summary: `query plan ${ordinal}`,
    }),
    evaluate: async () => ({
      status: "valid",
      gates: codingGates,
      metrics: {
        p95Latency: measurement(latencies.shift(), "benchmark", "latency"),
      },
    }),
  });

  const result = await engine.run({
    solution: "baseline",
    summary: "baseline query plan",
    evaluation: {
      status: "valid",
      gates: codingGates,
      metrics: {
        p95Latency: measurement(180, "benchmark", "baseline-latency"),
      },
    },
  });

  assert.equal(result.best.solution, "query-plan-2");
  assert.equal(result.best.evaluation.metrics.p95Latency.value, 80);
});
