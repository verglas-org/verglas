import assert from "node:assert/strict";
import test from "node:test";

import {
  RimeStateStore,
  VerglasGraphStore,
  compileCandidateContext,
} from "../src/index.mjs";

const buggy = (error = "failed") => ({ status: "buggy", error });
const worker = {
  preferences: ["low-cost"],
  agent: "test",
  model: "fast",
};
const execution = { agent: "test", model: "fast" };
const observedRuntime = { execution, assignmentMatched: true };

const validEvaluation = {
  status: "valid",
  score: 0.9,
  metrics: {
    task: {
      value: 1,
      unit: "ratio",
      utility: 1,
      evaluator: "task-evaluator",
      evidence: [],
    },
    architecture: {
      value: 0.8,
      unit: "ratio",
      utility: 0.8,
      evaluator: "architecture-evaluator",
      evidence: [],
    },
    changeSurface: {
      value: 0.4,
      unit: "ratio",
      utility: 0.6,
      evaluator: "change-surface-evaluator",
      evidence: [],
    },
  },
  gates: {
    taskCorrect: {
      passed: true,
      evaluator: "acceptance-harness",
      evidence: [
        {
          kind: "test",
          artifact: "artifact:test-report",
          summary: "acceptance tests passed",
        },
      ],
    },
  },
};

const root = {
  id: "candidate-0",
  ordinal: 0,
  parentId: null,
  operation: "root",
  solution: { workspaceId: "workspace-baseline" },
  summary: "failure reproduced",
  evaluation: buggy("duplicate observed"),
  debugDepth: 0,
  expanded: false,
};

const candidate = {
  id: "candidate-1",
  ordinal: 1,
  parentId: "candidate-0",
  operation: "draft",
  solution: { workspaceId: "workspace-one" },
  summary: "centralize idempotency ownership",
  evaluation: validEvaluation,
  debugDepth: 0,
  expanded: false,
};

test("a candidate is invisible to policy snapshots until its wave closes", async () => {
  const store = new RimeStateStore({ runId: "run-1" });
  await store.initialize(root);
  await store.openWave({
    id: "wave-0",
    ordinal: 0,
    operation: "draft",
    parentId: "candidate-0",
    attempts: [
      {
        ...worker,
        id: "attempt-0",
        ordinal: 0,
        workspaceId: "workspace-one",
      },
    ],
  });
  await store.recordAttempt({
    waveId: "wave-0",
    attemptId: "attempt-0",
    candidate,
    runtime: {
      ...observedRuntime,
      startedAt: 10,
      finishedAt: 20,
      tokens: 120,
      cost: 0.02,
    },
  });

  assert.deepEqual(
    store.snapshot().candidateMatrix.map(({ candidateId }) => candidateId),
    ["candidate-0"],
  );
  assert.equal(store.snapshot().attemptMatrix[0].state, "completed");

  await store.closeWave("wave-0");

  assert.deepEqual(
    store.snapshot().candidateMatrix.map(({ candidateId }) => candidateId),
    ["candidate-0", "candidate-1"],
  );
  assert.equal(store.snapshot().waves[0].closed, true);
});

test("closed waves expose separate candidate and attempt matrices", async () => {
  const store = new RimeStateStore({ runId: "run-2" });
  await store.initialize(root);
  await store.openWave({
    id: "wave-0",
    ordinal: 0,
    operation: "draft",
    parentId: "candidate-0",
    attempts: [
      { ...worker, id: "attempt-0", ordinal: 0, workspaceId: "workspace-one" },
    ],
  });
  await store.recordAttempt({
    waveId: "wave-0",
    attemptId: "attempt-0",
    candidate,
    runtime: {
      ...observedRuntime,
      startedAt: 10,
      finishedAt: 20,
      tokens: 120,
      cost: 0.02,
    },
  });
  await store.closeWave("wave-0");

  const snapshot = store.snapshot();
  assert.deepEqual(snapshot.candidateMatrix[1], {
    candidateId: "candidate-1",
    parentId: "candidate-0",
    operation: "draft",
    status: "valid",
    debugDepth: 0,
    metrics: validEvaluation.metrics,
    score: 0.9,
    gates: { taskCorrect: true },
    childCount: 0,
    isLeaf: true,
    waveId: "wave-0",
    eligibility: "improve",
  });
  assert.deepEqual(snapshot.attemptMatrix[0], {
    attemptId: "attempt-0",
    waveId: "wave-0",
    parentCandidateId: "candidate-0",
    operation: "draft",
    preferences: ["low-cost"],
    agent: "test",
    model: "fast",
    executedAgent: "test",
    executedModel: "fast",
    assignmentMatched: true,
    state: "completed",
    startedAt: 10,
    finishedAt: 20,
    tokens: 120,
    cost: 0.02,
    workspaceId: "workspace-one",
    workspaceState: "active",
    candidateId: "candidate-1",
    failureReason: null,
    leaseGeneration: null,
  });
  assert.equal(Object.isFrozen(snapshot.candidateMatrix), true);
  assert.equal(Object.isFrozen(snapshot.attemptMatrix), true);
});

test("Verglas graph writes normalized evidence and closes the wave last", async () => {
  const calls = [];
  const graph = {
    create: async () => calls.push(["create"]),
    insertNodes: async (nodes) => calls.push(["nodes", nodes]),
    insertEdges: async (edges) => calls.push(["edges", edges]),
  };
  const store = new VerglasGraphStore({ graph, runId: "run-3" });
  await store.initialize(root);
  await store.openWave({
    id: "wave-0",
    ordinal: 0,
    operation: "draft",
    parentId: "candidate-0",
    attempts: [
      { ...worker, id: "attempt-0", ordinal: 0, workspaceId: "workspace-one" },
    ],
  });
  await store.recordAttempt({
    waveId: "wave-0",
    attemptId: "attempt-0",
    candidate,
    runtime: { ...observedRuntime, startedAt: 10, finishedAt: 20 },
  });
  await store.closeWave("wave-0");

  const nodes = calls
    .filter(([kind]) => kind === "nodes")
    .flatMap(([, values]) => values);
  const edges = calls
    .filter(([kind]) => kind === "edges")
    .flatMap(([, values]) => values);
  assert.deepEqual(
    nodes.map(({ labels }) => labels[0]),
    [
      "RimeRun",
      "RimeCandidate",
      "RimeEvaluation",
      "RimeWave",
      "RimeAttempt",
      "RimeWorkspace",
      "RimeCandidate",
      "RimeEvaluation",
      "RimeGateEvaluation",
      "RimeMetricMeasurement",
      "RimeMetricMeasurement",
      "RimeMetricMeasurement",
      "RimeArtifact",
      "RimeWaveClosed",
    ],
  );
  assert.equal(
    nodes.find(({ labels }) => labels[0] === "RimeCandidate").properties.gates,
    undefined,
  );
  assert.ok(edges.some(({ predicate }) => predicate === "has_evaluation"));
  assert.ok(edges.some(({ predicate }) => predicate === "has_gate"));
  assert.ok(edges.some(({ predicate }) => predicate === "supported_by"));
  assert.equal(edges.at(-1).predicate, "closed_by");
});

test("state snapshots hydrate without exposing an unfinished wave", async () => {
  const first = new RimeStateStore({ runId: "run-4" });
  await first.initialize(root);
  await first.openWave({
    id: "wave-0",
    ordinal: 0,
    operation: "draft",
    parentId: "candidate-0",
    attempts: [{ ...worker, id: "attempt-0", ordinal: 0 }],
  });
  await first.recordAttempt({
    waveId: "wave-0",
    attemptId: "attempt-0",
    candidate,
    runtime: observedRuntime,
  });

  const resumed = RimeStateStore.hydrate(first.checkpoint());
  assert.deepEqual(
    resumed.snapshot().candidateMatrix.map(({ candidateId }) => candidateId),
    ["candidate-0"],
  );
  assert.deepEqual(resumed.searchSnapshot().candidates, [root]);
});

test("rejects overlapping waves and invalid restart lineage", async () => {
  const store = new RimeStateStore({ runId: "run-validation" });
  await store.initialize(root);
  await store.openWave({
    id: "wave-0",
    ordinal: 0,
    operation: "draft",
    parentId: "candidate-0",
    attempts: [{ ...worker, id: "attempt-0", ordinal: 0 }],
  });

  await assert.rejects(
    store.openWave({
      id: "wave-1",
      ordinal: 1,
      operation: "draft",
      parentId: "candidate-0",
      attempts: [{ ...worker, id: "attempt-1", ordinal: 0 }],
    }),
    /another wave is open/,
  );

  const corrupt = store.checkpoint();
  corrupt.waves[0].parentId = "candidate-missing";
  assert.throws(() => RimeStateStore.hydrate(corrupt), /not visible/);
});

test("bounded context includes local outcomes but no solutions or raw artifacts", async () => {
  const store = new RimeStateStore({ runId: "run-5" });
  await store.initialize(root);
  await store.openWave({
    id: "wave-0",
    ordinal: 0,
    operation: "draft",
    parentId: "candidate-0",
    attempts: [{ ...worker, id: "attempt-0", ordinal: 0 }],
  });
  await store.recordAttempt({
    waveId: "wave-0",
    attemptId: "attempt-0",
    candidate,
    runtime: observedRuntime,
  });
  await store.closeWave("wave-0");

  const context = compileCandidateContext(store.checkpoint(), {
    candidateId: "candidate-1",
    maxCandidates: 2,
    maxArtifacts: 1,
  });
  assert.equal(context.candidates.length, 2);
  assert.deepEqual(context.artifacts, [
    {
      artifact: "artifact:test-report",
      kind: "test",
      summary: "acceptance tests passed",
    },
  ]);
  assert.equal(JSON.stringify(context).includes("workspace-baseline"), false);
  assert.equal(JSON.stringify(context).includes("solution"), false);
  assert.throws(
    () =>
      compileCandidateContext(store.checkpoint(), {
        candidateId: "candidate-1",
        maxCandidates: 0,
      }),
    /maxCandidates/,
  );
});

test("operator contexts expose only the evidence needed for their decision", async () => {
  const store = new RimeStateStore({ runId: "run-operator-context" });
  await store.initialize(root);
  await store.openWave({
    id: "wave-0",
    ordinal: 0,
    operation: "draft",
    parentId: "candidate-0",
    attempts: [
      { ...worker, id: "attempt-0", ordinal: 0 },
      { ...worker, id: "attempt-1", ordinal: 1 },
    ],
  });
  await store.recordAttempt({
    waveId: "wave-0",
    attemptId: "attempt-0",
    candidate,
    runtime: observedRuntime,
  });
  await store.recordAttempt({
    waveId: "wave-0",
    attemptId: "attempt-1",
    candidate: {
      ...candidate,
      id: "candidate-2",
      ordinal: 2,
      solution: { workspaceId: "workspace-two" },
      summary: "retain the duplicate path",
      evaluation: buggy("acceptance test failed"),
    },
    runtime: observedRuntime,
  });
  await store.closeWave("wave-0");

  const draft = compileCandidateContext(store.checkpoint(), {
    candidateId: "candidate-0",
    operation: "draft",
  });
  assert.deepEqual(
    draft.candidates.map(({ candidateId }) => candidateId),
    ["candidate-2", "candidate-1", "candidate-0"],
  );
  assert.deepEqual(draft.artifacts, []);
  assert.equal("plateau" in draft, false);

  const debug = compileCandidateContext(store.checkpoint(), {
    candidateId: "candidate-2",
    operation: "debug",
  });
  assert.deepEqual(
    debug.candidates.map(({ candidateId }) => candidateId),
    ["candidate-2", "candidate-0"],
  );
  assert.equal("plateau" in debug, false);

  const improve = compileCandidateContext(store.checkpoint(), {
    candidateId: "candidate-1",
    operation: "improve",
  });
  assert.deepEqual(improve.plateau, {
    scores: [0.9],
    stalled: false,
  });
});

test("operator context rejects an unknown operation", async () => {
  const store = new RimeStateStore({ runId: "run-context-operation" });
  await store.initialize(root);

  assert.throws(
    () =>
      compileCandidateContext(store.checkpoint(), {
        candidateId: "candidate-0",
        operation: "review",
      }),
    /context operation/,
  );
});
