import assert from "node:assert/strict";
import test from "node:test";

import { VerglasRecursiveGraphStore } from "../src/index.mjs";

const buggy = (error = "failed") => ({ status: "buggy", error });
const root = {
  id: "candidate-0",
  ordinal: 0,
  parentId: null,
  operation: "root",
  solution: { workspaceId: "inner-baseline" },
  summary: "inner baseline",
  evaluation: buggy("reproduced"),
  debugDepth: 0,
  expanded: false,
};

function recordingGraph() {
  const calls = [];
  return {
    calls,
    graph: {
      create: async () => calls.push(["create"]),
      insertNodes: async (nodes) => calls.push(["nodes", nodes]),
      insertEdges: async (edges) => calls.push(["edges", edges]),
    },
  };
}

const step = {
  id: "step-0",
  ordinal: 0,
  parentCandidateId: "outer-0",
  candidates: [
    {
      id: "outer-1",
      ordinal: 0,
      summary: "compress the improve prompt",
      adjustments: [
        {
          id: "prompt-adjustment-1",
          kind: "prompt",
          artifact: "artifact:prompt-diff-1",
          summary: "remove repeated trajectory bodies",
        },
        {
          id: "work-adjustment-1",
          kind: "code",
          artifact: "artifact:commit-1",
          summary: "add operator-specific context projection",
        },
      ],
    },
  ],
};

test("links outer prompt and work adjustments to fully tracked inner runs", async () => {
  const { graph, calls } = recordingGraph();
  const outer = new VerglasRecursiveGraphStore({
    graph,
    experimentId: "recursive-1",
  });
  await outer.initialize({
    summary: "improve RIME under a fixed budget",
    baseline: { id: "outer-0", summary: "current RIME harness" },
  });
  await outer.openStep(step);
  const inner = outer.innerRunStore({
    runId: "inner-1",
    stepId: "step-0",
    candidateId: "outer-1",
    task: { id: "task-1", family: "coding", split: "private" },
  });
  await inner.initialize(root);

  const nodes = calls
    .filter(([kind]) => kind === "nodes")
    .flatMap(([, values]) => values);
  const edges = calls
    .filter(([kind]) => kind === "edges")
    .flatMap(([, values]) => values);
  assert.ok(nodes.some(({ labels }) => labels[0] === "RimeRecursiveExperiment"));
  assert.ok(nodes.some(({ labels }) => labels[0] === "RimeOuterCandidate"));
  assert.ok(nodes.some(({ labels }) => labels[0] === "RimeHarnessAdjustment"));
  assert.ok(nodes.some(({ labels }) => labels[0] === "RimeInnerTask"));
  assert.ok(nodes.some(({ labels }) => labels[0] === "RimeRun"));
  assert.ok(edges.some(({ predicate }) => predicate === "adjusted_by"));
  assert.ok(
    edges.some(
      ({ predicate, dstId }) =>
        predicate === "evaluated_by" && dstId.includes("/Run/inner-1"),
    ),
  );
  assert.ok(edges.some(({ predicate }) => predicate === "runs_task"));
  assert.ok(edges.some(({ predicate }) => predicate === "belongs_to_experiment"));
  assert.equal(JSON.stringify(calls).includes("remove repeated trajectory bodies"), true);
  assert.equal(JSON.stringify(calls).includes("full prompt body"), false);
});

test("records outer held-out evidence and writes the selection fence last", async () => {
  const { graph, calls } = recordingGraph();
  const outer = new VerglasRecursiveGraphStore({
    graph,
    experimentId: "recursive-2",
  });
  await outer.initialize({
    summary: "improve RIME",
    baseline: { id: "outer-0", summary: "baseline harness" },
  });
  await outer.openStep(step);
  const inner = outer.innerRunStore({
    runId: "inner-2",
    stepId: "step-0",
    candidateId: "outer-1",
    task: { id: "task-private", family: "coding", split: "private" },
  });
  await inner.initialize(root);
  await outer.recordEvaluation({
    stepId: "step-0",
    candidateId: "outer-1",
    status: "valid",
    score: 0.82,
    cost: 12.5,
    metrics: {
      heldOutQuality: { value: 0.82, unit: "ratio" },
      promptTokens: { value: 2400, unit: "tokens" },
    },
    evidence: [
      {
        artifact: "artifact:private-eval-1",
        kind: "held-out-evaluation",
        summary: "private coding tasks under the fixed budget",
      },
    ],
  });
  await outer.closeStep({ stepId: "step-0", selectedCandidateId: "outer-1" });

  const nodes = calls
    .filter(([kind]) => kind === "nodes")
    .flatMap(([, values]) => values);
  const edges = calls
    .filter(([kind]) => kind === "edges")
    .flatMap(([, values]) => values);
  assert.ok(nodes.some(({ labels }) => labels[0] === "RimeOuterEvaluation"));
  assert.ok(nodes.some(({ labels }) => labels[0] === "RimeOuterMetricMeasurement"));
  assert.ok(nodes.some(({ labels }) => labels[0] === "RimeArtifact"));
  assert.equal(nodes.at(-1).labels[0], "RimeOuterStepClosed");
  assert.equal(edges.at(-1).predicate, "closed_by");
  assert.ok(edges.some(({ predicate }) => predicate === "selected"));
});

test("rejects raw prompt bodies and outer evaluations without inner evidence", async () => {
  const { graph } = recordingGraph();
  const outer = new VerglasRecursiveGraphStore({
    graph,
    experimentId: "recursive-3",
  });
  await outer.initialize({
    summary: "improve RIME",
    baseline: { id: "outer-0", summary: "baseline harness" },
  });

  await assert.rejects(
    outer.openStep({
      ...step,
      candidates: [
        {
          ...step.candidates[0],
          adjustments: [
            {
              ...step.candidates[0].adjustments[0],
              prompt: "full prompt body",
            },
          ],
        },
      ],
    }),
    /artifact metadata only/,
  );

  await outer.openStep(step);
  await assert.rejects(
    outer.recordEvaluation({
      stepId: "step-0",
      candidateId: "outer-1",
      status: "valid",
      score: 1,
      cost: 1,
      metrics: {},
      evidence: [],
    }),
    /inner run/,
  );
});

test("does not treat an uninitialized inner store as evaluation evidence", async () => {
  const { graph } = recordingGraph();
  const outer = new VerglasRecursiveGraphStore({
    graph,
    experimentId: "recursive-uninitialized",
  });
  await outer.initialize({
    summary: "improve RIME",
    baseline: { id: "outer-0", summary: "baseline harness" },
  });
  await outer.openStep(step);
  outer.innerRunStore({
    runId: "inner-uninitialized",
    stepId: "step-0",
    candidateId: "outer-1",
    task: { id: "task-uninitialized", family: "coding", split: "private" },
  });

  await assert.rejects(
    outer.recordEvaluation({
      stepId: "step-0",
      candidateId: "outer-1",
      status: "valid",
      score: 1,
      cost: 1,
      metrics: {},
      evidence: [],
    }),
    /initialized inner run/,
  );
});

test("does not close an outer step until every candidate is evaluated", async () => {
  const { graph } = recordingGraph();
  const outer = new VerglasRecursiveGraphStore({
    graph,
    experimentId: "recursive-4",
  });
  await outer.initialize({
    summary: "improve RIME",
    baseline: { id: "outer-0", summary: "baseline harness" },
  });
  await outer.openStep(step);
  outer.innerRunStore({
    runId: "inner-4",
    stepId: "step-0",
    candidateId: "outer-1",
    task: { id: "task-4", family: "coding", split: "private" },
  });

  await assert.rejects(
    outer.closeStep({ stepId: "step-0", selectedCandidateId: "outer-1" }),
    /unevaluated candidate/,
  );
});

test("can retain the incumbent after evaluating and rejecting an outer mutation", async () => {
  const { graph, calls } = recordingGraph();
  const outer = new VerglasRecursiveGraphStore({
    graph,
    experimentId: "recursive-reject",
  });
  await outer.initialize({
    summary: "improve RIME",
    baseline: { id: "outer-0", summary: "baseline harness" },
  });
  await outer.openStep(step);
  const inner = outer.innerRunStore({
    runId: "inner-reject",
    stepId: "step-0",
    candidateId: "outer-1",
    task: { id: "task-reject", family: "coding", split: "private" },
  });
  await inner.initialize(root);
  await outer.recordEvaluation({
    stepId: "step-0",
    candidateId: "outer-1",
    status: "invalid",
    score: 0,
    cost: 2,
    metrics: {},
    evidence: [
      {
        artifact: "artifact:rejection",
        kind: "held-out-evaluation",
        summary: "the prompt mutation regressed private tasks",
      },
    ],
  });
  await outer.closeStep({ stepId: "step-0", selectedCandidateId: "outer-0" });

  const edges = calls
    .filter(([kind]) => kind === "edges")
    .flatMap(([, values]) => values);
  assert.ok(
    edges.some(
      ({ predicate, dstId }) =>
        predicate === "selected" && dstId.includes("/OuterCandidate/outer-0"),
    ),
  );
});

test("requires outer evidence and never selects an invalid mutation", async () => {
  const { graph } = recordingGraph();
  const outer = new VerglasRecursiveGraphStore({
    graph,
    experimentId: "recursive-invalid-selection",
  });
  await outer.initialize({
    summary: "improve RIME",
    baseline: { id: "outer-0", summary: "baseline harness" },
  });
  await outer.openStep(step);
  const inner = outer.innerRunStore({
    runId: "inner-invalid",
    stepId: "step-0",
    candidateId: "outer-1",
    task: { id: "task-invalid", family: "coding", split: "private" },
  });
  await inner.initialize(root);

  await assert.rejects(
    outer.recordEvaluation({
      stepId: "step-0",
      candidateId: "outer-1",
      status: "valid",
      score: 1,
      cost: 1,
      metrics: {},
      evidence: [],
    }),
    /evidence artifact/,
  );
  await outer.recordEvaluation({
    stepId: "step-0",
    candidateId: "outer-1",
    status: "invalid",
    score: 0,
    cost: 1,
    metrics: {},
    evidence: [
      {
        artifact: "artifact:invalid",
        kind: "held-out-evaluation",
        summary: "private gate failed",
      },
    ],
  });
  await assert.rejects(
    outer.closeStep({ stepId: "step-0", selectedCandidateId: "outer-1" }),
    /valid outer candidate/,
  );
});
