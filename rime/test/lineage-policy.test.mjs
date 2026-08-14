import assert from "node:assert/strict";
import test from "node:test";

import {
  CandidateGraph,
  normalizeSearchConfig,
  selectProposalBatch,
} from "../src/index.mjs";

const valid = (score) => ({
  status: "valid",
  score,
  metrics: {},
  gates: {},
});

const buggy = (error = "failed") => ({ status: "buggy", error });

const graphWith = (evaluation) =>
  new CandidateGraph(
    { solution: "baseline", evaluation },
    { objective: { normalize: (result) => result } },
  );

const config = (overrides = {}) => ({
  initialDrafts: 2,
  concurrency: 1,
  remainingCandidates: 1,
  maxDebugDepth: 2,
  debugProbability: 0,
  lineageExplorationWeight: 1,
  random: () => 0,
  ...overrides,
});

test("explores an under-sampled draft lineage instead of repeatedly exploiting the global best", () => {
  const graph = graphWith(buggy("reproduced"));
  const strong = graph.record({
    parentId: "candidate-0",
    operation: "draft",
    solution: "strong",
    summary: "strong draft",
    evaluation: valid(0.9),
  });
  const exploratory = graph.record({
    parentId: "candidate-0",
    operation: "draft",
    solution: "exploratory",
    summary: "different draft",
    evaluation: valid(0.6),
  });
  graph.record({
    parentId: strong.id,
    operation: "improve",
    solution: "strong-child",
    summary: "refine the strong lineage",
    evaluation: valid(0.91),
  });

  const [proposal] = selectProposalBatch(graph.snapshot(), config());

  assert.equal(proposal.operation, "improve");
  assert.equal(proposal.parentId, exploratory.id);
  assert.equal(proposal.lineageId, exploratory.id);
});

test("selects the best valid parent within the chosen lineage", () => {
  const graph = graphWith(buggy("reproduced"));
  const first = graph.record({
    parentId: "candidate-0",
    operation: "draft",
    solution: "first",
    summary: "first draft",
    evaluation: valid(0.8),
  });
  const second = graph.record({
    parentId: "candidate-0",
    operation: "draft",
    solution: "second",
    summary: "second draft",
    evaluation: valid(0.7),
  });
  const refined = graph.record({
    parentId: second.id,
    operation: "improve",
    solution: "second-refined",
    summary: "refine the second lineage",
    evaluation: valid(0.75),
  });
  graph.record({
    parentId: first.id,
    operation: "improve",
    solution: "first-refined",
    summary: "refine the first lineage",
    evaluation: valid(0.81),
  });

  const [proposal] = selectProposalBatch(
    graph.snapshot(),
    config({ lineageExplorationWeight: 2 }),
  );

  assert.equal(proposal.parentId, refined.id);
  assert.equal(proposal.lineageId, second.id);
});

test("zero exploration weight retains greedy global-best selection", () => {
  const graph = graphWith(buggy("reproduced"));
  const best = graph.record({
    parentId: "candidate-0",
    operation: "draft",
    solution: "best",
    summary: "best draft",
    evaluation: valid(0.9),
  });
  graph.record({
    parentId: "candidate-0",
    operation: "draft",
    solution: "other",
    summary: "other draft",
    evaluation: valid(0.5),
  });

  const [proposal] = selectProposalBatch(
    graph.snapshot(),
    config({ lineageExplorationWeight: 0 }),
  );

  assert.equal(proposal.parentId, best.id);
  assert.equal(proposal.lineageId, best.id);
});

test("rejects an invalid lineage exploration weight", () => {
  const graph = graphWith(buggy("reproduced"));

  assert.throws(
    () =>
      selectProposalBatch(
        graph.snapshot(),
        config({ lineageExplorationWeight: -1 }),
      ),
    /lineageExplorationWeight/,
  );
});

test("defaults lineage exploration to one and rejects non-finite weights", () => {
  assert.equal(
    normalizeSearchConfig({
      initialDrafts: 2,
      concurrency: 1,
      candidateBudget: 2,
      maxDebugDepth: 2,
      debugProbability: 0,
    }).lineageExplorationWeight,
    1,
  );
  const graph = graphWith(buggy("reproduced"));

  assert.throws(
    () =>
      selectProposalBatch(
        graph.snapshot(),
        config({ lineageExplorationWeight: Number.NaN }),
      ),
    /lineageExplorationWeight/,
  );
});
