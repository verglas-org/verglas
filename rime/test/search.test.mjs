import assert from "node:assert/strict";
import test from "node:test";
import {
  CandidateGraph,
  RimeStateStore,
  normalizeEvaluation,
  selectProposalBatch,
} from "../src/index.mjs";
import { createSearch } from "./preferred-fleet.mjs";

const gateEvidence = {
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
  noDeadCode: {
    passed: true,
    evaluator: "static-analyzer",
    evidence: [
      {
        kind: "static-analysis",
        artifact: "artifact:dead-code-report",
        summary: "dead-code analysis passed",
      },
    ],
  },
  noFallbacks: {
    passed: true,
    evaluator: "diff-analyzer",
    evidence: [
      {
        kind: "diff-analysis",
        artifact: "artifact:diff-report",
        summary: "diff contains one active path",
      },
    ],
  },
  architecturalFit: {
    passed: true,
    evaluator: "architecture-critic",
    evidence: [
      {
        kind: "architecture-review",
        artifact: "artifact:architecture-review",
        summary: "the owning invariant is centralized",
      },
    ],
  },
  semanticRigor: {
    passed: true,
    evaluator: "semantic-critic",
    evidence: [
      {
        kind: "typecheck",
        artifact: "artifact:typecheck",
        summary: "canonical parser and domain types are used",
      },
    ],
  },
};
const metricEvidence = (value, kind, name) => ({
  value,
  evaluator: `${name}-evaluator`,
  evidence: [
    {
      kind,
      artifact: `artifact:${name}`,
      summary: `${name} measured independently`,
    },
  ],
});
const valid = (task, overrides = {}) => ({
  status: "valid",
  gates: { ...gateEvidence, ...overrides.gates },
  metrics: {
    task: metricEvidence(task, "test", "task"),
    architecture: metricEvidence(
      overrides.metrics?.architecture ?? 1,
      "architecture-review",
      "architecture",
    ),
    changeSurface: metricEvidence(
      overrides.metrics?.changeSurface ?? 0,
      "diff-analysis",
      "change-surface",
    ),
  },
});
const buggy = (error = "failed") => ({ status: "buggy", error });

test("drafts the configured number of initial solutions in parallel", () => {
  const graph = new CandidateGraph({
    solution: "baseline",
    evaluation: buggy("reproduction fails"),
  });

  const proposals = selectProposalBatch(graph.snapshot(), {
    initialDrafts: 3,
    concurrency: 8,
    remainingCandidates: 10,
    maxDebugDepth: 2,
    debugProbability: 1,
    random: () => 0,
  });

  assert.equal(proposals.length, 3);
  assert.deepEqual(
    proposals.map(({ operation, parentId }) => [operation, parentId]),
    [
      ["draft", "candidate-0"],
      ["draft", "candidate-0"],
      ["draft", "candidate-0"],
    ],
  );
});

test("allows waves up to one hundred workers and rejects larger fleets", () => {
  const graph = new CandidateGraph({
    solution: "baseline",
    evaluation: buggy("reproduction fails"),
  });
  const config = {
    initialDrafts: 100,
    concurrency: 100,
    remainingCandidates: 100,
    maxDebugDepth: 2,
    debugProbability: 1,
    random: () => 0,
  };

  assert.equal(selectProposalBatch(graph.snapshot(), config).length, 100);
  assert.throws(
    () => selectProposalBatch(graph.snapshot(), { ...config, concurrency: 101 }),
    /at most 100/,
  );
});

test("debugs an unexpanded buggy candidate within the depth limit", () => {
  const graph = new CandidateGraph({
    solution: "baseline",
    evaluation: valid(0),
  });
  graph.record({
    parentId: "candidate-0",
    operation: "draft",
    solution: "broken-a",
    summary: "first draft",
    evaluation: buggy("syntax error"),
  });
  graph.record({
    parentId: "candidate-0",
    operation: "draft",
    solution: "broken-b",
    summary: "second draft",
    evaluation: buggy("test error"),
  });

  const proposals = selectProposalBatch(graph.snapshot(), {
    initialDrafts: 2,
    concurrency: 2,
    remainingCandidates: 2,
    maxDebugDepth: 2,
    debugProbability: 1,
    random: () => 0,
  });

  assert.deepEqual(
    proposals.map(({ operation, parentId, debugDepth }) => [
      operation,
      parentId,
      debugDepth,
    ]),
    [
      ["debug", "candidate-1", 1],
      ["debug", "candidate-1", 1],
    ],
  );
});

test("improves the best valid candidate after bounded debugging is exhausted", () => {
  const graph = new CandidateGraph({
    solution: "baseline",
    evaluation: valid(0.1),
  });
  graph.record({
    parentId: "candidate-0",
    operation: "draft",
    solution: "best",
    summary: "valid draft",
    evaluation: valid(0.9),
  });
  const broken = graph.record({
    parentId: "candidate-0",
    operation: "draft",
    solution: "broken",
    summary: "broken draft",
    evaluation: buggy(),
  });
  graph.markExpanded(broken.id);

  const proposals = selectProposalBatch(graph.snapshot(), {
    initialDrafts: 2,
    concurrency: 3,
    remainingCandidates: 3,
    maxDebugDepth: 2,
    debugProbability: 0,
    random: () => 0,
  });

  assert.deepEqual(
    proposals.map(({ operation, parentId }) => [operation, parentId]),
    [
      ["improve", "candidate-1"],
      ["improve", "candidate-1"],
      ["improve", "candidate-1"],
    ],
  );
});

test("does not debug a buggy candidate beyond the maximum debug depth", () => {
  const graph = new CandidateGraph({
    solution: "baseline",
    evaluation: valid(1),
  });
  graph.record({
    parentId: "candidate-0",
    operation: "debug",
    solution: "still broken",
    summary: "debug failed",
    evaluation: buggy(),
    debugDepth: 2,
  });

  const proposals = selectProposalBatch(graph.snapshot(), {
    initialDrafts: 0,
    concurrency: 1,
    remainingCandidates: 1,
    maxDebugDepth: 1,
    debugProbability: 1,
    random: () => 0,
  });

  assert.equal(proposals[0].operation, "improve");
  assert.equal(proposals[0].parentId, "candidate-0");
});

test("runs proposals concurrently, records evaluations, and returns the best solution", async () => {
  let active = 0;
  let peak = 0;
  const starts = [];
  const engine = createSearch({
    initialDrafts: 3,
    concurrency: 3,
    candidateBudget: 3,
    maxDebugDepth: 2,
    debugProbability: 1,
    random: () => 0,
    summarize: (snapshot) => snapshot.candidates.map((node) => node.summary),
    propose: async (proposal) => {
      active += 1;
      peak = Math.max(peak, active);
      starts.push(proposal);
      await new Promise((resolve) => setTimeout(resolve, 10));
      active -= 1;
      return {
        solution: `solution-${proposal.ordinal}`,
        summary: `draft ${proposal.ordinal}`,
      };
    },
    evaluate: async ({ solution }) =>
      valid(Number(solution.replace("solution-", "")) / 3),
  });

  const result = await engine.run({
    solution: "baseline",
    summary: "reproduction",
    evaluation: buggy("reproduced"),
  });

  assert.equal(peak, 3);
  assert.equal(starts.length, 3);
  assert.deepEqual(starts[0].evidence, ["reproduction"]);
  assert.equal(result.best.solution, "solution-3");
  assert.equal(result.graph.candidates.length, 4);
  assert.equal(result.consumedCandidates, 3);
});

test("candidate budget is a hard ceiling across parallel rounds", async () => {
  const engine = createSearch({
    initialDrafts: 2,
    concurrency: 4,
    candidateBudget: 5,
    maxDebugDepth: 1,
    debugProbability: 1,
    random: () => 0,
    propose: async (proposal) => ({
      solution: `${proposal.operation}-${proposal.ordinal}`,
      summary: proposal.operation,
    }),
    evaluate: async ({ solution }) =>
      valid(Number(solution.split("-").at(-1)) / 5),
  });

  const result = await engine.run({
    solution: "baseline",
    summary: "baseline",
    evaluation: valid(0),
  });

  assert.equal(result.consumedCandidates, 5);
  assert.equal(result.graph.candidates.length, 6);
  assert.equal(result.best.evaluation.score, 1);
});

test("rejects invalid configuration and evaluator results", async () => {
  assert.throws(
    () =>
      createSearch({
        initialDrafts: 1,
        concurrency: 0,
        candidateBudget: 1,
        maxDebugDepth: 1,
        debugProbability: 1,
        random: () => 0,
        propose: async () => ({ solution: "x", summary: "x" }),
        evaluate: async () => valid(1),
      }),
    /concurrency/,
  );

  const engine = createSearch({
    initialDrafts: 1,
    concurrency: 1,
    candidateBudget: 1,
    maxDebugDepth: 1,
    debugProbability: 1,
    random: () => 0,
    propose: async () => ({ solution: "x", summary: "x" }),
    evaluate: async () => valid(Number.NaN),
  });

  await assert.rejects(
    engine.run({
      solution: "baseline",
      summary: "baseline",
      evaluation: valid(0),
    }),
    /task.*finite/,
  );
});

test("uses AIDE's debugging probability and random leaf selection", () => {
  const graph = new CandidateGraph({
    solution: "baseline",
    evaluation: valid(0),
  });
  for (const summary of ["first bug", "second bug"]) {
    graph.record({
      parentId: "candidate-0",
      operation: "draft",
      solution: summary,
      summary,
      evaluation: buggy(summary),
    });
  }

  const draws = [0.4, 0.99];
  const debug = selectProposalBatch(graph.snapshot(), {
    initialDrafts: 2,
    concurrency: 2,
    remainingCandidates: 2,
    maxDebugDepth: 2,
    debugProbability: 0.5,
    random: () => draws.shift(),
  });
  assert.equal(debug[0].operation, "debug");
  assert.equal(debug[0].parentId, "candidate-2");

  const improve = selectProposalBatch(graph.snapshot(), {
    initialDrafts: 2,
    concurrency: 1,
    remainingCandidates: 1,
    maxDebugDepth: 2,
    debugProbability: 0.5,
    random: () => 0.75,
  });
  assert.equal(improve[0].operation, "improve");
  assert.equal(improve[0].parentId, "candidate-0");
});

test("returns to drafting when no valid candidate can be improved", () => {
  const graph = new CandidateGraph({
    solution: "baseline",
    evaluation: buggy("baseline fails"),
  });
  const draft = graph.record({
    parentId: "candidate-0",
    operation: "draft",
    solution: "broken draft",
    summary: "broken draft",
    evaluation: buggy("still fails"),
    debugDepth: 2,
  });
  graph.markExpanded(draft.id);

  const proposals = selectProposalBatch(graph.snapshot(), {
    initialDrafts: 1,
    concurrency: 2,
    remainingCandidates: 2,
    maxDebugDepth: 1,
    debugProbability: 1,
    random: () => 0,
  });

  assert.deepEqual(
    proposals.map(({ operation, parentId }) => [operation, parentId]),
    [
      ["draft", "candidate-0"],
      ["draft", "candidate-0"],
    ],
  );
});

test("failed quality gates make a candidate buggy regardless of task score", () => {
  const evaluation = normalizeEvaluation(
    valid(1, {
      gates: {
        noFallbacks: {
          passed: false,
          evaluator: "diff-analyzer",
          evidence: [
            {
              kind: "diff-analysis",
              artifact: "artifact:diff-report",
              summary: "the patch retains the superseded compatibility path",
            },
          ],
        },
      },
    }),
  );

  assert.equal(evaluation.status, "buggy");
  assert.match(evaluation.error, /noFallbacks/);
  assert.deepEqual(evaluation.failedGates, ["noFallbacks"]);
});

test("requires concrete evidence for every coding-quality gate", () => {
  assert.throws(
    () =>
      normalizeEvaluation(
        valid(1, {
          gates: {
            semanticRigor: {
              passed: true,
              evaluator: "semantic-critic",
              evidence: [],
            },
          },
        }),
      ),
    /semanticRigor.*evidence/,
  );
});

test("rejects unsupported evidence kinds for semantic rigor", () => {
  assert.throws(
    () =>
      normalizeEvaluation(
        valid(1, {
          gates: {
            semanticRigor: {
              passed: true,
              evaluator: "mutation-agent",
              evidence: [
                {
                  kind: "architecture-review",
                  artifact: "artifact:self-review",
                  summary: "the parser looks correct",
                },
              ],
            },
          },
        }),
      ),
    /semanticRigor.*architecture-review/,
  );
});

test("equal-task candidates prefer architectural quality and smaller change surface", () => {
  const graph = new CandidateGraph({
    solution: "baseline",
    evaluation: buggy("reproduced"),
  });
  graph.record({
    parentId: "candidate-0",
    operation: "draft",
    solution: "point-fix",
    summary: "Patch one call site",
    evaluation: valid(1, {
      metrics: { architecture: 0.4, changeSurface: 0.1 },
    }),
  });
  graph.record({
    parentId: "candidate-0",
    operation: "draft",
    solution: "owning-abstraction",
    summary: "Repair the shared invariant",
    evaluation: valid(1, {
      metrics: { architecture: 0.9, changeSurface: 0.2 },
    }),
  });

  assert.equal(graph.bestValid().solution, "owning-abstraction");
  assert.equal(graph.bestValid().evaluation.score, 0.954545454545);
});

test("AIDE summaries carry quality results without replaying evidence artifacts", async () => {
  let observedEvidence;
  const engine = createSearch({
    initialDrafts: 1,
    concurrency: 1,
    candidateBudget: 1,
    maxDebugDepth: 1,
    debugProbability: 0,
    random: () => 0,
    propose: async ({ evidence }) => {
      observedEvidence = evidence;
      return { solution: "candidate", summary: "one atomic change" };
    },
    evaluate: async () => valid(1),
  });

  await engine.run({
    solution: "baseline",
    summary: "baseline passes",
    evaluation: valid(0.5),
  });

  assert.deepEqual(observedEvidence[0].evaluation, {
    status: "valid",
    score: 0.681818181818,
    metrics: {
      task: { value: 0.5, unit: "ratio", utility: 0.5 },
      architecture: { value: 1, unit: "ratio", utility: 1 },
      changeSurface: { value: 0, unit: "ratio", utility: 1 },
    },
  });
});

test("records runtime measurements returned by the fleet worker", async () => {
  const store = new RimeStateStore({ runId: "run-runtime" });
  const engine = createSearch({
    initialDrafts: 1,
    concurrency: 1,
    candidateBudget: 1,
    maxDebugDepth: 1,
    debugProbability: 0,
    stateStore: store,
    propose: async () => ({
      solution: "candidate",
      summary: "one candidate",
      runtime: { startedAt: 10, finishedAt: 20, tokens: 55, cost: 0.01 },
    }),
    evaluate: async () => valid(1),
  });

  const result = await engine.run({
    solution: "baseline",
    summary: "baseline",
    evaluation: buggy("reproduced"),
  });

  assert.equal(result.state.attemptMatrix[0].tokens, 55);
  assert.equal(result.state.attemptMatrix[0].cost, 0.01);
});

test("resumes AIDE selection from a closed state-store checkpoint", async () => {
  const firstStore = new RimeStateStore({ runId: "run-resume" });
  const first = createSearch({
    initialDrafts: 1,
    concurrency: 1,
    candidateBudget: 1,
    maxDebugDepth: 1,
    debugProbability: 0,
    stateStore: firstStore,
    propose: async () => ({ solution: "first", summary: "first draft" }),
    evaluate: async () => valid(0.5),
  });
  await first.run({
    solution: "baseline",
    summary: "baseline",
    evaluation: buggy("reproduced"),
  });

  const resumedStore = RimeStateStore.hydrate(firstStore.checkpoint());
  const parents = [];
  const resumed = createSearch({
    initialDrafts: 1,
    concurrency: 1,
    candidateBudget: 2,
    maxDebugDepth: 1,
    debugProbability: 0,
    stateStore: resumedStore,
    propose: async ({ parentId }) => {
      parents.push(parentId);
      return { solution: "second", summary: "improved draft" };
    },
    evaluate: async () => valid(1),
  });
  const result = await resumed.resume();

  assert.deepEqual(parents, ["candidate-1"]);
  assert.equal(result.graph.candidates.length, 3);
  assert.equal(result.state.waves.length, 2);
  assert.equal(result.best.solution, "second");
});
