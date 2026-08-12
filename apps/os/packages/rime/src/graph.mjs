/** Candidate lineage and independently measured evaluations for one RIME run. */

import { createCodingObjective } from "./coding-objective.mjs";

const operations = new Set(["root", "draft", "debug", "improve"]);

function requireOperation(operation) {
  if (!operations.has(operation)) {
    throw new Error(`Unknown candidate operation: ${operation}.`);
  }
  return operation;
}

function requireSummary(summary) {
  if (typeof summary !== "string" || !summary.trim()) {
    throw new Error("Every candidate requires a concise summary.");
  }
  return summary.trim();
}

/** Stores an append-only solution tree and deterministic insertion order. */
export class CandidateGraph {
  #candidates = [];
  #objective;

  /** Creates a graph whose root is the independently evaluated baseline. */
  constructor(
    { solution, summary = "baseline", evaluation },
    { objective, objectiveWeights } = {},
  ) {
    if (objective && objectiveWeights) {
      throw new Error("Provide objective or objectiveWeights, not both.");
    }
    if (objective && typeof objective.normalize !== "function") {
      throw new Error("objective must provide normalize().");
    }
    this.#objective = objective ?? createCodingObjective(objectiveWeights);
    this.#candidates.push(
      Object.freeze({
        id: "candidate-0",
        ordinal: 0,
        parentId: null,
        operation: "root",
        solution,
        summary: requireSummary(summary),
        evaluation: this.#objective.normalize(evaluation),
        debugDepth: 0,
        expanded: false,
      }),
    );
  }

  /** Rebuilds a candidate tree from a validated closed-wave snapshot. */
  static hydrate(snapshot, { objective, objectiveWeights } = {}) {
    if (!snapshot || !Array.isArray(snapshot.candidates)) {
      throw new Error("Candidate snapshot must contain candidates.");
    }
    const [root, ...candidates] = snapshot.candidates;
    if (!root) throw new Error("Candidate snapshot requires a root.");
    const graph = new CandidateGraph(root, { objective, objectiveWeights });
    if (root.expanded) graph.markExpanded(root.id);
    for (const candidate of candidates) {
      const restored = graph.record(candidate);
      if (
        restored.id !== candidate.id ||
        restored.ordinal !== candidate.ordinal
      ) {
        throw new Error("Candidate snapshot order is not contiguous.");
      }
      if (candidate.expanded) graph.markExpanded(candidate.id);
    }
    return graph;
  }

  /** Appends one evaluated proposal to the solution tree. */
  record({
    parentId,
    operation,
    solution,
    summary,
    evaluation,
    debugDepth = 0,
  }) {
    if (!this.#candidates.some((candidate) => candidate.id === parentId)) {
      throw new Error(`Unknown parent candidate: ${parentId}.`);
    }
    if (!Number.isInteger(debugDepth) || debugDepth < 0) {
      throw new Error("Debug depth must be a non-negative integer.");
    }
    const ordinal = this.#candidates.length;
    const candidate = Object.freeze({
      id: `candidate-${ordinal}`,
      ordinal,
      parentId,
      operation: requireOperation(operation),
      solution,
      summary: requireSummary(summary),
      evaluation: this.#objective.normalize(evaluation),
      debugDepth,
      expanded: false,
    });
    this.#candidates.push(candidate);
    return candidate;
  }

  /** Marks a buggy node as already used for a debugging expansion. */
  markExpanded(candidateId) {
    const index = this.#candidates.findIndex(
      (candidate) => candidate.id === candidateId,
    );
    if (index < 0) throw new Error(`Unknown candidate: ${candidateId}.`);
    const candidate = this.#candidates[index];
    if (!candidate.expanded) {
      this.#candidates[index] = Object.freeze({ ...candidate, expanded: true });
    }
  }

  /** Returns the highest-scoring valid candidate, breaking ties by age. */
  bestValid() {
    return this.#candidates
      .filter((candidate) => candidate.evaluation.status === "valid")
      .toSorted(
        (left, right) =>
          right.evaluation.score - left.evaluation.score ||
          left.ordinal - right.ordinal,
      )[0];
  }

  /** Returns an immutable point-in-time view for policy and summarization. */
  snapshot() {
    return Object.freeze({ candidates: Object.freeze([...this.#candidates]) });
  }
}

/** Validates and normalizes an evaluator result. */
export function normalizeEvaluation(evaluation, objectiveWeights) {
  return createCodingObjective(objectiveWeights).normalize(evaluation);
}
