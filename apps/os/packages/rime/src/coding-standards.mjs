/** Reusable evidence gates that every winning software candidate must satisfy. */

/** Required software-engineering gates and their accepted evidence artifacts. */
export const CODING_GATE_DEFINITIONS = Object.freeze({
  taskCorrect: Object.freeze({
    evidenceKinds: Object.freeze(["test", "benchmark"]),
  }),
  noDeadCode: Object.freeze({
    evidenceKinds: Object.freeze(["static-analysis", "diff-analysis"]),
  }),
  noFallbacks: Object.freeze({
    evidenceKinds: Object.freeze(["diff-analysis", "architecture-review"]),
  }),
  architecturalFit: Object.freeze({
    evidenceKinds: Object.freeze(["architecture-review"]),
  }),
  semanticRigor: Object.freeze({
    evidenceKinds: Object.freeze([
      "parser-contract",
      "typecheck",
      "static-analysis",
      "test",
    ]),
  }),
});

/** Names of the software-engineering gates applied to every objective. */
export const REQUIRED_CODING_GATES = Object.freeze(
  Object.keys(CODING_GATE_DEFINITIONS),
);

/** Accepted gate evidence lookup used by evaluator integrations. */
export const CODING_GATE_EVIDENCE_KINDS = Object.freeze(
  Object.fromEntries(
    Object.entries(CODING_GATE_DEFINITIONS).map(([name, definition]) => [
      name,
      definition.evidenceKinds,
    ]),
  ),
);
