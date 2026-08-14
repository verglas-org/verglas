/** Standard software-quality profile built on the general engineering objective. */

import { createEngineeringObjective } from "./engineering-objective.mjs";

export {
  CODING_GATE_DEFINITIONS,
  CODING_GATE_EVIDENCE_KINDS,
  REQUIRED_CODING_GATES,
} from "./coding-standards.mjs";

/** Default weights for correctness, architecture, and change-surface utility. */
export const DEFAULT_CODING_OBJECTIVE_WEIGHTS = Object.freeze({
  task: 0.7,
  architecture: 0.3,
  changeSurface: 0.1,
});

function weight(value, label) {
  if (!Number.isFinite(value) || value < 0) {
    throw new Error(`${label} must be a finite non-negative number.`);
  }
  return value;
}

/** Builds the default coding reward as one general engineering objective profile. */
export function createCodingObjective(
  weights = DEFAULT_CODING_OBJECTIVE_WEIGHTS,
) {
  const normalizedWeights = Object.freeze({
    task: weight(weights.task, "objectiveWeights.task"),
    architecture: weight(weights.architecture, "objectiveWeights.architecture"),
    changeSurface: weight(
      weights.changeSurface,
      "objectiveWeights.changeSurface",
    ),
  });
  const objective = createEngineeringObjective({
    metrics: [
      {
        name: "task",
        direction: "maximize",
        unit: "ratio",
        weight: normalizedWeights.task,
        scale: { type: "linear", worst: 0, best: 1 },
        evidenceKinds: ["test", "benchmark"],
      },
      {
        name: "architecture",
        direction: "maximize",
        unit: "ratio",
        weight: normalizedWeights.architecture,
        scale: { type: "linear", worst: 0, best: 1 },
        evidenceKinds: ["architecture-review"],
      },
      {
        name: "changeSurface",
        direction: "minimize",
        unit: "ratio",
        weight: normalizedWeights.changeSurface,
        scale: { type: "linear", worst: 1, best: 0 },
        evidenceKinds: ["diff-analysis", "static-analysis"],
      },
    ],
  });
  return Object.freeze({ ...objective, weights: normalizedWeights });
}
