/** Declarative hard gates, measurements, constraints, and scalar utility. */

import { CODING_GATE_DEFINITIONS } from "./coding-standards.mjs";

const DIRECTIONS = new Set(["maximize", "minimize"]);
const SCALE_TYPES = new Set(["linear", "log"]);
const OPERATORS = Object.freeze({
  "<": (actual, threshold) => actual < threshold,
  "<=": (actual, threshold) => actual <= threshold,
  ">": (actual, threshold) => actual > threshold,
  ">=": (actual, threshold) => actual >= threshold,
});

function finiteNumber(value, label) {
  if (!Number.isFinite(value)) throw new Error(`${label} must be finite.`);
  return value;
}

function nonemptyString(value, label) {
  if (typeof value !== "string" || !value.trim()) {
    throw new Error(`${label} must be a non-empty string.`);
  }
  return value.trim();
}

function normalizeEvidenceKinds(value, label) {
  if (!Array.isArray(value) || value.length === 0) {
    throw new Error(`${label} requires accepted evidence kinds.`);
  }
  return Object.freeze(
    value.map((kind) => nonemptyString(kind, `${label} evidence kind`)),
  );
}

function normalizeGateDefinitions(customGates = {}) {
  if (!customGates || typeof customGates !== "object") {
    throw new Error("Objective gates must be an object.");
  }
  const definitions = { ...CODING_GATE_DEFINITIONS };
  for (const [name, definition] of Object.entries(customGates)) {
    nonemptyString(name, "gate name");
    if (name in definitions) throw new Error(`Duplicate gate: ${name}.`);
    definitions[name] = Object.freeze({
      evidenceKinds: normalizeEvidenceKinds(
        definition?.evidenceKinds,
        `gate ${name}`,
      ),
    });
  }
  return Object.freeze(definitions);
}

function normalizeMetricDefinition(metric) {
  if (!metric || typeof metric !== "object") {
    throw new Error("Every objective metric requires a definition.");
  }
  const name = nonemptyString(metric.name, "metric name");
  const direction = metric.direction;
  if (!DIRECTIONS.has(direction)) {
    throw new Error(`${name} direction must be maximize or minimize.`);
  }
  const unit = nonemptyString(metric.unit, `${name} unit`);
  const weight = finiteNumber(metric.weight, `${name} weight`);
  if (weight < 0) throw new Error(`${name} weight must be non-negative.`);
  if (!SCALE_TYPES.has(metric.scale?.type)) {
    throw new Error(`${name} scale type must be linear or log.`);
  }
  const worst = finiteNumber(metric.scale.worst, `${name} scale.worst`);
  const best = finiteNumber(metric.scale.best, `${name} scale.best`);
  if (
    worst === best ||
    (direction === "maximize" && best < worst) ||
    (direction === "minimize" && best > worst)
  ) {
    throw new Error(
      `${name} direction does not match its worst-to-best scale.`,
    );
  }
  if (metric.scale.type === "log" && (worst <= 0 || best <= 0)) {
    throw new Error(`${name} log scale bounds must be positive.`);
  }
  return Object.freeze({
    name,
    direction,
    unit,
    weight,
    scale: Object.freeze({ type: metric.scale.type, worst, best }),
    evidenceKinds: normalizeEvidenceKinds(
      metric.evidenceKinds,
      `metric ${name}`,
    ),
  });
}

function normalizeMetricDefinitions(metrics) {
  if (!Array.isArray(metrics) || metrics.length === 0) {
    throw new Error("An engineering objective requires at least one metric.");
  }
  const normalized = metrics.map(normalizeMetricDefinition);
  const names = new Set();
  for (const metric of normalized) {
    if (names.has(metric.name))
      throw new Error(`Duplicate metric: ${metric.name}.`);
    names.add(metric.name);
  }
  if (!normalized.some(({ weight }) => weight > 0)) {
    throw new Error("At least one objective metric must have positive weight.");
  }
  return Object.freeze(normalized);
}

function normalizeConstraints(constraints = [], metrics) {
  if (!Array.isArray(constraints)) {
    throw new Error("Objective constraints must be an array.");
  }
  const metricNames = new Set(metrics.map(({ name }) => name));
  return Object.freeze(
    constraints.map((constraint) => {
      const metric = nonemptyString(constraint?.metric, "constraint metric");
      if (!metricNames.has(metric)) {
        throw new Error(`Constraint references unknown metric: ${metric}.`);
      }
      if (!Object.hasOwn(OPERATORS, constraint.operator)) {
        throw new Error(
          `Constraint for ${metric} has an unsupported operator.`,
        );
      }
      return Object.freeze({
        metric,
        operator: constraint.operator,
        threshold: finiteNumber(
          constraint.threshold,
          `${metric} constraint threshold`,
        ),
      });
    }),
  );
}

function normalizeEvidence(value, acceptedKinds, label) {
  if (!Array.isArray(value) || value.length === 0) {
    throw new Error(`${label} requires at least one evidence artifact.`);
  }
  return Object.freeze(
    value.map((item) => {
      if (!item || typeof item !== "object") {
        throw new Error(`${label} evidence must be a typed artifact.`);
      }
      if (!acceptedKinds.includes(item.kind)) {
        throw new Error(`${label} does not accept ${item.kind} evidence.`);
      }
      return Object.freeze({
        kind: item.kind,
        artifact: nonemptyString(item.artifact, `${label} artifact`),
        summary: nonemptyString(item.summary, `${label} evidence summary`),
      });
    }),
  );
}

function normalizeGates(gates, definitions) {
  if (!gates || typeof gates !== "object") {
    throw new Error("A valid engineering evaluation requires gates.");
  }
  for (const name of Object.keys(gates)) {
    if (!(name in definitions)) throw new Error(`Unknown gate: ${name}.`);
  }
  const normalized = {};
  for (const [name, definition] of Object.entries(definitions)) {
    const result = gates[name];
    if (!result || typeof result.passed !== "boolean") {
      throw new Error(`${name} requires an explicit passed result.`);
    }
    normalized[name] = Object.freeze({
      passed: result.passed,
      evaluator: nonemptyString(result.evaluator, `${name} evaluator`),
      evidence: normalizeEvidence(
        result.evidence,
        definition.evidenceKinds,
        name,
      ),
    });
  }
  return Object.freeze(normalized);
}

function metricUtility(value, { name, scale }) {
  if (scale.type === "log" && value <= 0) {
    throw new Error(`${name} value must be positive for its log scale.`);
  }
  const transform = scale.type === "log" ? Math.log : (number) => number;
  const utility =
    (transform(value) - transform(scale.worst)) /
    (transform(scale.best) - transform(scale.worst));
  return Number(Math.min(1, Math.max(0, utility)).toFixed(12));
}

function normalizeMeasurements(values, definitions) {
  if (!values || typeof values !== "object") {
    throw new Error("A valid engineering evaluation requires metrics.");
  }
  const names = new Set(definitions.map(({ name }) => name));
  for (const name of Object.keys(values)) {
    if (!names.has(name)) throw new Error(`Unknown metric: ${name}.`);
  }
  const normalized = {};
  for (const definition of definitions) {
    const measurement = values[definition.name];
    if (!measurement || typeof measurement !== "object") {
      throw new Error(`${definition.name} requires a measurement.`);
    }
    const value = finiteNumber(measurement.value, `${definition.name} value`);
    normalized[definition.name] = Object.freeze({
      value,
      unit: definition.unit,
      utility: metricUtility(value, definition),
      evaluator: nonemptyString(
        measurement.evaluator,
        `${definition.name} evaluator`,
      ),
      evidence: normalizeEvidence(
        measurement.evidence,
        definition.evidenceKinds,
        definition.name,
      ),
    });
  }
  return Object.freeze(normalized);
}

function normalizedBug(evaluation) {
  if (typeof evaluation.error !== "string" || !evaluation.error.trim()) {
    throw new Error("A buggy evaluation requires an error description.");
  }
  return Object.freeze({ status: "buggy", error: evaluation.error.trim() });
}

/** Creates a deterministic scalar objective over independently measured metrics. */
export function createEngineeringObjective({
  gates: customGates,
  metrics: metricInput,
  constraints: constraintInput,
}) {
  const gates = normalizeGateDefinitions(customGates);
  const metrics = normalizeMetricDefinitions(metricInput);
  const constraints = normalizeConstraints(constraintInput, metrics);
  const totalWeight = metrics.reduce(
    (total, metric) => total + metric.weight,
    0,
  );
  if (!Number.isFinite(totalWeight)) {
    throw new Error("Objective metric weights must have a finite total.");
  }
  return Object.freeze({
    gates,
    metrics,
    constraints,

    /** Validates evaluator evidence and returns a scalar AIDE utility. */
    normalize(evaluation) {
      if (!evaluation || typeof evaluation !== "object") {
        throw new Error("An evaluation is required for every candidate.");
      }
      if (evaluation.status === "buggy") return normalizedBug(evaluation);
      if (evaluation.status !== "valid") {
        throw new Error('Evaluation status must be "valid" or "buggy".');
      }
      const gateResults = normalizeGates(evaluation.gates, gates);
      const measurements = normalizeMeasurements(evaluation.metrics, metrics);
      const failedGates = Object.keys(gates).filter(
        (name) => !gateResults[name].passed,
      );
      const failedConstraints = constraints
        .filter(
          ({ metric, operator, threshold }) =>
            !OPERATORS[operator](measurements[metric].value, threshold),
        )
        .map(({ metric, operator, threshold }) =>
          Object.freeze({
            metric,
            operator,
            threshold,
            actual: measurements[metric].value,
          }),
        );
      if (failedGates.length > 0 || failedConstraints.length > 0) {
        const failures = [
          ...(failedGates.length > 0
            ? [`gates: ${failedGates.join(", ")}`]
            : []),
          ...(failedConstraints.length > 0
            ? [
                `constraints: ${failedConstraints
                  .map(
                    ({ metric, operator, threshold }) =>
                      `${metric} ${operator} ${threshold}`,
                  )
                  .join(", ")}`,
              ]
            : []),
        ];
        return Object.freeze({
          status: "buggy",
          error: `Engineering objective failed ${failures.join("; ")}.`,
          failedGates: Object.freeze(failedGates),
          failedConstraints: Object.freeze(failedConstraints),
          gates: gateResults,
          metrics: measurements,
        });
      }
      const score = Number(
        (
          metrics.reduce(
            (total, metric) =>
              total + metric.weight * measurements[metric.name].utility,
            0,
          ) / totalWeight
        ).toFixed(12),
      );
      return Object.freeze({
        status: "valid",
        score,
        gates: gateResults,
        metrics: measurements,
      });
    },
  });
}
