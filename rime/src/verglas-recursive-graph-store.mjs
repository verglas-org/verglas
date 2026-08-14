//! Coordinates outer harness experiments with durable, metadata-only inner RIME run evidence.

import { VerglasGraphStore } from "./verglas-graph-store.mjs";

/** Rejects fields that could turn the graph into a prompt or source-code archive. */
function assertMetadata(value, label, allowed) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new Error(`${label} must be metadata only.`);
  }
  for (const key of Object.keys(value)) {
    if (!allowed.has(key)) {
      throw new Error(`${label} must be artifact metadata only.`);
    }
  }
}

/** Requires a compact artifact reference rather than an artifact's contents. */
function assertArtifact(value, label) {
  assertMetadata(value, label, new Set(["artifact", "kind", "summary"]));
  for (const field of ["artifact", "kind", "summary"]) {
    if (typeof value[field] !== "string" || value[field].length === 0) {
      throw new Error(`${label}.${field} must be a non-empty string.`);
    }
  }
}

/** Validates one prompt or work adjustment before it can enter graph state. */
function assertAdjustment(adjustment) {
  assertMetadata(adjustment, "adjustment", new Set(["id", "kind", "artifact", "summary"]));
  for (const field of ["id", "kind", "artifact", "summary"]) {
    if (typeof adjustment[field] !== "string" || adjustment[field].length === 0) {
      throw new Error(`adjustment.${field} must be a non-empty string.`);
    }
  }
}

/** Persists a fully initialized inner run only after its task linkage exists. */
class RecursiveInnerRunStore extends VerglasGraphStore {
  #outer;
  #registration;

  /** Binds an ordinary inner graph store to the owning outer candidate and task. */
  constructor({ outer, registration }) {
    super({ graph: outer.graph, runId: registration.runId });
    this.#outer = outer;
    this.#registration = registration;
  }

  /** Initializes inner RIME state, then exposes its durable run evidence to outer evaluation. */
  async initialize(root) {
    await super.initialize(root);
    await this.#outer.recordInnerRun(this.#registration);
  }
}

/** Coordinates recursive harness candidates while retaining only durable evidence metadata. */
export class VerglasRecursiveGraphStore {
  #graph;
  #experimentId;
  #steps = new Map();
  #innerRuns = new Map();

  /** Creates a coordinator backed by an existing Verglas SDK graph handle. */
  constructor({ graph, experimentId }) {
    if (typeof graph?.create !== "function" || typeof graph?.insertNodes !== "function" || typeof graph?.insertEdges !== "function") {
      throw new Error("Verglas graph handle must provide create(), insertNodes(), and insertEdges().");
    }
    if (typeof experimentId !== "string" || experimentId.length === 0) {
      throw new Error("experimentId must be a non-empty string.");
    }
    this.#graph = graph;
    this.#experimentId = experimentId;
  }

  /** Exposes the shared graph only to the inner-store adapter. */
  get graph() {
    return this.#graph;
  }

  /** Builds a stable graph identifier in this recursive experiment namespace. */
  #id(kind, id) {
    return `rime-recursive/${this.#experimentId}/${kind}/${encodeURIComponent(id)}`;
  }

  /** Addresses a run node written by the inner `VerglasGraphStore`. */
  #innerRunId(runId) {
    return `rime/${runId}/Run/${encodeURIComponent(runId)}`;
  }

  /** Creates one namespaced graph node. */
  #node(kind, id, properties) {
    return { id: this.#id(kind, id), labels: [`Rime${kind}`], namespace: this.#experimentId, properties };
  }

  /** Creates one provenance-bearing graph edge. */
  #edge(sourceKind, sourceId, predicate, destinationKind, destinationId, properties = {}) {
    return {
      srcId: this.#id(sourceKind, sourceId), predicate, dstId: this.#id(destinationKind, destinationId),
      provenance: `rime-recursive:${this.#experimentId}`, namespace: this.#experimentId, properties,
    };
  }

  /** Initializes the experiment and its outer baseline without storing harness contents. */
  async initialize({ summary, baseline }) {
    assertMetadata(baseline, "baseline", new Set(["id", "summary"]));
    if (typeof summary !== "string" || typeof baseline.id !== "string" || typeof baseline.summary !== "string") {
      throw new Error("experiment summaries and baseline metadata must be strings.");
    }
    await this.#graph.create();
    await this.#graph.insertNodes([
      this.#node("RecursiveExperiment", this.#experimentId, { summary }),
      this.#node("OuterCandidate", baseline.id, { summary: baseline.summary, ordinal: 0, baseline: true }),
    ]);
    await this.#graph.insertEdges([
      this.#edge("RecursiveExperiment", this.#experimentId, "has_candidate", "OuterCandidate", baseline.id),
    ]);
  }

  /** Opens an outer policy step and records candidate adjustment artifact metadata. */
  async openStep(step) {
    assertMetadata(step, "step", new Set(["id", "ordinal", "parentCandidateId", "candidates"]));
    if (!Array.isArray(step.candidates) || step.candidates.length === 0) {
      throw new Error("step must contain at least one candidate.");
    }
    if (this.#steps.has(step.id)) throw new Error(`step ${step.id} already exists.`);
    const candidateIds = new Set();
    for (const candidate of step.candidates) {
      assertMetadata(candidate, "candidate", new Set(["id", "ordinal", "summary", "adjustments"]));
      if (candidateIds.has(candidate.id)) throw new Error(`step ${step.id} has duplicate candidate ${candidate.id}.`);
      candidateIds.add(candidate.id);
      if (!Array.isArray(candidate.adjustments)) throw new Error("candidate.adjustments must be an array.");
      candidate.adjustments.forEach(assertAdjustment);
    }
    const state = { ...step, evaluations: new Map(), closed: false };
    this.#steps.set(step.id, state);
    const adjustments = step.candidates.flatMap((candidate) => candidate.adjustments.map((adjustment) => ({ candidate, adjustment })));
    await this.#graph.insertNodes([
      this.#node("OuterStep", step.id, { ordinal: step.ordinal, parentCandidateId: step.parentCandidateId }),
      ...step.candidates.map((candidate) => this.#node("OuterCandidate", candidate.id, { ordinal: candidate.ordinal, summary: candidate.summary })),
      ...adjustments.map(({ adjustment }) => this.#node("HarnessAdjustment", adjustment.id, { kind: adjustment.kind, artifact: adjustment.artifact, summary: adjustment.summary })),
    ]);
    await this.#graph.insertEdges([
      this.#edge("RecursiveExperiment", this.#experimentId, "has_step", "OuterStep", step.id),
      this.#edge("OuterStep", step.id, "uses_parent", "OuterCandidate", step.parentCandidateId),
      ...step.candidates.map((candidate) => this.#edge("OuterStep", step.id, "evaluates", "OuterCandidate", candidate.id)),
      ...adjustments.map(({ candidate, adjustment }) => this.#edge("OuterCandidate", candidate.id, "adjusted_by", "HarnessAdjustment", adjustment.id)),
    ]);
  }

  /** Returns an inner store whose initialization becomes required evidence for outer scoring. */
  innerRunStore({ runId, stepId, candidateId, task }) {
    const step = this.#steps.get(stepId);
    if (!step || !step.candidates.some((candidate) => candidate.id === candidateId)) {
      throw new Error("inner run must belong to an existing outer step candidate.");
    }
    assertMetadata(task, "task", new Set(["id", "family", "split"]));
    if (typeof runId !== "string" || typeof task.id !== "string" || typeof task.family !== "string" || typeof task.split !== "string") {
      throw new Error("inner run and task metadata must be strings.");
    }
    if (this.#innerRuns.has(runId)) throw new Error(`inner run ${runId} already exists.`);
    const registration = { runId, stepId, candidateId, task, initialized: false };
    this.#innerRuns.set(runId, registration);
    return new RecursiveInnerRunStore({ outer: this, registration });
  }

  /** Links a completed inner store and task to its outer candidate exactly once. */
  async recordInnerRun(registration) {
    if (registration.initialized) throw new Error(`inner run ${registration.runId} was initialized twice.`);
    await this.#graph.insertNodes([
      this.#node("InnerTask", registration.task.id, { family: registration.task.family, split: registration.task.split }),
    ]);
    await this.#graph.insertEdges([
      {
        srcId: this.#id("OuterCandidate", registration.candidateId),
        predicate: "evaluated_by",
        dstId: this.#innerRunId(registration.runId),
        provenance: `rime-recursive:${this.#experimentId}`,
        namespace: this.#experimentId,
        properties: {},
      },
      {
        srcId: this.#innerRunId(registration.runId),
        predicate: "runs_task",
        dstId: this.#id("InnerTask", registration.task.id),
        provenance: `rime-recursive:${this.#experimentId}`,
        namespace: this.#experimentId,
        properties: {},
      },
      {
        srcId: this.#innerRunId(registration.runId),
        predicate: "belongs_to_experiment",
        dstId: this.#id("RecursiveExperiment", this.#experimentId),
        provenance: `rime-recursive:${this.#experimentId}`,
        namespace: this.#experimentId,
        properties: {},
      },
    ]);
    registration.initialized = true;
  }

  /** Records held-out measurements only after an inner RIME run has been registered for the candidate. */
  async recordEvaluation({ stepId, candidateId, status, score, cost, metrics, evidence }) {
    const step = this.#steps.get(stepId);
    if (!step || step.closed || !step.candidates.some((candidate) => candidate.id === candidateId)) {
      throw new Error("evaluation must belong to an open outer step candidate.");
    }
    if (![...this.#innerRuns.values()].some((run) => run.stepId === stepId && run.candidateId === candidateId && run.initialized)) {
      throw new Error("outer evaluation requires initialized inner run evidence.");
    }
    if (step.evaluations.has(candidateId)) throw new Error(`candidate ${candidateId} is already evaluated.`);
    if (typeof status !== "string" || !Number.isFinite(score) || !Number.isFinite(cost) || !metrics || typeof metrics !== "object" || !Array.isArray(evidence)) {
      throw new Error("outer evaluation has invalid held-out measurements.");
    }
    if (evidence.length === 0) throw new Error("outer evaluation requires at least one evidence artifact.");
    evidence.forEach((entry) => assertArtifact(entry, "evidence"));
    for (const [name, metric] of Object.entries(metrics)) {
      assertMetadata(metric, `metric ${name}`, new Set(["value", "unit"]));
      if (!Number.isFinite(metric.value) || typeof metric.unit !== "string") throw new Error(`metric ${name} is invalid.`);
    }
    const innerRunIds = [...this.#innerRuns.values()]
      .filter((run) => run.stepId === stepId && run.candidateId === candidateId && run.initialized)
      .map((run) => run.runId);
    step.evaluations.set(candidateId, { status, score, cost });
    const evaluationId = `${stepId}.${candidateId}`;
    await this.#graph.insertNodes([
      this.#node("OuterEvaluation", evaluationId, { status, score, cost, innerRunIds }),
      ...Object.entries(metrics).map(([name, metric]) => this.#node("OuterMetricMeasurement", `${evaluationId}.${name}`, { name, value: metric.value, unit: metric.unit })),
      ...evidence.map((entry, ordinal) => this.#node("Artifact", `${evaluationId}.evidence.${ordinal}`, entry)),
    ]);
    await this.#graph.insertEdges([
      this.#edge("OuterCandidate", candidateId, "has_evaluation", "OuterEvaluation", evaluationId),
      ...Object.keys(metrics).map((name) => this.#edge("OuterEvaluation", evaluationId, "has_metric", "OuterMetricMeasurement", `${evaluationId}.${name}`)),
      ...evidence.map((_, ordinal) => this.#edge("OuterEvaluation", evaluationId, "supported_by", "Artifact", `${evaluationId}.evidence.${ordinal}`)),
    ]);
  }

  /** Selects the incumbent or a valid proposal after evaluating every proposal, then writes the visibility fence last. */
  async closeStep({ stepId, selectedCandidateId }) {
    const step = this.#steps.get(stepId);
    if (!step || step.closed) throw new Error("outer step is not open.");
    for (const candidate of step.candidates) {
      if (!step.evaluations.has(candidate.id)) throw new Error(`unevaluated candidate ${candidate.id}.`);
    }
    if (selectedCandidateId !== step.parentCandidateId) {
      const selectedEvaluation = step.evaluations.get(selectedCandidateId);
      if (!selectedEvaluation || selectedEvaluation.status !== "valid") {
        throw new Error("selected candidate must be a valid outer candidate or the incumbent.");
      }
    }
    await this.#graph.insertNodes([this.#node("OuterSelection", stepId, { candidateId: selectedCandidateId })]);
    await this.#graph.insertEdges([this.#edge("OuterStep", stepId, "selected", "OuterCandidate", selectedCandidateId)]);
    await this.#graph.insertNodes([this.#node("OuterStepClosed", stepId, { closed: true })]);
    await this.#graph.insertEdges([this.#edge("OuterStep", stepId, "closed_by", "OuterStepClosed", stepId)]);
    step.closed = true;
  }
}
