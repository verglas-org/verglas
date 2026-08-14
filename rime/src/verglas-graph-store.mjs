/** Projects normalized RIME experiment state into a Verglas property graph. */

import { RimeStateStore } from "./state-store.mjs";

function requireMethod(target, method) {
  if (typeof target?.[method] !== "function") {
    throw new Error(`Verglas graph handle must provide ${method}().`);
  }
}

function graphId(runId, kind, id) {
  return `rime/${runId}/${kind}/${encodeURIComponent(id)}`;
}

function candidateProperties(candidate) {
  return {
    ordinal: candidate.ordinal,
    operation: candidate.operation,
    summary: candidate.summary,
    status: candidate.evaluation.status,
    ...(candidate.evaluation.status === "valid"
      ? {
          score: candidate.evaluation.score,
        }
      : { error: candidate.evaluation.error }),
    debugDepth: candidate.debugDepth,
  };
}

/** Persists every state transition and maintains its closed-wave projection. */
export class VerglasGraphStore extends RimeStateStore {
  #graph;
  #runId;

  /** Binds one RIME run to an existing SDK `Graph` handle. */
  constructor({ graph, runId }) {
    requireMethod(graph, "create");
    requireMethod(graph, "insertNodes");
    requireMethod(graph, "insertEdges");
    super({ runId });
    this.#graph = graph;
    this.#runId = runId;
  }

  #id(kind, id) {
    return graphId(this.#runId, kind, id);
  }

  #node(kind, id, properties) {
    return {
      id: this.#id(kind, id),
      labels: [`Rime${kind}`],
      namespace: this.#runId,
      properties,
    };
  }

  #edge(srcKind, srcId, predicate, dstKind, dstId, properties = {}) {
    return {
      srcId: this.#id(srcKind, srcId),
      predicate,
      dstId: this.#id(dstKind, dstId),
      provenance: `rime:${this.#runId}`,
      namespace: this.#runId,
      properties,
    };
  }

  async #writeEvaluation(candidate) {
    const evaluationId = candidate.id;
    const evaluation = candidate.evaluation;
    const gateEntries = Object.entries(evaluation.gates ?? {});
    const metricEntries = Object.entries(evaluation.metrics ?? {});
    const gateArtifactEntries = gateEntries.flatMap(([gateName, gate]) =>
      gate.evidence.map((evidence, ordinal) => ({
        ownerKind: "GateEvaluation",
        ownerId: `${candidate.id}.${gateName}`,
        artifactId: `${candidate.id}.gate.${gateName}.${ordinal}`,
        evidence,
      })),
    );
    const metricArtifactEntries = metricEntries.flatMap(
      ([metricName, measurement]) =>
        measurement.evidence.map((evidence, ordinal) => ({
          ownerKind: "MetricMeasurement",
          ownerId: `${candidate.id}.${metricName}`,
          artifactId: `${candidate.id}.metric.${metricName}.${ordinal}`,
          evidence,
        })),
    );
    const artifactEntries = [...gateArtifactEntries, ...metricArtifactEntries];
    const nodes = [
      this.#node("Evaluation", evaluationId, {
        status: evaluation.status,
        ...(evaluation.status === "valid"
          ? { score: evaluation.score }
          : {
              error: evaluation.error,
              failedGates: evaluation.failedGates ?? [],
              failedConstraints: evaluation.failedConstraints ?? [],
            }),
      }),
      ...gateEntries.map(([name, gate]) =>
        this.#node("GateEvaluation", `${candidate.id}.${name}`, {
          name,
          passed: gate.passed,
          evaluator: gate.evaluator,
        }),
      ),
      ...metricEntries.map(([name, measurement]) =>
        this.#node("MetricMeasurement", `${candidate.id}.${name}`, {
          name,
          value: measurement.value,
          unit: measurement.unit,
          utility: measurement.utility,
          evaluator: measurement.evaluator,
        }),
      ),
      ...artifactEntries.map(({ artifactId, evidence }) =>
        this.#node("Artifact", artifactId, {
          artifact: evidence.artifact,
          kind: evidence.kind,
          summary: evidence.summary,
        }),
      ),
    ];
    const edges = [
      this.#edge(
        "Candidate",
        candidate.id,
        "has_evaluation",
        "Evaluation",
        evaluationId,
      ),
      ...gateEntries.map(([name]) =>
        this.#edge(
          "Evaluation",
          evaluationId,
          "has_gate",
          "GateEvaluation",
          `${candidate.id}.${name}`,
        ),
      ),
      ...metricEntries.map(([name]) =>
        this.#edge(
          "Evaluation",
          evaluationId,
          "has_metric",
          "MetricMeasurement",
          `${candidate.id}.${name}`,
        ),
      ),
      ...artifactEntries.map(({ ownerKind, ownerId, artifactId }) =>
        this.#edge(ownerKind, ownerId, "supported_by", "Artifact", artifactId),
      ),
    ];
    await this.#graph.insertNodes(nodes);
    await this.#graph.insertEdges(edges);
  }

  /** Creates graph storage and records the run and evaluated baseline. */
  async initialize(root) {
    const staged = new RimeStateStore({ runId: this.#runId });
    await staged.initialize(root);
    await this.#graph.create();
    await this.#graph.insertNodes([
      this.#node("Run", this.#runId, {}),
      this.#node("Candidate", root.id, candidateProperties(root)),
    ]);
    await this.#graph.insertEdges([
      this.#edge("Run", this.#runId, "has_candidate", "Candidate", root.id),
    ]);
    await this.#writeEvaluation(root);
    await super.initialize(root);
  }

  /** Records a policy wave and all scheduler-provided attempt identities. */
  async openWave(wave) {
    const staged = RimeStateStore.hydrate(this.checkpoint());
    await staged.openWave(wave);
    await this.#graph.insertNodes([
      this.#node("Wave", wave.id, {
        ordinal: wave.ordinal,
        operation: wave.operation,
        parentCandidateId: wave.parentId,
      }),
      ...wave.attempts.map((attempt) =>
        this.#node("Attempt", attempt.id, {
          ordinal: attempt.ordinal,
          preferences: attempt.preferences ?? [],
          agent: attempt.agent ?? null,
          model: attempt.model ?? null,
          workspaceId: attempt.workspaceId ?? null,
          leaseGeneration: attempt.leaseGeneration ?? null,
          state: "scheduled",
        }),
      ),
      ...wave.attempts
        .filter(({ workspaceId }) => workspaceId)
        .map((attempt) =>
          this.#node("Workspace", attempt.workspaceId, {
            state: attempt.workspaceState ?? "active",
          }),
        ),
    ]);
    await this.#graph.insertEdges([
      this.#edge("Run", this.#runId, "has_wave", "Wave", wave.id),
      this.#edge("Wave", wave.id, "uses_parent", "Candidate", wave.parentId),
      ...wave.attempts.map((attempt) =>
        this.#edge("Wave", wave.id, "dispatched", "Attempt", attempt.id),
      ),
      ...wave.attempts
        .filter(({ workspaceId }) => workspaceId)
        .map((attempt) =>
          this.#edge(
            "Attempt",
            attempt.id,
            "executed_in",
            "Workspace",
            attempt.workspaceId,
          ),
        ),
    ]);
    await super.openWave(wave);
  }

  /** Persists a candidate, normalized evaluation evidence, and attempt result. */
  async recordAttempt({ waveId, attemptId, candidate, runtime = {} }) {
    const staged = RimeStateStore.hydrate(this.checkpoint());
    await staged.recordAttempt({ waveId, attemptId, candidate, runtime });
    await this.#graph.insertNodes([
      this.#node("Candidate", candidate.id, candidateProperties(candidate)),
    ]);
    await this.#writeEvaluation(candidate);
    await this.#graph.insertEdges([
      this.#edge(
        "Candidate",
        candidate.id,
        "derived_from",
        "Candidate",
        candidate.parentId,
        { operation: candidate.operation },
      ),
      this.#edge(
        "Attempt",
        attemptId,
        "produced",
        "Candidate",
        candidate.id,
        runtime,
      ),
    ]);
    await super.recordAttempt({ waveId, attemptId, candidate, runtime });
  }

  /** Persists a terminal fleet failure without inventing a candidate. */
  async failAttempt({ waveId, attemptId, failureReason, runtime = {} }) {
    const staged = RimeStateStore.hydrate(this.checkpoint());
    await staged.failAttempt({ waveId, attemptId, failureReason, runtime });
    const markerId = `${attemptId}.failure`;
    await this.#graph.insertNodes([
      this.#node("AttemptFailure", markerId, {
        failureReason,
        runtime,
      }),
    ]);
    await this.#graph.insertEdges([
      this.#edge("Attempt", attemptId, "failed_by", "AttemptFailure", markerId),
    ]);
    await super.failAttempt({ waveId, attemptId, failureReason, runtime });
  }

  /** Appends evidence that the policy consumed a buggy leaf for debugging. */
  async markExpanded(candidateId) {
    const staged = RimeStateStore.hydrate(this.checkpoint());
    await staged.markExpanded(candidateId);
    const markerId = `${candidateId}.expanded`;
    await this.#graph.insertNodes([
      this.#node("CandidateExpanded", markerId, { expanded: true }),
    ]);
    await this.#graph.insertEdges([
      this.#edge(
        "Candidate",
        candidateId,
        "expanded_by",
        "CandidateExpanded",
        markerId,
      ),
    ]);
    await super.markExpanded(candidateId);
  }

  /** Appends an auditable workspace lifecycle transition. */
  async markWorkspaceState({ workspaceId, state, reason }) {
    const staged = RimeStateStore.hydrate(this.checkpoint());
    await staged.markWorkspaceState({ workspaceId, state, reason });
    const markerId = `${workspaceId}.${state}`;
    await this.#graph.insertNodes([
      this.#node("WorkspaceLifecycle", markerId, { state, reason }),
    ]);
    await this.#graph.insertEdges([
      this.#edge(
        "Workspace",
        workspaceId,
        "transitioned_by",
        "WorkspaceLifecycle",
        markerId,
      ),
    ]);
    await super.markWorkspaceState({ workspaceId, state, reason });
  }

  /** Writes the visibility fence after all wave evidence has been appended. */
  async closeWave(waveId) {
    const staged = RimeStateStore.hydrate(this.checkpoint());
    await staged.closeWave(waveId);
    await this.#graph.insertNodes([
      this.#node("WaveClosed", waveId, { closed: true }),
    ]);
    await this.#graph.insertEdges([
      this.#edge("Wave", waveId, "closed_by", "WaveClosed", waveId),
    ]);
    await super.closeWave(waveId);
  }
}
