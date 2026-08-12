/** Closed-wave experiment state and bounded context projections for RIME. */

import {
  observeExecution,
  requireModelIdentity,
  requireWorkerPreferences,
} from "./fleet-policy.mjs";

function requireId(value, label) {
  if (typeof value !== "string" || !/^[a-zA-Z0-9_.-]+$/.test(value)) {
    throw new Error(
      `${label} must contain only letters, numbers, dots, underscores, and dashes.`,
    );
  }
  return value;
}

function requireLimit(value, label) {
  if (!Number.isInteger(value) || value < 1) {
    throw new Error(`${label} must be a positive integer.`);
  }
  return value;
}

function clone(value) {
  return structuredClone(value);
}

function frozenArray(values) {
  return Object.freeze(values.map((value) => Object.freeze(value)));
}

function gateResults(evaluation) {
  return Object.fromEntries(
    Object.entries(evaluation.gates ?? {}).map(([name, gate]) => [
      name,
      gate.passed,
    ]),
  );
}

function candidateRow(candidate, waveId, childCount) {
  const valid = candidate.evaluation.status === "valid";
  return {
    candidateId: candidate.id,
    parentId: candidate.parentId,
    operation: candidate.operation,
    status: candidate.evaluation.status,
    debugDepth: candidate.debugDepth,
    metrics: candidate.evaluation.metrics ?? Object.freeze({}),
    score: valid ? candidate.evaluation.score : null,
    gates: Object.freeze(gateResults(candidate.evaluation)),
    childCount,
    isLeaf: childCount === 0,
    waveId,
    eligibility: valid
      ? "improve"
      : childCount === 0 && !candidate.expanded
        ? "debug"
        : "none",
  };
}

function attemptRow(wave, attempt) {
  return {
    attemptId: attempt.id,
    waveId: wave.id,
    parentCandidateId: wave.parentId,
    operation: wave.operation,
    preferences: attempt.preferences ?? Object.freeze([]),
    agent: attempt.agent ?? null,
    model: attempt.model ?? null,
    executedAgent: attempt.runtime?.execution?.agent ?? null,
    executedModel: attempt.runtime?.execution?.model ?? null,
    assignmentMatched: attempt.runtime?.assignmentMatched ?? null,
    state: attempt.state,
    startedAt: attempt.runtime?.startedAt ?? null,
    finishedAt: attempt.runtime?.finishedAt ?? null,
    tokens: attempt.runtime?.tokens ?? null,
    cost: attempt.runtime?.cost ?? null,
    workspaceId: attempt.workspaceId ?? null,
    workspaceState: attempt.workspaceState ?? null,
    candidateId: attempt.candidate?.id ?? null,
    failureReason: attempt.failureReason ?? null,
    leaseGeneration: attempt.leaseGeneration ?? null,
  };
}

function validateWave(wave) {
  requireId(wave?.id, "wave id");
  requireId(wave?.parentId, "parent candidate id");
  if (!Number.isInteger(wave.ordinal) || wave.ordinal < 0) {
    throw new Error("wave ordinal must be a non-negative integer.");
  }
  if (!Array.isArray(wave.attempts) || wave.attempts.length === 0) {
    throw new Error("wave attempts must be a non-empty array.");
  }
  const ids = new Set();
  for (const attempt of wave.attempts) {
    requireId(attempt.id, "attempt id");
    if (ids.has(attempt.id))
      throw new Error(`Duplicate attempt: ${attempt.id}.`);
    ids.add(attempt.id);
    requireWorkerPreferences(attempt.preferences);
    requireModelIdentity(
      {
        agent: attempt.agent,
        model: attempt.model,
      },
      `Attempt ${attempt.id} assignment`,
    );
  }
}

/** Maintains the coordinator projection while exposing only closed waves to policy. */
export class RimeStateStore {
  #runId;
  #root = null;
  #waves = [];

  /** Creates state for one independently identified RIME run. */
  constructor({ runId }) {
    this.#runId = requireId(runId, "RIME run id");
  }

  /** Restores validated coordinator state without publishing unfinished candidates. */
  static hydrate(checkpoint) {
    if (!checkpoint || typeof checkpoint !== "object") {
      throw new Error("RIME checkpoint must be an object.");
    }
    const store = new RimeStateStore({ runId: checkpoint.runId });
    if (!checkpoint.root || !Array.isArray(checkpoint.waves)) {
      throw new Error("RIME checkpoint requires root and waves.");
    }
    store.#root = clone(checkpoint.root);
    store.#waves = clone(checkpoint.waves);
    if (store.#root.parentId !== null || store.#root.operation !== "root") {
      throw new Error("RIME checkpoint root is invalid.");
    }
    const waveIds = new Set();
    const attemptIds = new Set();
    const visibleCandidates = new Set([store.#root.id]);
    let sawOpenWave = false;
    for (const [index, wave] of store.#waves.entries()) {
      validateWave(wave);
      if (wave.ordinal !== index || waveIds.has(wave.id)) {
        throw new Error("RIME checkpoint wave order is invalid.");
      }
      if (!visibleCandidates.has(wave.parentId)) {
        throw new Error(`Wave parent is not visible: ${wave.parentId}.`);
      }
      if (sawOpenWave) {
        throw new Error(
          "A RIME checkpoint may contain only one final open wave.",
        );
      }
      sawOpenWave = !wave.closed;
      waveIds.add(wave.id);
      for (const attempt of wave.attempts) {
        if (attemptIds.has(attempt.id)) {
          throw new Error(`Duplicate attempt: ${attempt.id}.`);
        }
        attemptIds.add(attempt.id);
        if (wave.closed && attempt.state === "scheduled") {
          throw new Error(`Closed wave has unfinished attempt: ${attempt.id}.`);
        }
        if (wave.closed && attempt.candidate) {
          const observed = observeExecution(attempt.runtime?.execution, {
            agent: attempt.agent,
            model: attempt.model,
          });
          if (
            attempt.runtime.assignmentMatched !== observed.assignmentMatched
          ) {
            throw new Error(
              `Attempt ${attempt.id} assignment observation is invalid.`,
            );
          }
          if (
            attempt.candidate.parentId !== wave.parentId ||
            visibleCandidates.has(attempt.candidate.id)
          ) {
            throw new Error("RIME checkpoint candidate lineage is invalid.");
          }
          visibleCandidates.add(attempt.candidate.id);
        }
      }
    }
    return store;
  }

  /** Records the evaluated baseline that anchors every candidate lineage. */
  async initialize(root) {
    if (this.#root) throw new Error("RIME state is already initialized.");
    if (root?.parentId !== null || root?.operation !== "root") {
      throw new Error("RIME root must be a root candidate without a parent.");
    }
    this.#root = clone(root);
  }

  /** Schedules one atomic policy decision as an open parallel wave. */
  async openWave(wave) {
    if (!this.#root) throw new Error("RIME state must be initialized first.");
    validateWave(wave);
    if (this.#waves.some(({ id }) => id === wave.id)) {
      throw new Error(`Duplicate wave: ${wave.id}.`);
    }
    if (this.#waves.some(({ closed }) => !closed)) {
      throw new Error("RIME cannot open a wave while another wave is open.");
    }
    this.#waves.push({
      ...clone(wave),
      closed: false,
      attempts: wave.attempts.map((attempt) => ({
        ...clone(attempt),
        state: "scheduled",
        candidate: null,
        runtime: null,
        failureReason: null,
        workspaceState: attempt.workspaceId
          ? (attempt.workspaceState ?? "active")
          : null,
      })),
    });
  }

  /** Attaches one evaluated candidate and runtime measurements to its attempt. */
  async recordAttempt({ waveId, attemptId, candidate, runtime = {} }) {
    const wave = this.#waves.find(({ id }) => id === waveId);
    if (!wave) throw new Error(`Unknown wave: ${waveId}.`);
    if (wave.closed) throw new Error(`Wave is already closed: ${waveId}.`);
    const attempt = wave.attempts.find(({ id }) => id === attemptId);
    if (!attempt) throw new Error(`Unknown attempt: ${attemptId}.`);
    if (attempt.candidate)
      throw new Error(`Attempt already recorded: ${attemptId}.`);
    if (candidate.parentId !== wave.parentId) {
      throw new Error("Candidate parent must match its wave parent.");
    }
    const observed = observeExecution(runtime.execution, {
      agent: attempt.agent,
      model: attempt.model,
    });
    if (runtime.assignmentMatched !== observed.assignmentMatched) {
      throw new Error(
        "Attempt assignment observation does not match execution.",
      );
    }
    const candidateExists =
      this.#root.id === candidate.id ||
      this.#waves.some(({ attempts }) =>
        attempts.some(
          ({ candidate: recorded }) => recorded?.id === candidate.id,
        ),
      );
    if (candidateExists)
      throw new Error(`Duplicate candidate: ${candidate.id}.`);
    attempt.candidate = clone(candidate);
    attempt.runtime = clone(runtime);
    attempt.state = "completed";
  }

  /** Marks a failed attempt without manufacturing a candidate result. */
  async failAttempt({ waveId, attemptId, failureReason, runtime = {} }) {
    const wave = this.#waves.find(({ id }) => id === waveId);
    const attempt = wave?.attempts.find(({ id }) => id === attemptId);
    if (!attempt || wave.closed)
      throw new Error(`Unknown open attempt: ${attemptId}.`);
    if (typeof failureReason !== "string" || !failureReason.trim()) {
      throw new Error("Attempt failure requires a reason.");
    }
    attempt.failureReason = failureReason.trim();
    attempt.runtime = clone(runtime);
    attempt.state = "failed";
  }

  /** Retires one buggy leaf after the policy spends its debug expansion. */
  async markExpanded(candidateId) {
    if (this.#root?.id === candidateId) {
      this.#root.expanded = true;
      return;
    }
    for (const wave of this.#waves) {
      if (!wave.closed) continue;
      const attempt = wave.attempts.find(
        ({ candidate }) => candidate?.id === candidateId,
      );
      if (attempt) {
        attempt.candidate.expanded = true;
        return;
      }
    }
    throw new Error(`Unknown visible candidate: ${candidateId}.`);
  }

  /** Records the latest lifecycle state for one opaque attempt workspace. */
  async markWorkspaceState({ workspaceId, state, reason }) {
    const allowedStates = new Set([
      "active",
      "promoted",
      "retained",
      "removed",
    ]);
    if (!allowedStates.has(state)) {
      throw new Error(`Unknown workspace state: ${state}.`);
    }
    const attempt = this.#waves
      .flatMap(({ attempts }) => attempts)
      .find((candidate) => candidate.workspaceId === workspaceId);
    if (!attempt) throw new Error(`Unknown workspace: ${workspaceId}.`);
    if (attempt.workspaceState === "removed") {
      throw new Error(`Workspace is already removed: ${workspaceId}.`);
    }
    attempt.workspaceState = state;
    attempt.workspaceStateReason = reason;
  }

  /** Publishes a complete wave as the next policy-visible state boundary. */
  async closeWave(waveId) {
    const wave = this.#waves.find(({ id }) => id === waveId);
    if (!wave) throw new Error(`Unknown wave: ${waveId}.`);
    if (wave.closed) throw new Error(`Wave is already closed: ${waveId}.`);
    if (wave.attempts.some(({ state }) => state === "scheduled")) {
      throw new Error(`Wave has unfinished attempts: ${waveId}.`);
    }
    wave.attempts.sort((left, right) => left.ordinal - right.ordinal);
    wave.closed = true;
  }

  /** Returns full coordinator data suitable for durable checkpoint storage. */
  checkpoint() {
    if (!this.#root) throw new Error("RIME state is not initialized.");
    return Object.freeze({
      runId: this.#runId,
      root: clone(this.#root),
      waves: clone(this.#waves),
    });
  }

  /** Returns the AIDE solution tree formed only from closed waves. */
  searchSnapshot() {
    if (!this.#root) throw new Error("RIME state is not initialized.");
    const candidates = [clone(this.#root)];
    for (const wave of this.#waves) {
      if (!wave.closed) continue;
      for (const attempt of wave.attempts) {
        if (attempt.candidate) candidates.push(clone(attempt.candidate));
      }
    }
    return Object.freeze({ candidates: frozenArray(candidates) });
  }

  /** Derives immutable policy and fleet matrices from the current run state. */
  snapshot() {
    const candidates = this.searchSnapshot().candidates;
    const children = new Map(candidates.map(({ id }) => [id, 0]));
    for (const candidate of candidates) {
      if (candidate.parentId) {
        children.set(
          candidate.parentId,
          (children.get(candidate.parentId) ?? 0) + 1,
        );
      }
    }
    const candidateWaves = new Map();
    for (const wave of this.#waves) {
      if (!wave.closed) continue;
      for (const attempt of wave.attempts) {
        if (attempt.candidate)
          candidateWaves.set(attempt.candidate.id, wave.id);
      }
    }
    return Object.freeze({
      runId: this.#runId,
      candidateMatrix: frozenArray(
        candidates.map((candidate) =>
          candidateRow(
            candidate,
            candidateWaves.get(candidate.id) ?? null,
            children.get(candidate.id) ?? 0,
          ),
        ),
      ),
      attemptMatrix: frozenArray(
        this.#waves.flatMap((wave) =>
          wave.attempts.map((attempt) => attemptRow(wave, attempt)),
        ),
      ),
      waves: frozenArray(
        this.#waves.map(({ id, ordinal, operation, parentId, closed }) => ({
          id,
          ordinal,
          operation,
          parentId,
          closed,
        })),
      ),
    });
  }
}

/** Compiles bounded lineage, sibling outcomes, and artifact summaries for an operator. */
export function compileCandidateContext(
  checkpoint,
  { candidateId, maxCandidates = 8, maxArtifacts = 12 },
) {
  requireLimit(maxCandidates, "maxCandidates");
  requireLimit(maxArtifacts, "maxArtifacts");
  const state = RimeStateStore.hydrate(checkpoint);
  const candidates = state.searchSnapshot().candidates;
  const byId = new Map(
    candidates.map((candidate) => [candidate.id, candidate]),
  );
  const selected = byId.get(candidateId);
  if (!selected) throw new Error(`Unknown visible candidate: ${candidateId}.`);

  const ordered = [];
  const seen = new Set();
  const include = (candidate) => {
    if (
      candidate &&
      !seen.has(candidate.id) &&
      ordered.length < maxCandidates
    ) {
      seen.add(candidate.id);
      ordered.push(candidate);
    }
  };
  include(selected);
  let ancestor = selected.parentId ? byId.get(selected.parentId) : null;
  while (ancestor && ordered.length < maxCandidates) {
    include(ancestor);
    ancestor = ancestor.parentId ? byId.get(ancestor.parentId) : null;
  }
  for (const candidate of candidates) {
    if (candidate.parentId === selected.parentId) include(candidate);
  }

  const artifacts = [];
  for (const candidate of ordered) {
    for (const gate of Object.values(candidate.evaluation.gates ?? {})) {
      for (const evidence of gate.evidence) {
        if (artifacts.length >= maxArtifacts) break;
        artifacts.push({
          artifact: evidence.artifact,
          kind: evidence.kind,
          summary: evidence.summary,
        });
      }
    }
  }
  return Object.freeze({
    candidates: frozenArray(
      ordered.map((candidate) => ({
        candidateId: candidate.id,
        parentId: candidate.parentId,
        operation: candidate.operation,
        summary: candidate.summary,
        evaluation:
          candidate.evaluation.status === "valid"
            ? {
                status: "valid",
                score: candidate.evaluation.score,
                metrics: Object.fromEntries(
                  Object.entries(candidate.evaluation.metrics).map(
                    ([name, measurement]) => [
                      name,
                      {
                        value: measurement.value,
                        unit: measurement.unit,
                        utility: measurement.utility,
                      },
                    ],
                  ),
                ),
              }
            : {
                status: "buggy",
                error: candidate.evaluation.error,
                failedGates: candidate.evaluation.failedGates ?? [],
                failedConstraints: candidate.evaluation.failedConstraints ?? [],
                metrics: Object.fromEntries(
                  Object.entries(candidate.evaluation.metrics ?? {}).map(
                    ([name, measurement]) => [
                      name,
                      {
                        value: measurement.value,
                        unit: measurement.unit,
                        utility: measurement.utility,
                      },
                    ],
                  ),
                ),
              },
      })),
    ),
    artifacts: frozenArray(artifacts),
  });
}
