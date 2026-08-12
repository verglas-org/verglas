/** Budgeted parallel execution of the AIDE solution-tree algorithm. */

import { CandidateGraph } from "./graph.mjs";
import { normalizeSearchConfig, selectProposalBatch } from "./policy.mjs";
import { compileCandidateContext } from "./state-store.mjs";
import {
  WORKER_PREFERENCES,
  assignPreferredWorker,
  observeExecution,
  requireFleet,
} from "./fleet-policy.mjs";
import {
  WorkspaceLifecycle,
  optionalWorkspacePool,
} from "./workspace-lifecycle.mjs";

function compactMetrics(metrics) {
  return Object.fromEntries(
    Object.entries(metrics).map(([name, measurement]) => [
      name,
      {
        value: measurement.value,
        unit: measurement.unit,
        utility: measurement.utility,
      },
    ]),
  );
}

function defaultSummarize(snapshot) {
  return snapshot.candidates.map((candidate) => ({
    candidateId: candidate.id,
    operation: candidate.operation,
    summary: candidate.summary,
    evaluation:
      candidate.evaluation.status === "valid"
        ? {
            status: "valid",
            score: candidate.evaluation.score,
            metrics: compactMetrics(candidate.evaluation.metrics),
          }
        : {
            status: "buggy",
            error: candidate.evaluation.error,
            ...(candidate.evaluation.failedGates
              ? { failedGates: candidate.evaluation.failedGates }
              : {}),
            ...(candidate.evaluation.failedConstraints
              ? { failedConstraints: candidate.evaluation.failedConstraints }
              : {}),
            ...(candidate.evaluation.metrics
              ? { metrics: compactMetrics(candidate.evaluation.metrics) }
              : {}),
          },
  }));
}

function requireFunction(value, label) {
  if (typeof value !== "function") {
    throw new Error(`${label} must be a function.`);
  }
  return value;
}

function requireProposalResult(result) {
  if (!result || typeof result !== "object" || !("solution" in result)) {
    throw new Error("The fleet worker must return a solution and summary.");
  }
  if (typeof result.summary !== "string" || !result.summary.trim()) {
    throw new Error("The fleet worker must return a concise summary.");
  }
  if (
    result.runtime !== undefined &&
    (!result.runtime || typeof result.runtime !== "object")
  ) {
    throw new Error("Proposal runtime measurements must be an object.");
  }
  return {
    solution: result.solution,
    summary: result.summary.trim(),
    execution: result.execution,
    runtime: result.runtime ?? {},
  };
}

function requireStateStore(store) {
  if (store === undefined) return null;
  for (const method of [
    "initialize",
    "openWave",
    "recordAttempt",
    "closeWave",
    "markExpanded",
    "searchSnapshot",
    "snapshot",
    "checkpoint",
    "markWorkspaceState",
  ]) {
    requireFunction(store?.[method], `stateStore.${method}`);
  }
  return store;
}

function requireAbortSignal(signal) {
  if (signal === undefined) return null;
  if (
    !signal ||
    typeof signal.aborted !== "boolean" ||
    typeof signal.throwIfAborted !== "function"
  ) {
    throw new Error("signal must be an AbortSignal.");
  }
  return signal;
}

/** Runs parallel proposal waves while preserving AIDE's selection priority. */
export class RimeSearch {
  #config;
  #fleet;
  #evaluate;
  #summarize;
  #stateStore;
  #random;
  #objectiveWeights;
  #objective;
  #workspacePool;
  #signal;

  /** Creates a search engine over governed fleet and evaluation boundaries. */
  constructor({
    fleet,
    evaluate,
    summarize,
    stateStore,
    random = Math.random,
    objective,
    objectiveWeights,
    workspacePool,
    signal,
    ...config
  }) {
    this.#config = normalizeSearchConfig(config);
    this.#fleet = requireFleet(fleet);
    this.#evaluate = requireFunction(evaluate, "evaluate");
    this.#summarize =
      summarize === undefined ? null : requireFunction(summarize, "summarize");
    this.#stateStore = requireStateStore(stateStore);
    this.#random = requireFunction(random, "random");
    if (objective && objectiveWeights) {
      throw new Error("Provide objective or objectiveWeights, not both.");
    }
    if (objective && typeof objective.normalize !== "function") {
      throw new Error("objective must provide normalize().");
    }
    this.#objective = objective;
    this.#objectiveWeights = objectiveWeights;
    this.#workspacePool = optionalWorkspacePool(workspacePool);
    this.#signal = requireAbortSignal(signal);
  }

  /** Evaluates proposals in parallel and returns the best valid candidate. */
  async run(baseline) {
    const graph = new CandidateGraph(baseline, {
      objective: this.#objective,
      objectiveWeights: this.#objectiveWeights,
    });
    if (this.#stateStore) {
      await this.#stateStore.initialize(graph.snapshot().candidates[0]);
    }
    return this.#search(graph, 0);
  }

  /** Continues from the closed waves in the injected state store. */
  async resume() {
    if (!this.#stateStore) {
      throw new Error("RIME resume requires a stateStore.");
    }
    const graph = CandidateGraph.hydrate(this.#stateStore.searchSnapshot(), {
      objective: this.#objective,
      objectiveWeights: this.#objectiveWeights,
    });
    return this.#search(graph, graph.snapshot().candidates.length - 1);
  }

  /** Executes policy waves against one fresh or hydrated candidate graph. */
  async #search(graph, initialConsumedCandidates) {
    let consumedCandidates = initialConsumedCandidates;
    let waveOrdinal = this.#stateStore?.snapshot().waves.length ?? 0;
    const workspaces = this.#workspacePool
      ? new WorkspaceLifecycle({
          workspacePool: this.#workspacePool,
          stateStore: this.#stateStore,
        })
      : null;

    try {
      while (consumedCandidates < this.#config.candidateBudget) {
        this.#signal?.throwIfAborted();
        const snapshot = graph.snapshot();
        const proposals = selectProposalBatch(snapshot, {
          ...this.#config,
          remainingCandidates:
            this.#config.candidateBudget - consumedCandidates,
          random: this.#random,
        });
        if (proposals.length === 0) break;

        const waveId = `wave-${waveOrdinal}`;
        const assignments = await Promise.all(
          proposals.map((proposal, index) =>
            assignPreferredWorker(this.#fleet, {
              role: "worker",
              operation: proposal.operation,
              waveId,
              attemptId: `attempt-${waveOrdinal}-${index}`,
              ordinal: index,
              signal: this.#signal,
            }),
          ),
        );
        if (proposals[0].operation === "debug") {
          graph.markExpanded(proposals[0].parentId);
          if (this.#stateStore) {
            await this.#stateStore.markExpanded(proposals[0].parentId);
          }
        }
        const attempts = proposals.map((proposal, index) => ({
          id: `attempt-${waveOrdinal}-${index}`,
          ordinal: index,
          preferences: WORKER_PREFERENCES,
          agent: assignments[index].agent,
          model: assignments[index].model,
        }));
        const attemptWorkspaces = workspaces
          ? await workspaces.allocateBatch({
              waveId,
              proposals,
              attempts,
              signal: this.#signal,
            })
          : proposals.map(() => null);
        for (const [index, workspace] of attemptWorkspaces.entries()) {
          if (workspace) {
            attempts[index].workspaceId = workspace.id;
            attempts[index].workspaceState = "active";
          }
        }
        if (this.#stateStore) {
          await this.#stateStore.openWave({
            id: waveId,
            ordinal: waveOrdinal,
            operation: proposals[0].operation,
            parentId: proposals[0].parentId,
            attempts,
          });
        }
        const evidence = this.#summarize
          ? await this.#summarize(snapshot)
          : this.#stateStore
            ? compileCandidateContext(this.#stateStore.checkpoint(), {
                candidateId: proposals[0].parentId,
              })
            : defaultSummarize(snapshot);
        const results = await Promise.all(
          proposals.map(async (proposal, index) => {
            this.#signal?.throwIfAborted();
            const proposed = requireProposalResult(
              await this.#fleet.execute({
                ...proposal,
                evidence,
                waveId,
                attemptId: attempts[index].id,
                assignment: assignments[index],
                workspace: attemptWorkspaces[index],
                signal: this.#signal,
              }),
            );
            const observation = observeExecution(
              proposed.execution,
              assignments[index],
            );
            const runtime = { ...proposed.runtime, ...observation };
            this.#signal?.throwIfAborted();
            const evaluation = await this.#evaluate({
              ...proposed,
              runtime,
              proposal,
              parent: proposal.parent,
              workspace: attemptWorkspaces[index],
              signal: this.#signal,
            });
            this.#signal?.throwIfAborted();
            return {
              ...proposal,
              ...proposed,
              runtime,
              evaluation,
              attemptId: attempts[index].id,
              workspace: attemptWorkspaces[index],
            };
          }),
        );

        for (const result of results) {
          const candidate = graph.record({
            parentId: result.parentId,
            operation: result.operation,
            solution: result.solution,
            summary: result.summary,
            evaluation: result.evaluation,
            debugDepth: result.debugDepth,
          });
          if (this.#stateStore) {
            await this.#stateStore.recordAttempt({
              waveId,
              attemptId: result.attemptId,
              candidate,
              runtime: result.runtime ?? {},
            });
          }
          if (workspaces) {
            workspaces.associate(result.workspace.id, candidate.id);
          }
        }
        if (this.#stateStore) await this.#stateStore.closeWave(waveId);
        consumedCandidates += results.length;
        waveOrdinal += 1;
        if (workspaces) {
          const current = graph.snapshot().candidates;
          const best = graph.bestValid();
          const retained = new Set(best ? [best.id] : []);
          for (const candidate of current) {
            if (
              candidate.operation !== "root" &&
              candidate.evaluation.status === "buggy" &&
              !candidate.expanded &&
              candidate.debugDepth <= this.#config.maxDebugDepth
            ) {
              retained.add(candidate.id);
            }
          }
          await workspaces.prune(retained);
        }
      }
      this.#signal?.throwIfAborted();
    } catch (error) {
      if (workspaces) return workspaces.abort(error);
      throw error;
    }

    const best = graph.bestValid() ?? null;
    const promotion = workspaces ? await workspaces.finish(best) : null;
    return Object.freeze({
      best,
      graph: graph.snapshot(),
      state: this.#stateStore?.snapshot() ?? null,
      consumedCandidates,
      promotion,
    });
  }
}
