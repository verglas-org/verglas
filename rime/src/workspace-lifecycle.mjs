/** Ownership, pruning, promotion, and exhaustive cleanup of speculative workspaces. */

function requireMethod(target, method) {
  if (typeof target?.[method] !== "function") {
    throw new Error(`workspacePool.${method} must be a function.`);
  }
}

function requireWorkspace(workspace) {
  if (
    !workspace ||
    typeof workspace !== "object" ||
    typeof workspace.id !== "string" ||
    !workspace.id.trim()
  ) {
    throw new Error("workspacePool.create must return a workspace with an id.");
  }
  return Object.freeze({ ...workspace, id: workspace.id.trim() });
}

async function settledErrors(promises) {
  const results = await Promise.allSettled(promises);
  return results
    .filter(({ status }) => status === "rejected")
    .map(({ reason }) => reason);
}

/** Coordinates resources created by an injected host workspace pool. */
export class WorkspaceLifecycle {
  #pool;
  #stateStore;
  #entries = new Map();

  /** Validates the host boundary and restores active checkpoint identities. */
  constructor({ workspacePool, stateStore }) {
    requireMethod(workspacePool, "create");
    requireMethod(workspacePool, "promote");
    requireMethod(workspacePool, "remove");
    this.#pool = workspacePool;
    this.#stateStore = stateStore;
    for (const attempt of stateStore?.snapshot().attemptMatrix ?? []) {
      if (
        attempt.workspaceId &&
        attempt.workspaceState !== "removed" &&
        attempt.candidateId
      ) {
        this.#entries.set(attempt.workspaceId, {
          workspace: Object.freeze({ id: attempt.workspaceId }),
          candidateId: attempt.candidateId,
          removalAttempted: false,
        });
      }
    }
  }

  /** Allocates one isolated workspace per attempt and rolls back partial batches. */
  async allocateBatch({ waveId, proposals, attempts, signal }) {
    const results = await Promise.allSettled(
      proposals.map((proposal, index) => {
        const parentWorkspace = this.workspaceForCandidate(proposal.parentId);
        return this.#pool.create({
          waveId,
          attemptId: attempts[index].id,
          parent: proposal.parent,
          parentWorkspaceId: parentWorkspace?.id ?? null,
          signal,
        });
      }),
    );
    const errors = [];
    const allocated = [];
    for (const result of results) {
      if (result.status === "rejected") {
        errors.push(result.reason);
        continue;
      }
      try {
        const workspace = requireWorkspace(result.value);
        if (
          this.#entries.has(workspace.id) ||
          allocated.some(({ id }) => id === workspace.id)
        ) {
          throw new Error(`Duplicate workspace id: ${workspace.id}.`);
        }
        allocated.push(workspace);
      } catch (error) {
        errors.push(error);
      }
    }
    for (const workspace of allocated) {
      this.#entries.set(workspace.id, {
        workspace,
        candidateId: null,
        removalAttempted: false,
      });
    }
    if (errors.length > 0) {
      const cleanupErrors = await this.#removeEntries(
        allocated.map(({ id }) => this.#entries.get(id)),
        "allocation-failed",
      );
      throw new AggregateError(
        [...errors, ...cleanupErrors],
        "Workspace allocation failed.",
      );
    }
    return Object.freeze(allocated);
  }

  /** Associates a completed attempt workspace with its candidate node. */
  associate(workspaceId, candidateId) {
    const entry = this.#entries.get(workspaceId);
    if (!entry) throw new Error(`Unknown workspace: ${workspaceId}.`);
    if (entry.candidateId) {
      throw new Error(`Workspace already has a candidate: ${workspaceId}.`);
    }
    entry.candidateId = candidateId;
  }

  /** Returns the live workspace for a candidate when RIME owns one. */
  workspaceForCandidate(candidateId) {
    return [...this.#entries.values()].find(
      (entry) => entry.candidateId === candidateId && !entry.removalAttempted,
    )?.workspace;
  }

  /** Removes candidates that can no longer be selected as AIDE parents. */
  async prune(retainedCandidateIds) {
    const entries = [...this.#entries.values()].filter(
      (entry) =>
        entry.candidateId &&
        !retainedCandidateIds.has(entry.candidateId) &&
        !entry.removalAttempted,
    );
    const errors = await this.#removeEntries(entries, "pruned");
    if (errors.length > 0) {
      throw new AggregateError(errors, "Workspace pruning failed.");
    }
  }

  /** Promotes the winner, then removes every remaining speculative workspace. */
  async finish(winner) {
    if (!winner || winner.operation === "root") {
      const errors = await this.#removeEntries(
        [...this.#entries.values()],
        "search-complete",
      );
      if (errors.length > 0) {
        throw new AggregateError(errors, "Workspace cleanup failed.");
      }
      return null;
    }
    const winnerEntry = [...this.#entries.values()].find(
      ({ candidateId, removalAttempted }) =>
        candidateId === winner.id && !removalAttempted,
    );
    if (!winnerEntry) {
      const cause = new Error(
        `Winning candidate has no live workspace: ${winner.id}.`,
      );
      const errors = await this.#removeEntries(
        [...this.#entries.values()],
        "search-failed",
      );
      throw new AggregateError(
        [cause, ...errors],
        "Workspace finalization failed.",
      );
    }
    let promotion;
    try {
      promotion = await this.#pool.promote({
        workspace: winnerEntry.workspace,
        candidate: winner,
      });
    } catch (error) {
      const stateErrors = await settledErrors([
        this.#recordState(
          winnerEntry.workspace.id,
          "retained",
          "promotion-failed",
        ),
      ]);
      const cleanupErrors = await this.#removeEntries(
        [...this.#entries.values()].filter((entry) => entry !== winnerEntry),
        "promotion-failed",
      );
      const failure =
        stateErrors.length === 0 && cleanupErrors.length === 0
          ? error
          : new AggregateError(
              [error, ...stateErrors, ...cleanupErrors],
              "Promotion and workspace cleanup failed.",
            );
      failure.retainedWorkspaceId = winnerEntry.workspace.id;
      throw failure;
    }
    const stateErrors = await settledErrors([
      this.#recordState(winnerEntry.workspace.id, "promoted", "winner"),
    ]);
    const cleanupErrors = await this.#removeEntries(
      [...this.#entries.values()],
      (entry) => (entry === winnerEntry ? "promoted" : "search-complete"),
    );
    const errors = [...stateErrors, ...cleanupErrors];
    if (errors.length > 0) {
      const failure = new AggregateError(errors, "Workspace cleanup failed.");
      failure.workspaceIds = errors.map(({ workspaceId }) => workspaceId);
      throw failure;
    }
    return promotion;
  }

  /** Cleans all unremoved workspaces after a failed search and rethrows its cause. */
  async abort(cause) {
    const errors = await this.#removeEntries(
      [...this.#entries.values()],
      "search-failed",
    );
    if (errors.length > 0) {
      throw new AggregateError(
        [cause, ...errors],
        "Search and workspace cleanup failed.",
      );
    }
    throw cause;
  }

  async #recordState(workspaceId, state, reason) {
    if (this.#stateStore) {
      await this.#stateStore.markWorkspaceState({ workspaceId, state, reason });
    }
  }

  async #removeEntries(entries, reason) {
    const targets = entries.filter((entry) => entry && !entry.removalAttempted);
    for (const entry of targets) entry.removalAttempted = true;
    return settledErrors(
      targets.map(async (entry) => {
        const resolvedReason =
          typeof reason === "function" ? reason(entry) : reason;
        try {
          await this.#pool.remove({
            workspace: entry.workspace,
            reason: resolvedReason,
            candidateId: entry.candidateId,
          });
          await this.#recordState(
            entry.workspace.id,
            "removed",
            resolvedReason,
          );
        } catch (error) {
          if (error && typeof error === "object") {
            error.workspaceId = entry.workspace.id;
          }
          throw error;
        }
      }),
    );
  }
}

/** Validates and returns an optional host workspace pool. */
export function optionalWorkspacePool(workspacePool) {
  if (workspacePool === undefined) return null;
  requireMethod(workspacePool, "create");
  requireMethod(workspacePool, "promote");
  requireMethod(workspacePool, "remove");
  return workspacePool;
}
