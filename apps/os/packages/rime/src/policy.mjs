/** AIDE's draft, debug, and improve search policy with parallel fan-out. */

function requireNonNegativeInteger(value, label) {
  if (!Number.isInteger(value) || value < 0) {
    throw new Error(`${label} must be a non-negative integer.`);
  }
  return value;
}

const MAX_CONCURRENCY = 100;

function requireConcurrency(value) {
  const concurrency = requireNonNegativeInteger(value, "concurrency");
  if (concurrency === 0) {
    throw new Error("concurrency must be greater than zero.");
  }
  if (concurrency > MAX_CONCURRENCY) {
    throw new Error(`concurrency must be at most ${MAX_CONCURRENCY}.`);
  }
  return concurrency;
}

function bestValid(candidates) {
  return candidates
    .filter((candidate) => candidate.evaluation.status === "valid")
    .toSorted(
      (left, right) =>
        right.evaluation.score - left.evaluation.score ||
        left.ordinal - right.ordinal,
    )[0];
}

function requireProbability(value, label) {
  if (!Number.isFinite(value) || value < 0 || value > 1) {
    throw new Error(`${label} must be between zero and one.`);
  }
  return value;
}

function draw(random) {
  const value = random();
  if (!Number.isFinite(value) || value < 0 || value >= 1) {
    throw new Error(
      "random must return a number from zero up to, but not including, one.",
    );
  }
  return value;
}

function proposal(parent, operation, ordinal, debugDepth) {
  return Object.freeze({
    ordinal,
    parentId: parent.id,
    parent,
    operation,
    debugDepth,
  });
}

/**
 * Selects one AIDE operation and fans it out from the same parent.
 *
 * Drafting stops at the requested initial count. Debugging probabilistically
 * selects an unexpanded buggy node within the depth limit. Improvement selects
 * the best valid node. Only the fan-out is parallel; policy priority is unchanged.
 */
export function selectProposalBatch(snapshot, config) {
  const initialDrafts = requireNonNegativeInteger(
    config.initialDrafts,
    "initialDrafts",
  );
  const concurrency = requireConcurrency(config.concurrency);
  const remainingCandidates = requireNonNegativeInteger(
    config.remainingCandidates,
    "remainingCandidates",
  );
  const maxDebugDepth = requireNonNegativeInteger(
    config.maxDebugDepth,
    "maxDebugDepth",
  );
  const debugProbability = requireProbability(
    config.debugProbability,
    "debugProbability",
  );
  if (typeof config.random !== "function") {
    throw new Error("random must be a function.");
  }
  const capacity = Math.min(concurrency, remainingCandidates);
  if (capacity === 0) return Object.freeze([]);

  const candidates = snapshot.candidates;
  const root = candidates[0];
  if (!root) throw new Error("The solution graph requires a root candidate.");
  const draftCount = candidates.filter(
    (candidate) => candidate.operation === "draft",
  ).length;

  if (draftCount < initialDrafts) {
    const count = Math.min(capacity, initialDrafts - draftCount);
    return Object.freeze(
      Array.from({ length: count }, (_, index) =>
        proposal(root, "draft", candidates.length + index, 0),
      ),
    );
  }

  const debuggable = candidates.filter(
    (candidate) =>
      candidate.operation !== "root" &&
      candidate.evaluation.status === "buggy" &&
      !candidate.expanded &&
      candidate.debugDepth <= maxDebugDepth,
  );
  if (
    debuggable.length > 0 &&
    (debugProbability === 1 ||
      (debugProbability > 0 && draw(config.random) < debugProbability))
  ) {
    const buggy =
      debuggable[
        debugProbability === 1 && debuggable.length === 1
          ? 0
          : Math.floor(draw(config.random) * debuggable.length)
      ];
    return Object.freeze(
      Array.from({ length: capacity }, (_, index) =>
        proposal(
          buggy,
          "debug",
          candidates.length + index,
          buggy.debugDepth + 1,
        ),
      ),
    );
  }

  const parent = bestValid(candidates);
  if (!parent) {
    return Object.freeze(
      Array.from({ length: capacity }, (_, index) =>
        proposal(root, "draft", candidates.length + index, 0),
      ),
    );
  }
  return Object.freeze(
    Array.from({ length: capacity }, (_, index) =>
      proposal(parent, "improve", candidates.length + index, 0),
    ),
  );
}

/** Validates the hard limits used by the parallel search loop. */
export function normalizeSearchConfig(config) {
  const normalized = {
    initialDrafts: requireNonNegativeInteger(
      config.initialDrafts,
      "initialDrafts",
    ),
    concurrency: requireConcurrency(config.concurrency),
    candidateBudget: requireNonNegativeInteger(
      config.candidateBudget,
      "candidateBudget",
    ),
    maxDebugDepth: requireNonNegativeInteger(
      config.maxDebugDepth,
      "maxDebugDepth",
    ),
    debugProbability: requireProbability(
      config.debugProbability,
      "debugProbability",
    ),
  };
  return Object.freeze(normalized);
}
