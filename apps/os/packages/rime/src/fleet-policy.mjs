/** Expresses RIME's low-cost scheduling preference without prescribing models. */

export const LOW_COST_PREFERENCE = "low-cost";
export const WORKER_PREFERENCES = Object.freeze([LOW_COST_PREFERENCE]);

function requireFunction(value, label) {
  if (typeof value !== "function")
    throw new Error(`${label} must be a function.`);
  return value;
}

function requireIdentityPart(value, label) {
  if (typeof value !== "string" || !value.trim()) {
    throw new Error(`${label} must be a non-empty string.`);
  }
  return value.trim();
}

/** Normalizes the provider/runtime agent and model that handled an attempt. */
export function requireModelIdentity(identity, label = "Model identity") {
  if (!identity || typeof identity !== "object") {
    throw new Error(`${label} must identify an agent and model.`);
  }
  return Object.freeze({
    agent: requireIdentityPart(identity.agent, `${label} agent`),
    model: requireIdentityPart(identity.model, `${label} model`),
  });
}

/** Validates the host adapter that assigns and executes speculative workers. */
export function requireFleet(fleet) {
  if (!fleet || typeof fleet !== "object") {
    throw new Error("fleet must provide assign() and execute().");
  }
  return Object.freeze({
    assign: requireFunction(fleet.assign, "fleet.assign").bind(fleet),
    execute: requireFunction(fleet.execute, "fleet.execute").bind(fleet),
  });
}

/** Asks the host for a low-cost worker while accepting its selected model. */
export async function assignPreferredWorker(fleet, request) {
  return requireModelIdentity(
    await fleet.assign({ ...request, preferences: WORKER_PREFERENCES }),
    "Fleet assignment",
  );
}

/** Compares observed execution with the assignment without making it a gate. */
export function observeExecution(execution, assignment) {
  const actual = requireModelIdentity(execution, "Fleet execution");
  return Object.freeze({
    execution: actual,
    assignmentMatched:
      actual.agent === assignment.agent && actual.model === assignment.model,
  });
}

/** Validates the exact preference RIME records in durable attempt state. */
export function requireWorkerPreferences(preferences) {
  if (
    !Array.isArray(preferences) ||
    preferences.length !== 1 ||
    preferences[0] !== LOW_COST_PREFERENCE
  ) {
    throw new Error("Worker preferences must contain only low-cost.");
  }
  return WORKER_PREFERENCES;
}
