import type { VerglasWorkerSummary } from "@verglas/workshop-shared/api";

export type WorkerSchedule = {
  label: string;
  kind: "scheduled" | "event" | "manual" | "unknown";
};

/** Separates a registered worker's lifecycle from the state of any individual run. */
export function workerLifecycleLabel(
  state: VerglasWorkerSummary["state"],
): "Active" | "Disabled" {
  return state === "running" ? "Active" : "Disabled";
}

/** Returns a compact label for the first declared trigger. */
export function workerScheduleSummary(
  worker: Pick<VerglasWorkerSummary, "triggers">,
): WorkerSchedule {
  try {
    const trigger = (
      JSON.parse(worker.triggers) as Array<{
        type?: string;
        schedule?: string;
        path?: string;
        eventType?: string;
      }>
    )[0];
    if (!trigger) return { label: "Manual only", kind: "manual" };
    if (trigger.type === "cron")
      return {
        label: trigger.schedule ? `Cron · ${trigger.schedule}` : "Scheduled",
        kind: "scheduled",
      };
    if (trigger.type === "webhook")
      return {
        label: trigger.path ? `Webhook · ${trigger.path}` : "Webhook",
        kind: "event",
      };
    if (trigger.type === "event")
      return {
        label: trigger.eventType
          ? `Event · ${trigger.eventType}`
          : "Event trigger",
        kind: "event",
      };
    return {
      label: trigger.type || "Manual only",
      kind: trigger.type ? "unknown" : "manual",
    };
  } catch {
    return { label: "Invalid trigger declaration", kind: "unknown" };
  }
}

/** Computes worker-board totals from registry state and bounded run history. */
export function summarizeWorkers(workers: VerglasWorkerSummary[]) {
  return {
    total: workers.length,
    active: workers.filter((worker) => worker.activeRun).length,
    scheduled: workers.filter(
      (worker) => workerScheduleSummary(worker).kind === "scheduled",
    ).length,
    failed: workers.filter((worker) =>
      worker.recentRuns?.some((run) => run.state === "failed"),
    ).length,
  };
}
