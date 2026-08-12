import type { ReactNode } from "react";

/** The lifecycle presentation shared by chat-created resources and approval requests. */
export type ChatWidgetState =
  | "pending"
  | "deploying"
  | "needs_configuration"
  | "ready"
  | "approved"
  | "denied"
  | "rejected"
  | "error";

type WidgetTone = "default" | "pending" | "success" | "danger";

/** A short, human-readable lifecycle label for a persisted chat event. */
export function chatWidgetStatus(state: ChatWidgetState): {
  label: string;
  tone: WidgetTone;
} {
  switch (state) {
    case "pending":
      return { label: "Pending approval", tone: "pending" };
    case "deploying":
      return { label: "Deploying", tone: "pending" };
    case "needs_configuration":
      return { label: "Needs setup", tone: "default" };
    case "ready":
      return { label: "Ready", tone: "success" };
    case "approved":
      return { label: "Approved", tone: "success" };
    case "denied":
    case "rejected":
      return { label: "Denied", tone: "danger" };
    case "error":
      return { label: "Failed", tone: "danger" };
  }
}

const statusClasses: Record<WidgetTone, string> = {
  default: "bg-kumo-tint text-kumo-subtle",
  pending: "bg-kumo-brand/10 text-kumo-brand",
  success: "bg-kumo-success-tint/50 text-kumo-success",
  danger: "bg-kumo-danger-tint/50 text-kumo-danger",
};

/** Consistent shell for a resource created or configured from the Workspace chat. */
export function ResourceStatusWidget({
  kind,
  title,
  description,
  state,
  icon,
  identifier,
  children,
  actions,
}: {
  kind: "Integration" | "Table" | "Worker" | "Application" | "Source";
  title: string;
  description?: string;
  state?: ChatWidgetState;
  icon: ReactNode;
  identifier?: string;
  children?: ReactNode;
  actions?: ReactNode;
}) {
  const status = state ? chatWidgetStatus(state) : undefined;
  return (
    <div className="group/work max-w-[860px] text-[14px] leading-5 tracking-[-0.25px] text-kumo-subtle">
      <div className="rounded-2xl border border-kumo-line bg-kumo-base px-4 py-3">
        <div className="flex items-start gap-3">
          <span
            className="flex h-9 w-9 flex-shrink-0 items-center justify-center rounded-lg bg-kumo-tint text-kumo-brand"
            aria-hidden="true"
          >
            {icon}
          </span>
          <div className="min-w-0 flex-1">
            <div className="flex flex-wrap items-center gap-2">
              <span className="font-medium text-kumo-default">{title}</span>
              <span className="rounded-full bg-kumo-tint px-2 py-0.5 text-[11px] leading-4">
                {kind}
              </span>
              {status && (
                <span
                  className={`rounded-full px-2 py-0.5 text-[11px] font-medium leading-4 ${statusClasses[status.tone]}`}
                >
                  {status.label}
                </span>
              )}
            </div>
            {description && (
              <p className="mt-1 text-[13px] leading-[18px]">{description}</p>
            )}
            {identifier && (
              <p className="mt-1 font-mono text-[11px] text-kumo-inactive">
                {identifier}
              </p>
            )}
            {children}
            {actions && <div className="mt-3 flex gap-2">{actions}</div>}
          </div>
        </div>
      </div>
    </div>
  );
}

/** A decision card which makes the requesting principal, scope, requested action, and reason explicit. */
export function PermissionRequestWidget({
  title,
  principal = "Workspace agent",
  resource,
  actions,
  reason,
  state,
  icon,
  controls,
  children,
}: {
  title: string;
  principal?: string;
  resource?: ReactNode;
  actions: string;
  reason: ReactNode;
  state: "pending" | "approved" | "denied" | "rejected";
  icon: ReactNode;
  controls?: ReactNode;
  children?: ReactNode;
}) {
  const status = chatWidgetStatus(state);
  return (
    <div className="group/work max-w-[860px] text-[14px] leading-5 tracking-[-0.25px] text-kumo-subtle">
      <div
        className={`rounded-2xl border px-4 py-3 ${state === "pending" ? "border-kumo-brand/40 bg-kumo-brand/10" : "border-kumo-line bg-kumo-base"}`}
      >
        <div className="flex items-start gap-3">
          <span
            className="flex h-9 w-9 flex-shrink-0 items-center justify-center rounded-lg bg-kumo-tint text-kumo-brand"
            aria-hidden="true"
          >
            {icon}
          </span>
          <div className="min-w-0 flex-1">
            <div className="flex flex-wrap items-center gap-2">
              <span className="font-medium text-kumo-default">{title}</span>
              <span
                className={`rounded-full px-2 py-0.5 text-[11px] font-medium leading-4 ${statusClasses[status.tone]}`}
              >
                {status.label}
              </span>
            </div>
            <dl className="mt-3 grid gap-x-4 gap-y-1 text-[12px] leading-4 sm:grid-cols-[auto_1fr]">
              <dt className="text-kumo-inactive">Principal</dt>
              <dd className="min-w-0 text-kumo-default">{principal}</dd>
              <dt className="text-kumo-inactive">Resource</dt>
              <dd className="min-w-0 text-kumo-default">
                {resource ?? "Workspace resource"}
              </dd>
              <dt className="text-kumo-inactive">Requested</dt>
              <dd className="min-w-0 text-kumo-default">{actions}</dd>
              <dt className="text-kumo-inactive">Reason</dt>
              <dd className="min-w-0 text-kumo-subtle">{reason}</dd>
            </dl>
            {children}
          </div>
          {controls && (
            <div className="ml-3 flex flex-shrink-0 items-center gap-1 self-center">
              {controls}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
