import { createFileRoute, Link } from "@tanstack/react-router";
import { ArrowSquareOut, Browser, Plus, Trash } from "@phosphor-icons/react";
import { useCallback, useEffect, useState } from "react";
import type { VerglasVesselSummary } from "@verglas/workshop-shared/api";
import { useAuthenticatedApi } from "../AuthContext";
import DeleteConfirmationDialog from "../components/DeleteConfirmationDialog";
import { Card } from "../components/ui/card";
import {
  CatalogDetailCard,
  CatalogEmpty,
  CatalogError,
  CatalogPage,
  CatalogStatus,
} from "../components/CatalogTable";
import {
  applicationLifecycleAvailable,
  nextApplicationLifecycleState,
} from "../applicationLifecycle";
import { useServerConfig } from "../ServerConfigContext";
import { useAutomaticRefresh } from "../useAutomaticRefresh";
import { useDocumentTitle } from "../useDocumentTitle";

export const Route = createFileRoute("/applications")({
  component: ApplicationsPage,
});

function ApplicationsPage() {
  useDocumentTitle("Applications");
  const { authenticatedApi } = useAuthenticatedApi();
  const serverConfig = useServerConfig();
  const [applications, setApplications] = useState<VerglasVesselSummary[]>([]);
  const [selected, setSelected] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [deleting, setDeleting] = useState<string | null>(null);
  const [confirmDelete, setConfirmDelete] = useState<string | null>(null);
  const [lifecycleBusy, setLifecycleBusy] = useState(false);

  const load = useCallback(
    async (showLoading = false) => {
      if (showLoading) setLoading(true);
      setError(null);
      try {
        setApplications(
          (await authenticatedApi.listVerglasVessels()).filter(
            (v) => v.role === "application",
          ),
        );
      } catch (err) {
        setError(err instanceof Error ? err.message : String(err));
      } finally {
        if (showLoading) setLoading(false);
      }
    },
    [authenticatedApi],
  );
  useEffect(() => {
    void load(true);
  }, [load]);
  useAutomaticRefresh(load);

  const app = applications.find((entry) => entry.name === selected) ?? null;
  const canManageLifecycle = applicationLifecycleAvailable(
    serverConfig?.localContainerRuntime,
  );

  const remove = async () => {
    if (!confirmDelete) return;
    setDeleting(confirmDelete);
    setError(null);
    try {
      await authenticatedApi.deleteVerglasApplication(confirmDelete);
      if (selected === confirmDelete) setSelected(null);
      setConfirmDelete(null);
      await load();
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setDeleting(null);
    }
  };

  const toggleLifecycle = async () => {
    if (!app || !canManageLifecycle) return;
    setLifecycleBusy(true);
    setError(null);
    try {
      await authenticatedApi.setVerglasApplicationState(
        app.name,
        nextApplicationLifecycleState(app.running),
      );
      await load();
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setLifecycleBusy(false);
    }
  };

  return (
    <CatalogPage
      title="Applications"
      description="Full-stack applications built over your lakehouse and integrations."
      actions={
        <Link
          to="/"
          search={{ prompt: "Build an application that " }}
          className="inline-flex h-9 shrink-0 cursor-pointer items-center gap-1.5 rounded-lg bg-kumo-brand px-3.5 text-[13px] font-medium text-white hover:bg-kumo-brand-hover"
        >
          <Plus size={14} weight="bold" />
          New application
        </Link>
      }
    >
      {error && <CatalogError message={error} />}
      {app ? (
        <CatalogDetailCard
          open
          title={app.name}
          subtitle={app.image}
          meta={
            <CatalogStatus value={app.health} good={app.health === "ready"} />
          }
          screenshotUrl={app.screenshotUrl}
          onBack={() => setSelected(null)}
          footer={
            <>
              <button
                type="button"
                onClick={() => setConfirmDelete(app.name)}
                className="mr-auto inline-flex h-9 cursor-pointer items-center gap-1.5 rounded-lg border border-kumo-line px-3 text-[13px] text-kumo-subtle hover:border-kumo-danger/40 hover:bg-kumo-danger-tint hover:text-kumo-danger"
              >
                <Trash size={14} />
                Delete
              </button>
              {canManageLifecycle && (
                <button
                  type="button"
                  disabled={lifecycleBusy}
                  onClick={() => void toggleLifecycle()}
                  className="inline-flex h-9 cursor-pointer items-center gap-1.5 rounded-lg border border-kumo-line px-3 text-[13px] text-kumo-default hover:bg-kumo-tint disabled:cursor-not-allowed disabled:opacity-40"
                >
                  {app.running === false ? "Start container" : "Stop container"}
                </button>
              )}
              {app.previewUrl && (
                <a
                  href={app.previewUrl}
                  target="_blank"
                  rel="noreferrer"
                  className="inline-flex h-9 items-center gap-1.5 rounded-lg bg-kumo-brand px-3 text-[13px] font-medium text-white hover:bg-kumo-brand-hover"
                >
                  Open preview <ArrowSquareOut size={14} />
                </a>
              )}
            </>
          }
        >
          <dl className="grid gap-3 text-[13px] sm:grid-cols-2">
            <div>
              <dt className="text-[11px] font-medium uppercase tracking-wide text-kumo-inactive">
                Vessel
              </dt>
              <dd className="mt-1 font-mono text-kumo-default">{app.name}</dd>
            </div>
            <div>
              <dt className="text-[11px] font-medium uppercase tracking-wide text-kumo-inactive">
                Health
              </dt>
              <dd className="mt-1 text-kumo-default">{app.health}</dd>
            </div>
            <div>
              <dt className="text-[11px] font-medium uppercase tracking-wide text-kumo-inactive">
                Container
              </dt>
              <dd className="mt-1 text-kumo-default">
                {app.state ?? "State unavailable"}
              </dd>
            </div>
            <div className="sm:col-span-2">
              <dt className="text-[11px] font-medium uppercase tracking-wide text-kumo-inactive">
                Image
              </dt>
              <dd className="mt-1 break-all font-mono text-[12px] text-kumo-subtle">
                {app.image}
              </dd>
            </div>
          </dl>
          {!canManageLifecycle && serverConfig && (
            <p className="mt-5 rounded-lg border border-kumo-line bg-kumo-tint/40 px-3 py-2 text-[12px] text-kumo-subtle">
              Container lifecycle controls are available in self-hosted OSS
              deployments.
            </p>
          )}
        </CatalogDetailCard>
      ) : loading ? (
        <CatalogEmpty>Loading applications…</CatalogEmpty>
      ) : applications.length === 0 ? (
        <CatalogEmpty>
          <p>No applications yet.</p>
          <Link
            to="/"
            search={{ prompt: "Build an application that " }}
            className="mt-3 inline-flex text-kumo-brand hover:underline"
          >
            Describe the application you want to build
          </Link>
        </CatalogEmpty>
      ) : (
        <div className="grid grid-cols-1 gap-5 lg:grid-cols-2">
          {applications.map((entry) => (
            <ApplicationCard
              key={entry.name}
              application={entry}
              onOpen={() => setSelected(entry.name)}
            />
          ))}
        </div>
      )}

      <DeleteConfirmationDialog
        open={confirmDelete !== null}
        title="Delete application"
        description={
          confirmDelete
            ? `Stop and remove “${confirmDelete}” from the local runtime. This cannot be undone.`
            : null
        }
        isDeleting={deleting !== null}
        onOpenChange={(open) => {
          if (!open) setConfirmDelete(null);
        }}
        onConfirm={() => void remove()}
      />
    </CatalogPage>
  );
}

function ApplicationCard({
  application,
  onOpen,
}: {
  application: VerglasVesselSummary;
  onOpen: () => void;
}) {
  const title = application.title || application.name;
  const running =
    application.running !== false &&
    (application.state === "running" || application.health === "ready");

  return (
    <Card className="group overflow-hidden rounded-2xl transition-colors hover:border-kumo-brand/40">
      <button
        type="button"
        onClick={onOpen}
        className="block w-full cursor-pointer text-left focus:outline-none focus-visible:ring-2 focus-visible:ring-kumo-brand"
      >
        <div className="relative aspect-[16/9] overflow-hidden border-b border-kumo-line bg-kumo-tint/60">
          {application.screenshotUrl ? (
            <img
              src={application.screenshotUrl}
              alt={`Preview of ${title}`}
              className="h-full w-full object-cover object-top transition-transform duration-300 group-hover:scale-[1.015]"
            />
          ) : application.previewUrl ? (
            <iframe
              src={application.previewUrl}
              title={`Live preview of ${title}`}
              loading="lazy"
              tabIndex={-1}
              className="h-full w-full origin-top-left border-0 bg-white pointer-events-none"
            />
          ) : (
            <div className="flex h-full flex-col items-center justify-center bg-[radial-gradient(circle_at_50%_0%,var(--color-kumo-fill),transparent_65%)] text-kumo-inactive">
              <Browser size={34} weight="duotone" />
              <span className="mt-3 text-[12px] font-medium">
                Preview unavailable
              </span>
            </div>
          )}
          <div className="absolute left-3 top-3 flex items-center gap-2">
            <span className="rounded-full bg-kumo-base/90 px-2 py-1 text-[10px] font-semibold uppercase tracking-wide text-kumo-subtle shadow-sm backdrop-blur">
              Application
            </span>
            <CatalogStatus
              value={application.state ?? application.health}
              good={running}
            />
          </div>
        </div>
        <div className="px-5 pb-4 pt-4">
          <h2 className="truncate text-base font-semibold tracking-tight text-kumo-default">
            {title}
          </h2>
          <p className="mt-1 line-clamp-2 min-h-10 text-[12px] leading-5 text-kumo-subtle">
            {application.description || "A standalone application Vessel."}
          </p>
        </div>
      </button>
      <div className="flex items-center justify-between gap-3 border-t border-kumo-line px-5 py-3">
        <span
          className="min-w-0 truncate font-mono text-[10px] text-kumo-inactive"
          title={application.image}
        >
          {application.image}
        </span>
        {application.previewUrl ? (
          <a
            href={application.previewUrl}
            target="_blank"
            rel="noreferrer"
            className="inline-flex shrink-0 items-center gap-1.5 text-[12px] font-medium text-kumo-brand hover:underline"
          >
            Open preview <ArrowSquareOut size={13} />
          </a>
        ) : (
          <span className="shrink-0 text-[11px] text-kumo-inactive">
            No preview URL
          </span>
        )}
      </div>
    </Card>
  );
}
