import { createFileRoute, Link, useNavigate } from "@tanstack/react-router";
import { Hexagon, Plus, Star, Trash } from "@phosphor-icons/react";
import { useCallback, useEffect, useState } from "react";
import type { WorkspaceMetadataWithTimestamps } from "@verglas/workshop-shared/api";
import { useAuthenticatedApi } from "../AuthContext";
import DeleteConfirmationDialog from "../components/DeleteConfirmationDialog";
import {
  CatalogDetailCard,
  CatalogEmpty,
  CatalogError,
  CatalogPage,
  CatalogStatus,
  CatalogTable,
} from "../components/CatalogTable";
import { useDocumentTitle } from "../useDocumentTitle";
import { useAutomaticRefresh } from "../useAutomaticRefresh";

export const Route = createFileRoute("/workspaces")({
  component: WorkspacesPage,
});

function initials(title: string | undefined): string {
  const t = (title || "Untitled").trim();
  if (!t) return "UG";
  const parts = t.split(/\s+/).slice(0, 2);
  return (
    parts.map((p) => p[0]?.toUpperCase() ?? "").join("") ||
    t.slice(0, 2).toUpperCase()
  );
}

function formatRelativeTime(date: Date): string {
  const diff = Date.now() - date.getTime();
  const minutes = Math.floor(diff / 60000);
  if (minutes < 1) return "just now";
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  return `${days}d ago`;
}

function WorkspacesPage() {
  useDocumentTitle("Workspaces");
  const { authenticatedApi } = useAuthenticatedApi();
  const navigate = useNavigate();
  const [workspaces, setVessels] = useState<WorkspaceMetadataWithTimestamps[]>(
    [],
  );
  const [selected, setSelected] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [deleting, setDeleting] = useState(false);
  const [confirmDelete, setConfirmDelete] = useState(false);

  const load = useCallback(
    async (showLoading = false) => {
      if (showLoading) setLoading(true);
      setError(null);
      try {
        const list = await authenticatedApi.listWorkspaces();
        setVessels(
          list.toSorted((a, b) => {
            if (a.pinned && !b.pinned) return -1;
            if (!a.pinned && b.pinned) return 1;
            return b.lastActive.getTime() - a.lastActive.getTime();
          }),
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

  const workspace = workspaces.find((entry) => entry.id === selected) ?? null;

  const remove = async () => {
    if (!workspace) return;
    setDeleting(true);
    try {
      if (workspace.owner) {
        await authenticatedApi.dismissSharedWorkspace(workspace.id);
      } else {
        const overseer = await authenticatedApi.openWorkspace(workspace.id);
        try {
          await overseer.deleteSelf();
        } finally {
          overseer[Symbol.dispose]();
        }
      }
      setConfirmDelete(false);
      setSelected(null);
      await load();
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setDeleting(false);
    }
  };

  return (
    <CatalogPage
      title="Workspaces"
      description="Each workspace is an isolated environment with its own conversations, gatekeepers, and outputs."
      actions={
        <Link
          to="/"
          className="inline-flex h-9 shrink-0 cursor-pointer items-center gap-1.5 rounded-lg bg-kumo-brand px-3.5 text-[13px] font-medium text-white hover:bg-kumo-brand-hover"
        >
          <Plus size={14} weight="bold" />
          Create workspace
        </Link>
      }
    >
      {error && <CatalogError message={error} />}
      {workspace ? (
        <CatalogDetailCard
          open
          title={workspace.title || "Untitled"}
          subtitle={
            workspace.owner
              ? `Shared by ${workspace.owner.name || workspace.owner.id}`
              : "Owned by you"
          }
          meta={
            workspace.pinned ? (
              <span className="inline-flex items-center gap-1 rounded-full bg-kumo-tint px-2 py-0.5 text-[10px] font-semibold uppercase text-kumo-brand">
                <Star size={10} weight="fill" /> Favorite
              </span>
            ) : undefined
          }
          onBack={() => setSelected(null)}
          footer={
            <>
              <button
                type="button"
                onClick={() => setConfirmDelete(true)}
                className="mr-auto inline-flex h-9 cursor-pointer items-center gap-1.5 rounded-lg border border-kumo-line px-3 text-[13px] text-kumo-subtle hover:border-kumo-danger/40 hover:bg-kumo-danger-tint hover:text-kumo-danger"
              >
                <Trash size={14} />
                {workspace.owner ? "Remove" : "Delete"}
              </button>
              <button
                type="button"
                onClick={() => {
                  void navigate({
                    to: "/workspace/$id",
                    params: { id: workspace.id },
                  });
                }}
                className="inline-flex h-9 cursor-pointer items-center gap-1.5 rounded-lg bg-kumo-brand px-3 text-[13px] font-medium text-white hover:bg-kumo-brand-hover"
              >
                <Hexagon size={14} />
                Open workspace
              </button>
            </>
          }
        >
          <dl className="grid gap-4 text-[13px] sm:grid-cols-2">
            <div>
              <dt className="text-[11px] font-medium uppercase tracking-wide text-kumo-inactive">
                Last active
              </dt>
              <dd className="mt-1 text-kumo-default">
                {formatRelativeTime(workspace.lastActive)}
              </dd>
            </div>
            <div>
              <dt className="text-[11px] font-medium uppercase tracking-wide text-kumo-inactive">
                Created
              </dt>
              <dd className="mt-1 text-kumo-default">
                {formatRelativeTime(workspace.created)}
              </dd>
            </div>
            <div className="sm:col-span-2">
              <dt className="text-[11px] font-medium uppercase tracking-wide text-kumo-inactive">
                Workspace id
              </dt>
              <dd className="mt-1 break-all font-mono text-[12px] text-kumo-subtle">
                {workspace.id}
              </dd>
            </div>
          </dl>
        </CatalogDetailCard>
      ) : loading ? (
        <CatalogEmpty>Loading workspaces…</CatalogEmpty>
      ) : (
        <CatalogTable
          empty="You haven't created any workspaces yet."
          cards={workspaces.map((entry) => ({
            id: entry.id,
            icon: (
              <span className="text-[12px] font-medium text-kumo-subtle">
                {initials(entry.title)}
              </span>
            ),
            primary: entry.title || "Untitled",
            secondary: entry.owner
              ? `Shared by ${entry.owner.name || entry.owner.id}`
              : "Owned by you",
            tertiary: `Active ${formatRelativeTime(entry.lastActive)}`,
            meta: (
              <>
                {entry.pinned && (
                  <Star size={12} weight="fill" className="text-kumo-brand" />
                )}
                {entry.owner && <CatalogStatus value="shared" />}
              </>
            ),
            onOpen: () => setSelected(entry.id),
          }))}
        />
      )}

      <DeleteConfirmationDialog
        open={confirmDelete && workspace !== null}
        title={workspace?.owner ? "Remove workspace" : "Delete workspace"}
        description={
          workspace
            ? workspace.owner
              ? `Remove “${workspace.title || "Untitled"}” from your list?`
              : `Delete “${workspace.title || "Untitled"}”? This cannot be undone.`
            : null
        }
        isDeleting={deleting}
        onOpenChange={(open) => {
          if (!open) setConfirmDelete(false);
        }}
        onConfirm={() => void remove()}
      />
    </CatalogPage>
  );
}
