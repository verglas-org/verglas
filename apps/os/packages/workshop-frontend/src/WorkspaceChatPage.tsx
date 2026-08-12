/**
 * Workspace shell: agent chat over lakehouse data, and build/edit of Vessels
 * (Applications / Integrations) through agent tools. No legacy iframe editor.
 */
import { useCallback, useState } from "react";
import { Link, useNavigate, useParams } from "@tanstack/react-router";
import { Hexagon, House } from "@phosphor-icons/react";
import type {
  Overseer,
  WorkspaceMetadata,
  WorkpieceId,
  BlueprintOutput,
} from "@verglas/workshop-shared/api";
import type { RpcStub } from "capnweb";
import { useAuthenticatedApi } from "./AuthContext";
import ChatInterface from "./ChatInterface";
import SiteLogo from "./components/SiteLogo";
import UserMenu from "./components/UserMenu";
import WorkspaceOpenErrorPage from "./components/WorkspaceOpenErrorPage";
import { useWorkspaceOpen } from "./useWorkspaceOpen";
import { WorkshopIconButton } from "./components/WorkshopControls";

function WorkspaceConversation({
  overseer,
  outputOfWorkpiece,
}: {
  overseer: RpcStub<Overseer>;
  outputOfWorkpiece: (workspaceId: WorkpieceId) => BlueprintOutput | undefined;
}) {
  const [selectedChatId, setSelectedChatId] = useState<number | null>(null);

  return (
    <ChatInterface
      overseer={overseer}
      selectedChatId={selectedChatId}
      onNavigateToChat={(chatId) => setSelectedChatId(chatId)}
      pendingConsoleLogCount={0}
      consoleLogPreview=""
      consoleLogSeverity="info"
      onConsumeConsoleLogs={() => ""}
      onDiscardConsoleLogs={() => {}}
      constrainChatWidth
      singleChat
      onOpenVessel={() => {}}
      outputOfWorkpiece={outputOfWorkpiece}
    />
  );
}

export default function WorkspaceChatPage() {
  const { id } = useParams({ from: "/workspace/$id" });
  const navigate = useNavigate();
  const { authenticatedApi } = useAuthenticatedApi();
  const [title, setTitle] = useState("Workspace");

  const onMetadata = useCallback((metadata: WorkspaceMetadata) => {
    setTitle(metadata.title || "Workspace");
  }, []);

  const { overseer, error, retry } = useWorkspaceOpen({
    id,
    authenticatedApi,
    onMetadata,
    onShareKeyConsumed: () => {},
    onInvalidShareKey: () => {},
  });

  const outputOfWorkpiece = useCallback(
    (_workspaceId: WorkpieceId): BlueprintOutput | undefined => {
      return undefined;
    },
    [],
  );

  if (error) {
    const kind = error.kind === "open" ? error.failure : "unexpected";
    return (
      <WorkspaceOpenErrorPage
        kind={kind}
        onRetry={retry}
        onGoToWorkspaces={() => void navigate({ to: "/workspaces" })}
      />
    );
  }

  return (
    <div className="flex h-screen flex-col bg-kumo-base">
      <div className="flex h-12 flex-shrink-0 items-center gap-3 border-b border-kumo-line px-3">
        <Link
          to="/"
          className="flex items-center gap-2 text-kumo-default hover:opacity-80"
        >
          <SiteLogo size={24}>
            <Hexagon size={24} weight="duotone" className="text-kumo-brand" />
          </SiteLogo>
        </Link>
        <WorkshopIconButton
          aria-label="Home"
          onClick={() => void navigate({ to: "/" })}
        >
          <House size={18} />
        </WorkshopIconButton>
        <h1 className="min-w-0 flex-1 truncate text-sm font-medium text-kumo-default">
          {title}
        </h1>
        <UserMenu />
      </div>
      <div className="min-h-0 flex-1">
        {overseer ? (
          <WorkspaceConversation
            key={id}
            overseer={overseer.stub}
            outputOfWorkpiece={outputOfWorkpiece}
          />
        ) : (
          <div className="flex h-full items-center justify-center">
            <div className="h-6 w-6 animate-spin rounded-full border-2 border-kumo-brand border-t-transparent" />
          </div>
        )}
      </div>
    </div>
  );
}
