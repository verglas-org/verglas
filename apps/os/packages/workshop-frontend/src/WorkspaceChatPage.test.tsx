// @vitest-environment jsdom
/* eslint-disable react/react-in-jsx-scope */

import { act, type ReactNode } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, describe, expect, it, vi } from "vitest";

const testState = vi.hoisted(() => ({
  chatProps: [] as Array<{
    selectedChatId: number | null;
    singleChat?: boolean;
    onNavigateToChat: (id: number | null) => void;
  }>,
  navigate: vi.fn<() => void>(),
}));

vi.mock("@tanstack/react-router", () => ({
  Link: ({ children }: { children: ReactNode }) => <a href="/">{children}</a>,
  useNavigate: () => testState.navigate,
  useParams: () => ({ id: "workspace-1" }),
}));

vi.mock("./AuthContext", () => ({
  useAuthenticatedApi: () => ({ authenticatedApi: {} }),
}));

vi.mock("./useWorkspaceOpen", () => ({
  useWorkspaceOpen: () => ({
    overseer: { stub: {} },
    error: null,
    retry: vi.fn<() => void>(),
  }),
}));

vi.mock("./ChatInterface", () => ({
  default: (props: (typeof testState.chatProps)[number]) => {
    testState.chatProps.push(props);
    return null;
  },
}));

vi.mock("./components/SiteLogo", () => ({
  default: ({ children }: { children: ReactNode }) => <>{children}</>,
}));
vi.mock("./components/UserMenu", () => ({ default: () => null }));
vi.mock("./components/WorkshopControls", () => ({
  WorkshopIconButton: ({ children }: { children: ReactNode }) => (
    <button>{children}</button>
  ),
}));

import WorkspaceChatPage from "./WorkspaceChatPage";

(
  globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }
).IS_REACT_ACT_ENVIRONMENT = true;

describe("WorkspaceChatPage", () => {
  let container: HTMLDivElement | undefined;
  let root: Root | undefined;

  afterEach(() => {
    act(() => root?.unmount());
    container?.remove();
    root = undefined;
    container = undefined;
    testState.chatProps.length = 0;
    vi.clearAllMocks();
  });

  it("uses local, single-chat selection rather than a chat URL parameter", async () => {
    container = document.createElement("div");
    document.body.append(container);
    root = createRoot(container);

    await act(async () => root!.render(<WorkspaceChatPage />));

    expect(testState.chatProps.at(-1)).toMatchObject({
      selectedChatId: null,
      singleChat: true,
    });

    await act(async () => testState.chatProps.at(-1)!.onNavigateToChat(42));

    expect(testState.chatProps.at(-1)).toMatchObject({
      selectedChatId: 42,
      singleChat: true,
    });
    expect(testState.navigate).not.toHaveBeenCalled();
  });
});
