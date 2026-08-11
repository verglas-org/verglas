import { createFileRoute } from "@tanstack/react-router";
import WorkspaceChatPage from "../WorkspaceChatPage";

export const Route = createFileRoute("/workspace/$id")({
  component: WorkspaceChatPage,
});
