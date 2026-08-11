import { Link } from "@tanstack/react-router";
import {
  Browser,
  Compass,
  Database,
  FlowArrow,
  Hexagon,
  House,
  MagnifyingGlass,
  PlugsConnected,
  SidebarSimple,
  SquaresFour,
} from "@phosphor-icons/react";
import { useSiteName } from "../../ServerConfigContext";
import SiteLogo from "../SiteLogo";
import { openCommandPalette } from "./commandPaletteBus";
import SidebarItem from "./SidebarItem";
import {
  SidebarWorkspacesProvider,
  SidebarWorkspacesTools,
  SidebarWorkspacesLists,
} from "./SidebarWorkspaces";
import SidebarUtilityStrip from "./SidebarUtilityStrip";

// The persistent left rail. Three pinned regions sandwich a single scrolling region of lists, so
// the user can always reach Search, primary nav, and the bottom utility strip no matter how many
// workspaces they have.
//
// Layout (top → bottom):
//   • brand row                            pinned
//   • primary nav (Home, Workspaces, …)    pinned
//   • workspace tools (⌘K search)          pinned
//   • Favorites / Recent workspaces        SCROLLS
//   • utility strip (plug, avatar)         pinned
export default function Sidebar({
  collapsed,
  onToggleCollapsed,
}: {
  collapsed: boolean;
  onToggleCollapsed: () => void;
}) {
  const siteName = useSiteName();
  return (
    <aside
      aria-label="Primary"
      className={[
        // Sidebar is the app chrome: a hair greyer than the (lighter) content canvas so the two
        // surfaces read as distinct without a heavy divider.
        "flex h-screen flex-col border-r border-kumo-line bg-kumo-elevated",
        collapsed ? "w-[56px]" : "w-[260px]",
        "shrink-0 transition-[width] duration-200 ease-out",
      ].join(" ")}
    >
      {/* Brand row */}
      <div
        className={[
          "flex h-14 shrink-0 items-center border-b border-kumo-line",
          collapsed ? "justify-center px-1.5" : "justify-between gap-2 px-3",
        ].join(" ")}
      >
        <Link
          to="/"
          aria-label={siteName}
          className="flex min-w-0 items-center gap-2"
        >
          <SiteLogo size={20} className="shrink-0">
            <Hexagon
              size={20}
              weight="bold"
              className="text-kumo-brand shrink-0"
            />
          </SiteLogo>
          {!collapsed && (
            <span className="truncate text-[14px] leading-5 font-semibold tracking-[-0.25px] text-kumo-default">
              {siteName}
            </span>
          )}
        </Link>
        {!collapsed && (
          <div className="flex items-center gap-0.5">
            <button
              type="button"
              onClick={() => openCommandPalette()}
              aria-label="Search"
              title="Search (⌘K)"
              className="press flex h-7 w-7 cursor-pointer items-center justify-center rounded-md text-kumo-inactive transition-colors hover:bg-kumo-tint hover:text-kumo-default"
            >
              <MagnifyingGlass size={15} />
            </button>
            <button
              type="button"
              onClick={onToggleCollapsed}
              aria-label="Collapse sidebar"
              title="Collapse sidebar"
              className="flex h-7 w-7 cursor-pointer items-center justify-center rounded-md text-kumo-inactive transition-colors hover:bg-kumo-tint hover:text-kumo-default"
            >
              <SidebarSimple size={15} />
            </button>
          </div>
        )}
      </div>

      {/* Expand affordance when collapsed — placed just under the logo for discoverability. */}
      {collapsed && (
        <button
          type="button"
          onClick={onToggleCollapsed}
          aria-label="Expand sidebar"
          title="Expand sidebar"
          className="mx-auto mt-2 flex h-7 w-7 shrink-0 cursor-pointer items-center justify-center rounded-md text-kumo-inactive transition-colors hover:bg-kumo-tint hover:text-kumo-default"
        >
          <SidebarSimple size={15} className="rotate-180" />
        </button>
      )}

      <SidebarWorkspacesProvider>
        {/* Pinned top stack. shrink-0 keeps it from squishing when the lists below grow. */}
        <div className="flex shrink-0 flex-col gap-3 pt-3">
          {/* Primary nav */}
          <nav className="flex flex-col gap-0.5 px-2">
            <SidebarItem
              to="/"
              label="Home"
              icon={<House size={14} weight="regular" />}
              collapsed={collapsed}
            />
            <SidebarItem
              to="/workspaces"
              label="Workspaces"
              icon={<SquaresFour size={14} weight="regular" />}
              collapsed={collapsed}
            />
            <SidebarItem
              to="/workflows"
              label="Workers"
              icon={<FlowArrow size={14} weight="regular" />}
              collapsed={collapsed}
            />
            <SidebarItem
              to="/applications"
              label="Applications"
              icon={<Browser size={14} weight="regular" />}
              collapsed={collapsed}
            />
            <SidebarItem
              to="/integrations"
              label="Integrations"
              icon={<PlugsConnected size={14} weight="regular" />}
              collapsed={collapsed}
            />
            <SidebarItem
              to="/data"
              label="Databases"
              icon={<Database size={14} weight="regular" />}
              collapsed={collapsed}
            />
            <SidebarItem
              to="/explore"
              label="Explore"
              icon={<Compass size={14} weight="regular" />}
              collapsed={collapsed}
            />
          </nav>

          {/* Workspace tools: search. Pinned so it's always reachable. */}
          <SidebarWorkspacesTools collapsed={collapsed} />
        </div>

        {/* Scrolling middle: only the Favorites / Recent workspaces / Recent blueprints lists.
            min-h-0 lets flex children compute scroll height correctly. */}
        <div className="sidebar-scroll mt-1 min-h-0 flex-1 overflow-y-auto">
          <SidebarWorkspacesLists collapsed={collapsed} />
        </div>
      </SidebarWorkspacesProvider>

      <SidebarUtilityStrip collapsed={collapsed} />
    </aside>
  );
}
