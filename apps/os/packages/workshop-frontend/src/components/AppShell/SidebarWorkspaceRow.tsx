import { Link } from '@tanstack/react-router'
import { DotsThree, Star, ShareNetwork, Trash, Pencil } from '@phosphor-icons/react'
import { DropdownMenu } from '@cloudflare/kumo'
import { MENU_CONTENT, MENU_ITEM, MENU_ITEM_DANGER, MENU_POSITIONER_STYLE } from '../menuStyles'
import { useState, useEffect, useRef } from 'react'
import type { WorkspaceMetadataWithTimestamps } from '@verglas/workshop-shared/api'

function initials(title: string | undefined): string {
  const t = (title || 'Untitled').trim()
  if (!t) return 'UG'
  const parts = t.split(/\s+/).slice(0, 2)
  return parts.map((p) => p[0]?.toUpperCase() ?? '').join('') || t.slice(0, 2).toUpperCase()
}

// One row in the sidebar's Favorites / Recent list. Compact, with a monogram avatar, a truncated
// title, and an overflow menu (favorite, rename, share, delete). Favorite/rename/share/delete
// callbacks are passed in by the parent so this row stays a pure presentational component.
export default function SidebarWorkspaceRow({
  workspace,
  collapsed = false,
  onTogglePin,
  onRename,
  onShare,
  onDelete,
}: {
  workspace: WorkspaceMetadataWithTimestamps
  collapsed?: boolean
  onTogglePin: (g: WorkspaceMetadataWithTimestamps) => void
  onRename: (g: WorkspaceMetadataWithTimestamps, newTitle: string) => void
  onShare: (g: WorkspaceMetadataWithTimestamps) => void
  onDelete: (g: WorkspaceMetadataWithTimestamps) => void
}) {
  const [renaming, setRenaming] = useState(false)
  const [renameValue, setRenameValue] = useState(workspace.title || '')
  const inputRef = useRef<HTMLInputElement>(null)

  useEffect(() => {
    if (renaming) inputRef.current?.focus()
  }, [renaming])

  const commit = () => {
    const trimmed = renameValue.trim()
    if (trimmed && trimmed !== workspace.title) onRename(workspace, trimmed)
    setRenaming(false)
  }

  const startRename = () => {
    setRenameValue(workspace.title || '')
    setRenaming(true)
  }

  return (
    <Link
      to="/workspace/$id"
      params={{ id: workspace.id }}
      className="group flex h-8 items-center gap-2 rounded-lg pl-1.5 pr-1 text-[13px] leading-[18px] tracking-[-0.25px] text-kumo-default transition-colors hover:bg-kumo-tint"
      activeProps={{ className: 'flex h-8 items-center gap-2 rounded-lg pl-1.5 pr-1 text-[13px] leading-[18px] tracking-[-0.25px] bg-kumo-fill text-kumo-strong font-medium' }}
      onClick={(e) => {
        if (renaming) e.preventDefault()
      }}
      title={collapsed ? workspace.title || 'Untitled workspace' : undefined}
    >
      <div
        className="flex h-6 w-6 shrink-0 items-center justify-center rounded-md bg-kumo-fill text-[10px] font-medium text-kumo-subtle"
        aria-hidden="true"
      >
        {initials(workspace.title)}
      </div>

      {!collapsed && (
        <>
          {renaming ? (
            <input
              ref={inputRef}
              value={renameValue}
              onChange={(e) => setRenameValue(e.target.value)}
              onBlur={commit}
              onKeyDown={(e) => {
                if (e.key === 'Enter') commit()
                if (e.key === 'Escape') setRenaming(false)
              }}
              className="min-w-0 flex-1 bg-transparent text-[13px] leading-[18px] tracking-[-0.25px] outline-none border-b border-kumo-brand text-kumo-default"
              onClick={(e) => e.preventDefault()}
            />
          ) : (
            <span className="min-w-0 flex-1 truncate">{workspace.title || 'Untitled workspace'}</span>
          )}

          {/* Inside the row's <Link>: stopPropagation blocks the Link's SPA handler, so preventDefault
              is needed to stop the native <a> from navigating. */}
          <div onClick={(e) => { e.stopPropagation(); e.preventDefault() }}>
            <DropdownMenu>
              <DropdownMenu.Trigger
                render={
                  <button
                    type="button"
                    aria-label="Workspace actions"
                    className="flex h-6 w-6 items-center justify-center rounded-md text-kumo-subtle opacity-0 transition-[opacity,color,background-color] group-hover:opacity-100 hover:bg-kumo-fill hover:text-kumo-default focus:opacity-100"
                  >
                    <DotsThree size={14} weight="bold" />
                  </button>
                }
              />
              <DropdownMenu.Content className={MENU_CONTENT} style={MENU_POSITIONER_STYLE}>
                <DropdownMenu.Item
                  onClick={startRename}
                  className={MENU_ITEM}
                >
                  <Pencil size={13} className="mr-2" /> Rename
                </DropdownMenu.Item>
                <DropdownMenu.Item
                  onClick={() => onTogglePin(workspace)}
                  className={MENU_ITEM}
                >
                  <Star size={13} className="mr-2" weight={workspace.pinned ? 'fill' : 'regular'} />
                  {workspace.pinned ? 'Unfavorite' : 'Favorite'}
                </DropdownMenu.Item>
                <DropdownMenu.Item
                  onClick={() => onShare(workspace)}
                  className={MENU_ITEM}
                >
                  <ShareNetwork size={13} className="mr-2" /> Share
                </DropdownMenu.Item>
                <DropdownMenu.Separator />
                <DropdownMenu.Item
                  variant="danger"
                  onClick={() => onDelete(workspace)}
                  className={MENU_ITEM_DANGER}
                >
                  <Trash size={13} className="mr-2" />
                  {workspace.owner ? 'Dismiss' : 'Delete'}
                </DropdownMenu.Item>
              </DropdownMenu.Content>
            </DropdownMenu>
          </div>
        </>
      )}

      {/* Collapsed rows show only the monogram (aria-hidden), so name the link for screen readers. */}
      {collapsed && <span className="sr-only">{workspace.title || 'Untitled workspace'}</span>}
    </Link>
  )
}
