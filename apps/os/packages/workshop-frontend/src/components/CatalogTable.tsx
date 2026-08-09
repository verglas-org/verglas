import { ArrowLeft } from '@phosphor-icons/react'
import type { ReactNode } from 'react'

/** Shared page chrome for Workspaces, Integrations, Applications, Jobs. */
export function CatalogPage({
  title,
  description,
  children,
  actions,
}: {
  title: string
  description: string
  children: ReactNode
  actions?: ReactNode
}) {
  return (
    <div className="mx-auto w-full max-w-5xl px-6 pb-12 pt-10 sm:px-10">
      <header className="mb-6 flex items-end justify-between gap-4">
        <div>
          <h1 className="text-2xl font-semibold tracking-tight text-kumo-default">{title}</h1>
          <p className="mt-1 text-[13px] text-kumo-subtle">{description}</p>
        </div>
        <div className="flex items-center gap-2">
          {actions}
        </div>
      </header>
      {children}
    </div>
  )
}

/** One tile in the shared 2-wide catalog grid. */
export type CatalogCard = {
  id: string
  icon: ReactNode
  primary: string
  secondary?: string
  tertiary?: string
  meta?: ReactNode
  onOpen: () => void
}

/**
 * Fixed 2-column card stack. Click a tile to open CatalogDetailCard as an
 * inline slide-in view (not a popup, not a sidebar).
 */
export function CatalogTable({
  cards,
  empty,
}: {
  cards: CatalogCard[]
  empty: ReactNode
}) {
  if (cards.length === 0) return <CatalogEmpty>{empty}</CatalogEmpty>

  return (
    <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
      {cards.map((card) => (
        <button
          key={card.id}
          type="button"
          onClick={card.onOpen}
          className="flex w-full cursor-pointer items-start gap-3 rounded-xl border border-kumo-line bg-kumo-base p-4 text-left transition-colors hover:border-kumo-brand/40 hover:bg-kumo-tint/50"
        >
          <div className="mt-0.5 flex h-10 w-10 shrink-0 items-center justify-center rounded-lg bg-kumo-fill text-kumo-brand">
            {card.icon}
          </div>
          <div className="min-w-0 flex-1">
            <div className="flex flex-wrap items-center gap-2">
              <div className="truncate text-sm font-medium text-kumo-default">{card.primary}</div>
              {card.meta}
            </div>
            {card.secondary && (
              <div className="mt-1 line-clamp-2 text-[12px] leading-4 text-kumo-subtle">{card.secondary}</div>
            )}
            {card.tertiary && (
              <div className="mt-1.5 truncate font-mono text-[10px] text-kumo-inactive">{card.tertiary}</div>
            )}
          </div>
        </button>
      ))}
    </div>
  )
}

/**
 * Inline slide-in object view. Replaces the catalog grid in-page — not a popup,
 * overlay, or sidebar. Back returns to the grid.
 */
export function CatalogDetailCard({
  open,
  title,
  subtitle,
  meta,
  screenshotUrl,
  onBack,
  children,
  footer,
}: {
  open: boolean
  title: string
  subtitle?: string
  meta?: ReactNode
  /** Optional hero screenshot; when set, the card grows taller to showcase it. */
  screenshotUrl?: string
  onBack: () => void
  children: ReactNode
  footer?: ReactNode
}) {
  if (!open) return null

  return (
    <div className="catalog-detail-slide">
      <button
        type="button"
        onClick={onBack}
        className="mb-4 inline-flex h-9 cursor-pointer items-center gap-1.5 rounded-lg px-1 text-[13px] font-medium text-kumo-subtle hover:text-kumo-default"
      >
        <ArrowLeft size={16} />
        Back
      </button>

      <article
        className={`overflow-hidden rounded-2xl border border-kumo-line bg-kumo-base ${
          screenshotUrl ? 'min-h-[28rem]' : ''
        }`}
      >
        <div className="flex items-start justify-between gap-4 px-5 py-4 sm:px-6">
          <div className="min-w-0">
            <div className="flex flex-wrap items-center gap-2">
              <h2 className="truncate text-lg font-semibold tracking-tight text-kumo-default">{title}</h2>
              {meta}
            </div>
            {subtitle && <p className="mt-1 text-[13px] leading-5 text-kumo-subtle">{subtitle}</p>}
          </div>
        </div>

        {screenshotUrl && (
          <div className="border-y border-kumo-line bg-kumo-tint/30">
            <img
              src={screenshotUrl}
              alt=""
              className="aspect-[16/9] max-h-[min(420px,42vh)] w-full object-cover object-top"
            />
          </div>
        )}

        <div className="px-5 py-5 sm:px-6">{children}</div>

        {footer && (
          <div className="flex flex-wrap items-center justify-end gap-2 border-t border-kumo-line px-5 py-3 sm:px-6">
            {footer}
          </div>
        )}
      </article>
    </div>
  )
}

export function CatalogEmpty({ children }: { children: ReactNode }) {
  return (
    <div className="rounded-xl border border-dashed border-kumo-line px-6 py-12 text-center text-sm text-kumo-subtle">
      {children}
    </div>
  )
}

export function CatalogError({ message }: { message: string }) {
  return (
    <div className="mb-4 rounded-lg border border-kumo-danger/30 bg-kumo-danger-tint px-4 py-3 text-sm text-kumo-danger">
      {message}
    </div>
  )
}

export function CatalogStatus({ value, good }: { value: string; good?: boolean }) {
  return (
    <span
      className={`rounded-full px-2 py-0.5 text-[10px] font-semibold uppercase ${
        good ? 'bg-kumo-success-tint text-kumo-success' : 'bg-kumo-tint text-kumo-subtle'
      }`}
    >
      {value}
    </span>
  )
}
