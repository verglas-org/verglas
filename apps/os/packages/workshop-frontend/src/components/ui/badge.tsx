import type { ComponentProps } from 'react'
import { cn } from '@/lib/utils'

const badgeVariants = {
  default: 'border-kumo-line bg-kumo-tint text-kumo-default',
  secondary: 'border-transparent bg-kumo-elevated text-kumo-subtle',
  outline: 'border-kumo-line bg-transparent text-kumo-subtle',
  info: 'border-transparent bg-kumo-info-tint text-kumo-info',
  success: 'border-transparent bg-kumo-success-tint text-kumo-success',
  warning: 'border-transparent bg-kumo-warning-tint text-kumo-warning',
  destructive: 'border-transparent bg-kumo-danger-tint text-kumo-danger',
} as const

export type BadgeVariant = keyof typeof badgeVariants

export type BadgeProps = ComponentProps<'span'> & {
  variant?: BadgeVariant
}

/** A compact status or capability label, using the Workshop semantic color tokens. */
export function Badge({ className, variant = 'default', ...props }: BadgeProps) {
  return (
    <span
      data-slot="badge"
      className={cn(
        'inline-flex h-5 items-center rounded-md border px-1.5 text-[11px] leading-4 font-medium whitespace-nowrap',
        badgeVariants[variant],
        className,
      )}
      {...props}
    />
  )
}
