import { Dialog as KumoDialog } from '@cloudflare/kumo'
import type { ComponentProps } from 'react'
import { cn } from '@/lib/utils'

/** The accessible dialog root supplied by Kumo/Base UI. */
export const Dialog = KumoDialog.Root
/** Opens a {@link Dialog}; use Kumo's `render` prop to supply the trigger element. */
export const DialogTrigger = KumoDialog.Trigger
/** Closes a {@link Dialog}; use Kumo's `render` prop to supply the close element. */
export const DialogClose = KumoDialog.Close

/** The modal content surface. */
export function DialogContent({ className, ...props }: ComponentProps<typeof KumoDialog>) {
  return (
    <KumoDialog
      className={cn('w-[min(32rem,calc(100vw-2rem))] rounded-xl border border-kumo-line bg-kumo-base p-0 shadow-xl', className)}
      {...props}
    />
  )
}

/** A dialog header. */
export function DialogHeader({ className, ...props }: ComponentProps<'div'>) {
  return <div data-slot="dialog-header" className={cn('flex flex-col gap-1.5 px-5 pt-5', className)} {...props} />
}

/** A dialog footer for its actions. */
export function DialogFooter({ className, ...props }: ComponentProps<'div'>) {
  return <div data-slot="dialog-footer" className={cn('flex flex-row justify-end gap-2 px-5 pb-5', className)} {...props} />
}

/** A dialog title. */
export function DialogTitle({ className, ...props }: ComponentProps<typeof KumoDialog.Title>) {
  return <KumoDialog.Title className={cn('text-base font-medium tracking-[-0.25px] text-kumo-default', className)} {...props} />
}

/** A dialog description. */
export function DialogDescription({ className, ...props }: ComponentProps<typeof KumoDialog.Description>) {
  return <KumoDialog.Description className={cn('text-sm text-kumo-subtle', className)} {...props} />
}
