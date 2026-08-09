import { Dialog as KumoDialog } from '@cloudflare/kumo'
import type { ComponentProps } from 'react'
import {
  DialogClose,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from './dialog'

/**
 * A modal that requires an explicit decision. It uses Kumo/Base UI's alert-dialog behavior,
 * including the correct ARIA role and focus handling.
 */
export type AlertDialogProps = Omit<
  Extract<ComponentProps<typeof KumoDialog.Root>, { role: 'alertdialog' }>,
  'role'
>

export function AlertDialog(props: AlertDialogProps) {
  return <KumoDialog.Root {...props} role="alertdialog" />
}

export const AlertDialogTrigger = DialogTrigger
export const AlertDialogContent = DialogContent
export const AlertDialogHeader = DialogHeader
export const AlertDialogFooter = DialogFooter
export const AlertDialogTitle = DialogTitle
export const AlertDialogDescription = DialogDescription
export const AlertDialogCancel = DialogClose
export const AlertDialogAction = DialogClose
