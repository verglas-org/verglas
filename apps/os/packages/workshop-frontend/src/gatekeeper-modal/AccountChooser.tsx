/** Gatekeepers removed — type-compatible stub. */
import type { AccountDescription, SupportedResource, VendorDescription } from "@verglas/workshop-shared/gatekeeper"

export type AccountOption = {
  id: number
  description: AccountDescription
  vendorId: string
  vendorDescription: VendorDescription
  supportedResources: SupportedResource[]
  credentialsValid: boolean
  accountId?: number
  displayName?: string
  avatarUrl?: string
  logoUrl?: string
}

export function AccountAvatar(_props: { avatarUrl: string | undefined; logoUrl: string | undefined }) {
  return null
}

export function AccountChooser(_props: {
  accounts?: AccountOption[]
  onSelect?: (account: AccountOption) => void
  [key: string]: unknown
}) {
  return null
}
