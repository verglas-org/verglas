import { cn as kumoCn } from "@cloudflare/kumo";

/**
 * Combines Tailwind class values with the same conflict resolution used by Kumo.
 *
 * This is the conventional shadcn import point: `@/lib/utils`.
 */
export function cn(...inputs: Parameters<typeof kumoCn>) {
  return kumoCn(...inputs);
}
