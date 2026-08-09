import { defineConfig } from 'vitest/config'
import { cloudflareTest } from '@cloudflare/vitest-pool-workers'
import capnwebValidate from 'capnweb-validate/vite'

// Tests run inside workerd (via vitest-pool-workers) so they exercise the same runtime APIs as
// production -- e.g. Uint8Array.toHex/fromHex and crypto.subtle used by the sharing module.
export default defineConfig({
  plugins: [
    capnwebValidate(),
    cloudflareTest({
      main: './src/server.ts',
      miniflare: {
        compatibilityDate: '2026-02-02',
        compatibilityFlags: ['experimental', 'nodejs_compat'],
      },
    }),
  ],
  test: {
    include: ['__tests__/*.test.ts'],
  },
})
