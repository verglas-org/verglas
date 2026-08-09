#!/usr/bin/env node

// Compatibility entry point. The surface contract lives in
// contracts/api-surface.json and is checked by check-api-surface.mjs.

import { spawnSync } from "node:child_process";

const result = spawnSync(process.execPath, ["scripts/check-api-surface.mjs"], {
  stdio: "inherit",
});
process.exit(result.status ?? 1);
