# Worklog

- #29: Added the always-on tenant-scoped KV engine with a bounded RAM tier and a checksummed, synchronized NVMe log. Recovery preserves versions, expirations, tombstones, and idempotency while capacity refusal never evicts a live value.
