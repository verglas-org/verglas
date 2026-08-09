# Worklog

- feat: Moved the queue-scoped worker registry into scheduler Postgres. Worker declarations,
  immutable revisions, lifecycle state, cron reconciliation, encrypted runtime secrets, manual
  dispatch, and bounded job history no longer depend on a singleton Iceberg system catalog or the
  Verglas data-server worker routes.

- #11: Added the immutable object-backed tenant queue, fenced lease generations, cron planning, and durable scale-to-zero callback ordering. Scheduler correctness now reconstructs entirely from create-only objects instead of process state, filesystem guards, or mutable database rows.
- #11: Documented the scheduler crate as a durable queue rather than an execution or placement service. On-prem supplies a local executor while cloud claims the same run contract and selects a fleet microVM container in its private execution layer.
- #11: Defined the backend-neutral `RunQueue` contract and completed lease renewal, expiry fencing, retry timing, and immutable attempt reconstruction. Worker events are limited to manual, cron, complete HTTP callbacks, and data updates; the queue contains no connection or fleet-placement logic.
- #11: Replaced the prototype object queue with transactional Postgres storage. Jobs are idempotent per tenant queue, claims use `SKIP LOCKED`, and generation-fenced renewals, retries, completions, and attempt history survive scheduler restarts.
- #11: Materialized the first future run when a live cron worker has no prior job. This prevents each wake from advancing past the deadline without ever creating runnable work.
- #11: Made CloudEvents the durable queue object and removed the separate trigger-source union. Queue idempotency is now derived solely from worker, CloudEvent source, and CloudEvent id.

- #66: Rewrote the crate README so the durable queue boundary is local-executor only and no longer mentions Firecracker, microVMs, or a cloud consumer plane.
- #66: Neutralized RunQueue docs (no local-vs-cloud scheduler contrast).
