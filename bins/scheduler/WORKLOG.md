# Worklog

- feat: Made the scheduler the authenticated Postgres control plane for worker declarations,
  immutable lifecycle revisions, manual runs, bounded job history, and runtime secrets. Secret
  values are AES-256-GCM envelopes at rest and are decrypted only while preparing a worker run;
  the scheduler no longer calls Verglas REST for a singleton worker catalog.

- #11: Added the standalone `verglas-scheduler` service binary for Docker. On-prem it claims jobs from the object queue hosted by Verglas, executes stateless worker subprocesses, and reports fenced completions without a shared filesystem mount; cloud can drain the same one-tenant queue and exit.
- #11: Reworked the Docker scheduler into a running pushed-event service and exact-timer queue consumer. It persists events through Verglas before acknowledging them, renews claims during execution, and contains no polling loop, wake endpoint, cloud callback, systemd, or mounted state.
- #11: Made the scheduler own its Postgres queue directly while continuing to read worker declarations from Verglas REST. The event endpoint now acknowledges only after the database enqueue, and the service remains a bounded worker executor with HTTP callbacks but no WebSocket trigger or broker.
- #11: Replaced scheduler-specific trigger-source identities and payload variants with CloudEvents 1.0. Cron reconciliation now emits and reconstructs progress from stable CloudEvent source and id attributes.
- #11: Materialized worker bundles into a fresh temporary directory for each execution and passed declared environment variables to the subprocess. Relative bundled entrypoints now run without a shared filesystem, while unresolved secrets and unsafe paths fail before execution.
- #18: Renamed the scheduler's portable-bundle fixture to the neutral market-data ingestion example used by the worker guides. SPY remains test input data instead of becoming a worker, file, or table name.
- #109: Replaced scheduler subprocess execution with authenticated container-runtime builds and bounded runs. Registration stores an immutable image identity, while each claim resolves secrets at run time and renews its durable lease during the container call.
- #66: Dropped cloud-placement wording from the standalone scheduler binary docs.
