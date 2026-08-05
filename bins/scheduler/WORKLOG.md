# Worklog

- #11: Added the standalone `verglas-scheduler` service binary for Docker. On-prem it claims jobs from the object queue hosted by Verglas, executes stateless worker subprocesses, and reports fenced completions without a shared filesystem mount; cloud can drain the same one-tenant queue and exit.
- #11: Reworked the Docker scheduler into a running pushed-event service and exact-timer queue consumer. It persists events through Verglas before acknowledging them, renews claims during execution, and contains no polling loop, wake endpoint, cloud callback, systemd, or mounted state.
- #11: Made the scheduler own its Postgres queue directly while continuing to read worker declarations from Verglas REST. The event endpoint now acknowledges only after the database enqueue, and the service remains a bounded worker executor with HTTP callbacks but no WebSocket trigger or broker.
