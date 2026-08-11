# Worklog

- #107: Added a standalone authenticated queue container that serves one dedicated Neon database. Poll and acknowledgement use server time and fenced receipts; the process has no filesystem queue path.
- #20: Added the authenticated NDJSON subscription route for durable topic deliveries. The service streams its `QueueStore` subscription directly and retains the existing fenced acknowledgement contract.
