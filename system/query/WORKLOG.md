# Worklog

- #181: Added the Query Worker and Durable Object as a direct Pipeline batch consumer. It atomically maintains declared grouped aggregates, source watermarks, and replay receipts in Turso and exposes only typed, indexed, bounded endpoints through a fixed-name Worker binding.
- #0: Consume the published JavaScript Worker SDK for component and runtime-surface tests. Query no longer reaches into an SDK source tree owned by another repository.
