# Worklog

- #126: Replaced the RAM-only protocol comparison with a real DuckDB/Quack
  object-store benchmark. It requires a dataset larger than operator memory,
  observed spill and origin traffic, cold/warm/shared-cache reads, bounded
  containers, result equivalence, and durable origin readback for both writes.
