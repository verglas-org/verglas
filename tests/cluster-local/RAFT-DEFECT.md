# Measured defect: the object commit path never runs consensus

Profiled on 4 nodes (6 vCPU / 12 GB / 48 GB NVMe each) against Cloudflare R2.

## Ack path, p50 of 75 samples

| phase | p50 | share |
| --- | --- | --- |
| setup | 3.1 ms | 2% |
| EC fragment quorum (3 of 4 durable) | 32.7 ms | 21% |
| consensus commit | 115.6 ms | 74% |
| journal fsync | 4.8 ms | 3% |

Total ack ~157 ms. A direct R2 PUT is ~220 ms p50.

## What the 115.6 ms actually is

Not Raft. `ConsensusPlane::submit_with_timeout` polls:

```rust
loop {
    if let Some(leader) = local.leader_id().await {
        if leader == self.ring.safekeeper_id() {
            match local.execute(request.clone()).await { .. }   // never reached
        }
        // forwarded on every observed sample
        if let Ok(r) = self.network(group)?.command(leader, encoded).await { .. }
    }
    tokio::time::sleep(Duration::from_millis(25)).await;
}
```

Instrumented output under load:

```
SUBMIT spin=1 forwarded=1 elapsed_ms=5.0   group=object/a250a3ffd9562b7e/3
SUBMIT spin=8 forwarded=8 elapsed_ms=240.8 group=object/a250a3ffd9562b7e/2
FORWARDFAIL group=object/a250a3ffd9562b7e/0 leader=14577790744218774011
            self=14577794042753658644
            err=NetworkError: group leader refused application command
```

- `forwarded == spins` on every sample: the accepting node is never the group
  leader, so it forwards.
- The leader **refuses the application command**, every time.
- The loop sleeps a flat 25 ms and retries, 8+ times.
- Timing inside `commit_header` never printed once across four nodes, so
  `local.execute` — the real Raft path — is never reached.

The 115.6 ms is a retry storm against a leader that rejects the command. Raft
itself is fast: the `spin=1 elapsed_ms=5.0` samples are a completed submit.

## What correct looks like

33 ms EC quorum + one real Raft round trip + 5 ms journal ~= 45 ms, against
R2's 220 ms. That is the ~5x the architecture is supposed to deliver. Today it
is 157 ms and barely beats the origin.

## Three defects

1. **The leader refuses forwarded application commands.** Root cause. Find why
   the receiving side rejects it and fix the owning invariant.
2. **The submit path polls.** A fixed 25 ms sleep-retry loop sits in the write
   hot path. Leader resolution must be event-driven: learn the leader, submit,
   and surface a real error when the leader rejects. No sleep-poll loop.
3. **Metadata is erasure-coded.** `commit_header` stages the payload with
   `ReplicationMode::Coded` before the Raft append and seals it after — three
   network phases to commit a few hundred bytes of JSON that fit inside the
   Raft entry. EC is for object payloads, not for commit metadata.
