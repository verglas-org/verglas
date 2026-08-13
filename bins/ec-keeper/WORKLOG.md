# EC keeper worklog

- #durable-ec: Added `verglas-ec-keeper`, the sole long-lived Neon wire
  listener. It validates explicit EC geometry, uses the cache ring's private
  fragment RPC as a remote-only transport, and retains no fragment durability
  state locally.
