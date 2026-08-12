# Worklog

- #107: Added validated queue plans that assign every queue its own managed Neon deployment and independently scalable service container. Invalid names and empty tenants fail before provisioning begins.
- #20: Added exact queue topics, idempotent message identities, and push subscriptions backed by PostgreSQL `LISTEN/NOTIFY`. Durable consumer-group leases and fenced acknowledgements remain the delivery source of truth; notifications only wake claims and lease deadlines drive redelivery.
- #20: Serialized PostgreSQL queue schema installation with a transaction-scoped advisory lock. Concurrent queue clients can now start against an empty database without racing on implicit sequence creation.
