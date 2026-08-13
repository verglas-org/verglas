# Worklog

- #130: Added rendezvous placement and an S3-compatible workload-local gateway over every member of a database or lakehouse cache ring. The gateway preserves request streaming and keeps all multipart operations for an Iceberg object on the same ingress while distributing independent objects across the ring; a separate raw-TCP pool presents one logical Neon safekeeper address, rotates sessions across all members, and skips an unavailable ingress without turning endpoint count into a durability quorum.
