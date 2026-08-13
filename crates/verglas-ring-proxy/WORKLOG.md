# Worklog

- #130: Added rendezvous placement and an S3-compatible workload-local gateway over every member of a database or lakehouse cache ring. The gateway preserves request streaming and keeps all multipart operations for an Iceberg object on the same ingress while distributing independent objects across the ring.
