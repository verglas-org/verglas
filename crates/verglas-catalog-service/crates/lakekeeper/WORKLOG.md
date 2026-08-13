# Worklog

- #135: Consolidated the Lakekeeper core and its hosted Iceberg route changes
  into the Verglas repository. Hosted catalog requests can now bind directly to
  the CRaft storage contract without using the PostgreSQL catalog backend.
- #135: Exposed the canonical mounted Iceberg REST capability set to hosted
  catalog adapters. Clients now receive route declarations from the same typed
  endpoint source used by Lakekeeper instead of a separately maintained list.
