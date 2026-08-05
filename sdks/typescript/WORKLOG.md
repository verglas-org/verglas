# Worklog

- #91: Updated SDK endpoint documentation to call the local process
  `verglas-server`. Client wire behavior is unchanged.
- #11: Taught the endpoint runner to reconstruct manual, HTTP callback, cron, and data-update events from the scheduler harness environment. Removed WebSocket worker trigger types while leaving the catalog change-feed transport unchanged.
