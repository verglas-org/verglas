# Worklog

- #91: Updated SDK endpoint documentation to call the local process
  `verglas-server`. Client wire behavior is unchanged.
- #11: Taught the endpoint runner to reconstruct manual, HTTP callback, cron, and data-update events from the scheduler harness environment. Removed WebSocket worker trigger types while leaving the catalog change-feed transport unchanged.
- #11: Replaced the TypeScript runtime trigger union with a CloudEvents 1.0 contract and generic event subscriptions. The endpoint runner validates one structured CloudEvent, and the reference workers consume event-specific data from its payload.
