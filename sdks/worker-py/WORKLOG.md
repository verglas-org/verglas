# Worklog

- #0: Added the pinned componentize-py Python Worker builder, generated-binding
  adapter, transactional storage and socket authoring helpers, tests, and
  documentation. The build writes content-addressed components and a binding
  manifest for the supported Wrangler subset.
- #0: Matched componentize-py's synchronous WIT export convention and enabled
  its WASI stubs so generated components import only Verglas capabilities. SQL
  remains explicitly wired to the sibling `sql-rows` WIT verb without an Arrow
  or legacy fallback.
- #0: Regenerated the bindings after `storage.sql-rows` landed and rebuilt the
  py-counter component. The final component validates structurally and imports
  the versioned storage capability with the JSON-row verb present.
- #0: Added explicit checks for the WIT unsigned integer widths before storage,
  alarm, socket, and response calls. This keeps invalid Python integers from
  reaching the component boundary as truncated values.
