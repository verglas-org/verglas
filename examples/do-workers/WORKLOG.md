# Worklog

- #171: Added dedicated JavaScript and Python cold-chain Worker examples plus a
  production-only harness that builds the Stream, Pipeline, Sink, and Catalog
  artifacts into one aggregate gateway manifest. The harness preserves one data
  root through a full gateway/celld restart and refuses to claim success without
  explicit remote Turso credentials and the production runtime binaries. The
  aggregate route uses Stream object `events` to match Pipeline's baked source,
  and does not add the products' internal `*_DO` bindings as global routes.
  App status uses the same `SINK_A` identity as Pipeline delivery, and the
  published payload is scalar so the stock `SELECT *` Iceberg inference stays
  within its supported flat-row surface.
- #171: Switched the Python cold-chain manifest's Pipeline, Sink, and Catalog
  entries from fake Durable Object namespaces to strict direct service bindings;
  the Worker now calls each configured service with `fetch` while preserving its
  `COUNTER` Durable Object and `STREAM` Pipeline binding.
- #171: Required the operator-owned runtime host configuration for production
  cold-restart runs and pass it through celld only to Catalog children. Build-only
  verification remains credential-free, while a real run now fails before launch
  unless Turso, origin/cache, and runtime binary prerequisites are all explicit.
