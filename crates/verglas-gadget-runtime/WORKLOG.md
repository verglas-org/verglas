# Worklog

- #31: Added the Rust Gadget runtime, authenticated immutable bundle registry,
  multi-Gadget local process supervision, single-target cloud mode, Bun Cap'n
  Web host, container image, and default Compose service.
- #31: Wired the default Compose runtime to the Verglas KV API and changed
  per-Gadget storage to the valid exact namespace `gadget.<id>`.
- #43: Bundled the existing TypeScript SDK into the Gadget runtime and injected
  `env.VERGLAS` through a per-Gadget data capability. The trusted parent keeps
  upstream credentials, clears the child environment, and limits sinks to SQL,
  tables, queues, graphs, vectors, and KV while excluding worker control routes.
