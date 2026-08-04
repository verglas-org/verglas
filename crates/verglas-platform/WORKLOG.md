# verglas-platform worklog

- #331: New crate. Moved the platform registry out of `verglas-agentmem::platform`
  into its own crate: the `verglas_sys` system catalog (source/MV/sink/watermark
  declarations, their Iceberg schemas and Arrow codecs), the `SystemState` /
  `PlatformError` types, and the unified `Deployment` projection (§7.1). This is
  the local registry the daemon supervisor and the CLI both read — not
  memory-specific — so it now has one home below the harnesses. The
  memory-specific pipeline declaration (`declare_memory_pipeline`) stays with the
  memory workflow; the generic `injection_allowed` switch check moved here but now
  takes the sink name as a parameter rather than hard-coding the memory sink.
- #331: Took ownership of the generic registry tests (platform_tables.rs,
  deployment_parity.rs), moved here from verglas-memory-jobs since they exercise
  the SystemCatalog and Deployment projection that live here. The two
  memory-pipeline-specific tests (declare_memory_pipeline, injection_allowed) went
  to verglas-memory-jobs instead. Added the hermetic-SQLite-catalog dev-deps.
- index registry: Added `verglas_sys.indexes`, the durable append-only registry
  for vector (ANN) indexes, mirroring the source/MV/sink pattern. A row is keyed
  by `<cluster_id>/<target>/<field>` (composed by `index_row_name`) and carries
  target_kind, target, field, metric, params (R/L/alpha + id column as JSON),
  cluster_id, reflected_snapshot, blob_ref, state, created_by, created/updated
  timestamps, and revision. The blob itself never travels through the registry —
  it stays cluster-local in the shadow store; the row records only its
  `blob_ref` so a reboot knows whether to rehydrate a present blob or rebuild.
  New SystemCatalog methods: register_index(_state), list_indexes,
  list_running_indexes_for_cluster (the per-cluster reboot view), get_index,
  index_revisions, set_index_state, set_index_build. Same target+field on two
  clusters are two independent rows — a daemon rehydrates only rows under its own
  cluster id.

- cloud-agnostic sweep: removed every Cloudflare/R2 mention and tenant-named
  fixture from code, docs, and tests. Comments now describe the constraint
  ("strict S3-compatible stores reject variable-size parts", "some managed REST
  catalogs gzip responses") instead of naming a vendor; test fixtures use
  neutral hosts/entities (storage.example.com, acme, blobstore). No behavior
  change — the daemon and SDK are wiring-agnostic over any S3 bucket + Iceberg
  REST catalog.
- workers: Added the `verglas_sys.workers` registry — WorkerRow/WorkerSpec, the
  Iceberg schema/codecs, and SystemCatalog register/list/get/set_state, the
  single deployment registry that replaces sources/mvs/sinks. Added
  translate_legacy_to_workers: active `sources` rows become cron/webhook/on-demand
  workers (idempotent by name); MVs and sinks are reported as dropped (not
  translated) so the boot path logs them loudly. Legacy row types stay readable
  only for that one-time translation.
- chore: Remove the legacy sources/mvs/sinks registry, translate_legacy_to_workers, and injection_allowed. Workers, watermarks, and indexes are the only verglas_sys surfaces.
