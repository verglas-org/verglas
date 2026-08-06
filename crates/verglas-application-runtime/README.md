# Verglas Application runtime

This generic Bun image hosts one generated full-stack Application Vessel. The
module default-exports an object with `fetch(request, ctx)`. The runtime invokes
that method with the same authenticated SDK instance available as both
`ctx.verglas` and `this.verglas`, so generated applications can query lakehouse
data and reflected Integration APIs through one client.

The container receives `VERGLAS_APPLICATION_NAME`, a base64-encoded
`VERGLAS_APPLICATION_MODULE`, and scoped `VERGLAS_DATA_ENDPOINT` /
`VERGLAS_DATA_TOKEN` bindings from the trusted runtime manager.
