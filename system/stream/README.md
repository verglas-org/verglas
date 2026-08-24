# Stream system project

Build this literal Cloudflare-style Worker project with the JavaScript SDK:

```sh
node sdks/worker-js/bin/build.mjs system/stream --out /tmp/verglas-stream-build
```

`STREAM_NAME` selects the named object identity. Set `STREAM_AUTH_TOKEN` to
require `Authorization: Bearer <token>` on HTTP ingestion. Set
`STREAM_CORS_ORIGIN` to add the configured CORS response headers. Both settings
are optional and are carried by the manifest `vars` surface.

The internal append route is `POST https://verglas.internal/stream/append` with
a JSON array body. The bounded read route is
`GET https://verglas.internal/stream/read?after=<u64>&limit=<u32>`; `after` is
exclusive and `limit` is at most 1000. An optional
`x-verglas-producer-event-id` header supplies one identity for a one-record
append or a JSON string array with one identity per record.
