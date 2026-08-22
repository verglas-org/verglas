# Vendored Swagger UI

`swagger-ui-bundle.js` and `swagger-ui.css` are copied unmodified from the
`swagger-ui-dist` npm package. They are vendored rather than loaded from a CDN
so the API browser works on a node with no outbound network — an on-prem or
egress-restricted deployment still gets its own API documentation.

| | |
| --- | --- |
| Package | `swagger-ui-dist` |
| Version | 5.32.14 |
| License | Apache-2.0 (see `LICENSE`) |
| Source | `https://unpkg.com/swagger-ui-dist@5.32.14/` |

SHA-256 of the files as vendored:

```
16d93d5cc19e54c98fb0b81157dbb3bd90780aa36b914e128a643b31e54a93f4  swagger-ui-bundle.js
d7f39f764aa18c7b47dd05b9af5613e373e4ac0f3557c2693d52d0abc2464d76  swagger-ui.css
```

The CSS references no external fonts or images: every asset is an inline
`data:` URI, which is what makes the offline case work.

To update, re-download both files at the new version, refresh the version and
hashes above, and check the rendered page still loads with the browser offline.
