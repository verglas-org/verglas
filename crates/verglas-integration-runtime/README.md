# Verglas Integration runtime

This generic Bun image executes one generated Integration Vessel. The OS passes
an immutable JavaScript module and declarative setup definition when it creates
the Vessel. User configuration is stored in a Vessel-specific Verglas KV
namespace, verified by the generated module, and never returned by the runtime.

Generated modules default-export an object with `verify(ctx)` and a reflected
`api` contract. They may also implement `start(ctx)` for long-lived feeds or
`fetch(request, ctx)` for private setup-specific HTTP behavior. Every callback
receives the same authenticated SDK as both `ctx.verglas` and `this.verglas`.
The context also provides frozen configuration, `emit(CloudEvent)`, and
`enqueue(queue, rows)` methods.

```js
export default {
  async verify(ctx) {
    await ctx.verglas.namespace.example.status.get();
    return {ok: true};
  },
  api: {
    namespace: "example",
    title: "Example",
    description: "A reflected external API.",
    methods: {
      "records.list": {
        mode: "read",
        description: "Lists records.",
        input: {type: "object"},
        output: {type: "object"},
        async handler(input) {
          return this.verglas.table(input.table).scan({limit: 100});
        },
      },
    },
  },
};
```

The runtime publishes the data-only manifest at `GET /v1/namespace` and accepts
declared method calls at `POST /v1/namespace/invoke/{method}`. The Docker runtime
manager discovers those private endpoints and exposes them through the primary
Verglas server's `/v1/namespaces/*` gateway.
