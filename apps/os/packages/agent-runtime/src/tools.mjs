function schema(properties, required = Object.keys(properties)) {
  return { type: "object", properties, required, additionalProperties: false };
}

export const toolDefinitions = [
  {
    type: "function",
    function: {
      name: "requestPermission",
      description: "Ask the user to delegate missing Verglas access to this agent. End the turn after calling this tool.",
      parameters: schema({
        resourceId: { type: "string" },
        actions: { type: "array", items: { type: "string" } },
        reason: { type: "string" },
      }),
    },
  },
  {
    type: "function",
    function: {
      name: "listLakehouse",
      description: "List bounded Verglas databases and each Lakehouse's scoped tables and graphs.",
      parameters: schema({}, []),
    },
  },
  {
    type: "function",
    function: {
      name: "queryLakehouse",
      description: "Run bounded SQL against one named Verglas Lakehouse database.",
      parameters: schema({
        database: { type: "string" },
        sql: { type: "string" },
      }),
    },
  },
  {
    type: "function",
    function: {
      name: "deployApplication",
      description: "Build and start a standalone Application Vessel from a complete TypeScript project.",
      parameters: schema({
        name: { type: "string" },
        title: { type: "string" },
        description: { type: "string" },
        files: { type: "object", additionalProperties: { type: "string" } },
      }),
    },
  },
  {
    type: "function",
    function: {
      name: "deployIntegration",
      description: "Build and start a standalone Integration Vessel API from a complete TypeScript project.",
      parameters: schema({
        name: { type: "string" },
        files: { type: "object", additionalProperties: { type: "string" } },
        environment: { type: "object", additionalProperties: { type: "string" } },
      }, ["name", "files"]),
    },
  },
  {
    type: "function",
    function: {
      name: "deployJob",
      description: "Register a Verglas TypeScript worker Job with triggers and an output table.",
      parameters: schema({
        name: { type: "string" },
        code: { type: "string" },
        output: { type: "string" },
        triggers: { type: "array", items: { type: "object" } },
      }, ["name", "code", "output"]),
    },
  },
];

async function request(base, token, path, options = {}) {
  const response = await fetch(`${base.replace(/\/+$/, "")}${path}`, {
    ...options,
    headers: {
      Authorization: `Bearer ${token}`,
      ...(options.body === undefined ? {} : { "Content-Type": "application/json" }),
      ...options.headers,
    },
  });
  const text = await response.text();
  if (!response.ok) throw new Error(`Verglas ${path} failed: HTTP ${response.status} — ${text}`);
  return text ? JSON.parse(text) : null;
}

export function createToolExecutor(env, emit) {
  const admin = env.VERGLAS_DATA_ENDPOINT;
  const dataToken = env.VERGLAS_DATA_TOKEN;
  const runtime = env.VERGLAS_CONTAINER_RUNTIME_URL;
  const runtimeToken = env.VERGLAS_CONTAINER_RUNTIME_TOKEN;
  if (!admin || !dataToken || !runtime || !runtimeToken) {
    throw new Error("Agent runtime is missing Verglas data or container-runtime configuration.");
  }

  const access = env.VERGLAS_ACCESS_URI;
  const accessToken = env.VERGLAS_ACCESS_SERVICE_TOKEN;
  const tenantId = env.VERGLAS_TENANT_ID;
  const principalId = env.VERGLAS_AGENT_PRINCIPAL_ID;
  if (!access || !accessToken || !tenantId || !principalId) {
    throw new Error("Agent runtime is missing its Verglas authorization identity.");
  }

  const requireAccess = async (resourceId, action) => {
    const decision = await request(access, accessToken, "/v1/access/check", {
      method: "POST",
      body: JSON.stringify({
        tenant_id: tenantId,
        principal_id: principalId,
        resource_id: resourceId,
        action,
      }),
    });
    if (!decision.allowed) {
      throw new Error(
        `Permission denied: ${principalId} needs ${action} on ${resourceId}. ` +
        "Call requestPermission with that exact resource and action.",
      );
    }
  };

  return async (name, args) => {
    switch (name) {
      case "requestPermission": {
        const allowedActions = new Set([
          "discover", "describe", "query", "append", "modify", "create_child",
          "execute", "use_secret", "deploy", "pass_grants", "manage_grants", "own",
        ]);
        const actions = [...new Set(Array.isArray(args.actions) ? args.actions : [])];
        if (!args.resourceId || !args.reason?.trim() || actions.length === 0 ||
            actions.some(action => !allowedActions.has(action))) {
          throw new Error("A permission request requires a resource, valid actions, and a reason.");
        }
        const requestId = `${env.VERGLAS_AGENT_CHAT_ID}:${crypto.randomUUID()}`;
        await emit({
          type: "permissionRequest",
          requestId,
          principalId,
          resourceId: args.resourceId,
          actions,
          reason: args.reason.trim(),
          state: "pending",
        });
        return { permissionRequested: true, requestId };
      }
      case "listLakehouse": {
        await requireAccess("tenant", "discover");
        const databaseBody = await request(access, accessToken, "/v1/databases");
        const databases = (databaseBody.databases ?? []).slice(0, 100);
        const tables = [];
        for (const database of databases) {
          if (database.type !== "lakehouse") continue;
          await requireAccess(`database/${database.name}`, "discover");
          const catalog = `/v1/databases/${encodeURIComponent(database.name)}/catalog/v1`;
          const namespaceBody = await request(admin, dataToken, `${catalog}/namespaces`);
          for (const namespace of (namespaceBody.namespaces ?? []).slice(0, 100)) {
            const encoded = encodeURIComponent(namespace.join("\u001f"));
            const listed = await request(admin, dataToken, `${catalog}/namespaces/${encoded}/tables`);
            for (const table of (listed.identifiers ?? []).slice(0, 500)) {
              if (tables.length === 1000) break;
              tables.push({
                database: database.name,
                namespace: table.namespace,
                name: table.name,
                qualifiedName: [...table.namespace, table.name].join("."),
              });
            }
            if (tables.length === 1000) break;
          }
          if (tables.length === 1000) break;
        }
        const graphs = tables
          .filter(table => table.name.endsWith("_nodes"))
          .map(table => ({
            database: table.database,
            namespace: table.name.slice(0, -"_nodes".length),
          }));
        return { databases, tables, indexes: [], graphs };
      }
      case "queryLakehouse": {
        const database = String(args.database ?? "").trim();
        if (!database) throw new Error("queryLakehouse requires a database.");
        await requireAccess(`database/${database}`, "query");
        return await request(
          admin,
          dataToken,
          `/v1/databases/${encodeURIComponent(database)}/query`,
          {
          method: "POST",
          body: JSON.stringify({ sql: args.sql }),
          },
        );
      }
      case "deployApplication": {
        await requireAccess("tenant", "deploy");
        const result = await request(runtime, runtimeToken,
          `/v1/vessels/${encodeURIComponent(args.name)}/project`, {
            method: "PUT",
            body: JSON.stringify({
              name: args.name,
              role: "application",
              project: { files: args.files },
              environment: {},
              http: { port: 8380, healthPath: "/" },
            }),
          });
        await emit({
          type: "applicationPreview",
          vesselName: args.name,
          previewUrl: `/apps/${encodeURIComponent(args.name)}/`,
          title: args.title,
          description: args.description,
        });
        return result;
      }
      case "deployIntegration":
        await requireAccess("tenant", "deploy");
        return await request(runtime, runtimeToken,
          `/v1/vessels/${encodeURIComponent(args.name)}/project`, {
            method: "PUT",
            body: JSON.stringify({
              name: args.name,
              role: "integration",
              project: { files: args.files },
              environment: args.environment ?? {},
              http: { port: 8370, healthPath: "/health" },
            }),
          });
      case "deployJob":
        await requireAccess("tenant", "deploy");
        return await request(admin, dataToken, "/v1/workers", {
          method: "POST",
          body: JSON.stringify({
            name: args.name,
            code: JSON.stringify({
              exec: [
                "sh", "-c",
                "exec /usr/local/bin/bun /sdks/typescript/src/subprocess/endpoint-run.ts \"file://$PWD/source.ts\"",
              ],
              cwd: ".",
            }),
            output: args.output,
            triggers: JSON.stringify(args.triggers ?? []),
            config: JSON.stringify({files: {"source.ts": args.code}, env: {}}),
            created_by: principalId,
          }),
        });
      default:
        throw new Error(`Unknown agent tool ${name}.`);
    }
  };
}
