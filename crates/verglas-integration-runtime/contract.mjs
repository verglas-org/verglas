/** Validates one generated Integration API and returns its serializable manifest. */
export function describeIntegration(integration, expectedNamespace) {
  const api = integration?.api;
  if (!api || typeof api !== "object") {
    throw new Error("generated Integration must declare api");
  }
  const namespace = api.namespace || expectedNamespace;
  if (namespace !== expectedNamespace) {
    throw new Error(`Integration namespace ${namespace} must match Vessel name ${expectedNamespace}`);
  }
  if (!api.title || !api.description || !api.methods || typeof api.methods !== "object") {
    throw new Error("Integration api requires title, description, and methods");
  }
  const methods = {};
  for (const [name, method] of Object.entries(api.methods)) {
    if (!name || !method || typeof method.handler !== "function") {
      throw new Error(`Integration API method ${name || "<empty>"} requires handler`);
    }
    if (!new Set(["read", "write", "stream"]).has(method.mode)) {
      throw new Error(`Integration API method ${name} has invalid mode ${method.mode}`);
    }
    methods[name] = {
      description: String(method.description || ""),
      mode: method.mode,
      input: method.input ?? true,
      output: method.output ?? true,
    };
  }
  return {
    namespace,
    title: String(api.title),
    description: String(api.description),
    methods,
  };
}

/** Invokes one declared Integration API method with the connected SDK receiver. */
export async function invokeIntegration(integration, methodName, input, runtime) {
  const method = integration?.api?.methods?.[methodName];
  if (!method || typeof method.handler !== "function") {
    throw new Error(`Integration does not declare method ${methodName}`);
  }
  return await method.handler.call(runtime, input, runtime);
}
