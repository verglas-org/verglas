/** Invokes one generated Application with the connected SDK on `this.verglas`. */
export async function invokeApplication(application, request, runtime) {
  if (!application || typeof application.fetch !== "function") {
    throw new Error("generated Application must default-export an object with fetch(request, ctx)");
  }
  const result = await application.fetch.call(runtime, request, runtime);
  return result instanceof Response ? result : Response.json(result ?? null);
}
