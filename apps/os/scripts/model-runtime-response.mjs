/** Parse one JSON value, tolerating Cursor/Claude envelopes with trailing junk. */
export function parseJsonValue(text) {
  const candidate = text.trim();
  if (!candidate) return null;
  try {
    return JSON.parse(candidate);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    const match = /after JSON at position (\d+)/i.exec(message);
    if (match) {
      try {
        return JSON.parse(candidate.slice(0, Number(match[1])));
      } catch {
        // Fall through to brace extraction below.
      }
    }
    const start = candidate.indexOf("{");
    const end = candidate.lastIndexOf("}");
    if (start === -1 || end <= start) return null;
    const sliced = candidate.slice(start, end + 1);
    try {
      return JSON.parse(sliced);
    } catch (innerError) {
      const innerMessage = innerError instanceof Error ? innerError.message : String(innerError);
      const innerMatch = /after JSON at position (\d+)/i.exec(innerMessage);
      if (!innerMatch) return null;
      try {
        return JSON.parse(sliced.slice(0, Number(innerMatch[1])));
      } catch {
        return null;
      }
    }
  }
}

function looksLikeAssistantMessage(text) {
  return /"tool_calls"\s*:/.test(text) || /"content"\s*:/.test(text);
}

function normalizeToolCall(call) {
  if (!call || typeof call.name !== "string" || !call.name.trim()) return null;
  let argumentsValue = call.arguments;
  if (typeof call.argumentsJson === "string") {
    try {
      argumentsValue = JSON.parse(call.argumentsJson);
    } catch {
      return null;
    }
  }
  // OpenAI wire format nests under function.
  if (call.function && typeof call.function === "object") {
    const args = call.function.arguments;
    return normalizeToolCall({
      name: call.function.name,
      ...(typeof args === "string" ? { argumentsJson: args } : { arguments: args }),
    });
  }
  if (typeof argumentsValue === "string") {
    try {
      argumentsValue = JSON.parse(argumentsValue);
    } catch {
      argumentsValue = {};
    }
  }
  return {
    name: call.name.trim(),
    arguments: argumentsValue && typeof argumentsValue === "object" ? argumentsValue : {},
  };
}

/**
 * Normalize CLI structured output into an OpenAI-shaped assistant message:
 * `{ content: string|null, tool_calls: [{ name, arguments }] }`.
 * Returns null when the payload is empty/truncated so the adapter can retry.
 */
export function parseRuntimeOutput(output) {
  let candidate = output.trim();
  if (!candidate) return null;
  if (candidate.startsWith("```")) {
    candidate = candidate.replace(/^```(?:json)?\s*/i, "").replace(/\s*```$/, "");
  }
  let parsed = parseJsonValue(candidate);
  if (parsed == null) {
    // Truncated assistant JSON must not become a finished text turn.
    return looksLikeAssistantMessage(candidate) ? null : { content: output.trim(), tool_calls: [] };
  }
  if (Array.isArray(parsed)) {
    for (let index = parsed.length - 1; index >= 0; index--) {
      const result = parseRuntimeOutput(JSON.stringify(parsed[index]));
      if (result) return result;
    }
    return null;
  }
  if (parsed.structured_output && typeof parsed.structured_output === "object") {
    return parseRuntimeOutput(JSON.stringify(parsed.structured_output));
  }
  if (typeof parsed.result === "string") return parseRuntimeOutput(parsed.result);
  if (parsed.result && typeof parsed.result === "object") {
    return parseRuntimeOutput(JSON.stringify(parsed.result));
  }

  // Prefer the OpenAI assistant-message shape the adapter asks for.
  if (Array.isArray(parsed.tool_calls) || "content" in parsed) {
    const toolCalls = (Array.isArray(parsed.tool_calls) ? parsed.tool_calls : [])
      .map(normalizeToolCall)
      .filter(Boolean);
    const content = typeof parsed.content === "string" && parsed.content.trim()
      ? parsed.content.trim()
      : null;
    if (toolCalls.length > 0) return { content: null, tool_calls: toolCalls };
    if (content) return { content, tool_calls: [] };
    return null;
  }

  // Legacy adapter shape used during the PoC (`kind` / `calls`). Keep reading it so in-flight
  // CLI output does not break mid-deploy; new prompts no longer ask for it.
  if (parsed.kind === "tool_calls" && Array.isArray(parsed.calls)) {
    const toolCalls = parsed.calls.map(normalizeToolCall).filter(Boolean);
    return toolCalls.length > 0 ? { content: null, tool_calls: toolCalls } : null;
  }
  if (parsed.kind === "text" && typeof parsed.content === "string" && parsed.content.trim()) {
    return { content: parsed.content.trim(), tool_calls: [] };
  }

  return typeof parsed === "string" && parsed.trim()
    ? { content: parsed.trim(), tool_calls: [] }
    : null;
}
