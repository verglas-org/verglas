function parseBackendHost(backendHost) {
  const trimmed = backendHost.trim();
  if (!trimmed) return null;
  if (trimmed.includes("://")) {
    throw new Error("VITE_BACKEND_HOST must include a valid host with an optional port.");
  }

  let url;
  try {
    url = new URL(`http://${trimmed}`);
  } catch {
    if (/(^.*\]:|^[^:]+:)[^:]+$/.test(trimmed)) {
      throw new Error("VITE_BACKEND_HOST must include a valid port between 1 and 65535.");
    }
    throw new Error("VITE_BACKEND_HOST must include a valid host with an optional port.");
  }

  return url;
}

export function getWranglerIpFromBackendHost(backendHost) {
  const url = parseBackendHost(backendHost);
  return url?.hostname.replace(/^\[|\]$/g, "") ?? null;
}

export function getWranglerPortFromBackendHost(backendHost) {
  const url = parseBackendHost(backendHost);
  if (!url?.port) return null;

  const port = Number(url.port);
  if (port < 1) {
    throw new Error("VITE_BACKEND_HOST must include a valid port between 1 and 65535.");
  }

  return url.port;
}
