import { createHash, randomUUID } from "node:crypto";
import { mkdir, readFile, rename, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";

function credentialFilename(scope) {
  const digest = createHash("sha256").update(scope).digest("hex");
  return `${digest}.json`;
}

/**
 * Persistent, per-user Pi credential storage for the local model service.
 *
 * Pi performs OAuth refresh through `modify()`, so the per-user promise chain is the
 * synchronization boundary that prevents concurrent refreshes from rotating the same token or
 * overwriting another provider's update. The service is a single process; atomic rename keeps the
 * file crash-safe.
 */
export class ScopedCredentialStore {
  #directory;
  #file;
  #chain = Promise.resolve();

  constructor(directory, scope) {
    if (typeof scope !== "string" || !scope.trim() || scope.length > 512) {
      throw new Error("A valid credential scope is required.");
    }
    this.#directory = directory;
    this.#file = join(directory, credentialFilename(scope));
  }

  async #readAll() {
    try {
      const parsed = JSON.parse(await readFile(this.#file, "utf8"));
      return parsed && typeof parsed === "object" && !Array.isArray(parsed)
        ? parsed
        : {};
    } catch (error) {
      if (error?.code === "ENOENT") return {};
      throw error;
    }
  }

  async #writeAll(credentials) {
    await mkdir(this.#directory, { recursive: true, mode: 0o700 });
    const temporary = `${this.#file}.${randomUUID()}.tmp`;
    try {
      await writeFile(temporary, `${JSON.stringify(credentials)}\n`, {
        mode: 0o600,
      });
      await rename(temporary, this.#file);
    } finally {
      await rm(temporary, { force: true });
    }
  }

  async read(providerId) {
    return (await this.#readAll())[providerId];
  }

  async list() {
    const credentials = await this.#readAll();
    return Object.entries(credentials).flatMap(([providerId, credential]) =>
      credential &&
      (credential.type === "oauth" || credential.type === "api_key")
        ? [{ providerId, type: credential.type }]
        : [],
    );
  }

  async modify(providerId, update) {
    const operation = this.#chain
      .catch(() => undefined)
      .then(async () => {
        const credentials = await this.#readAll();
        const next = await update(credentials[providerId]);
        if (next !== undefined) {
          credentials[providerId] = next;
          await this.#writeAll(credentials);
        }
        return credentials[providerId];
      });
    this.#chain = operation.then(
      () => undefined,
      () => undefined,
    );
    return await operation;
  }

  async delete(providerId) {
    const operation = this.#chain
      .catch(() => undefined)
      .then(async () => {
        const credentials = await this.#readAll();
        delete credentials[providerId];
        if (Object.keys(credentials).length === 0) {
          await rm(this.#file, { force: true });
        } else {
          await this.#writeAll(credentials);
        }
      });
    this.#chain = operation.then(
      () => undefined,
      () => undefined,
    );
    await operation;
  }
}
