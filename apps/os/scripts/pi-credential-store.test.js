import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { ScopedCredentialStore } from "./pi-credential-store.mjs";

test("scopes credentials by Workshop user", async () => {
  const directory = await mkdtemp(join(tmpdir(), "verglas-pi-credentials-"));
  try {
    const first = new ScopedCredentialStore(directory, "user-a");
    const second = new ScopedCredentialStore(directory, "user-b");
    await first.modify("openai-codex", async () => ({
      type: "oauth",
      access: "access-a",
      refresh: "refresh-a",
      expires: Date.now() + 60_000,
    }));

    assert.equal((await first.read("openai-codex"))?.access, "access-a");
    assert.equal(await second.read("openai-codex"), undefined);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("serializes refresh-token rotations per provider", async () => {
  const directory = await mkdtemp(join(tmpdir(), "verglas-pi-credentials-"));
  try {
    const store = new ScopedCredentialStore(directory, "user-a");
    await store.modify("anthropic", async () => ({
      type: "oauth",
      access: "access-0",
      refresh: "refresh-0",
      expires: 0,
      generation: 0,
    }));
    await Promise.all(
      [1, 2, 3].map(() =>
        store.modify("anthropic", async (current) => ({
          ...current,
          generation: Number(current?.generation ?? 0) + 1,
        })),
      ),
    );

    assert.equal((await store.read("anthropic"))?.generation, 3);
    assert.deepEqual(await store.list(), [
      { providerId: "anthropic", type: "oauth" },
    ]);
    await store.delete("anthropic");
    assert.equal(await store.read("anthropic"), undefined);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("preserves concurrent updates to different providers", async () => {
  const directory = await mkdtemp(join(tmpdir(), "verglas-pi-credentials-"));
  try {
    const store = new ScopedCredentialStore(directory, "user-a");
    await Promise.all([
      store.modify("openai-codex", async () => ({
        type: "oauth",
        access: "openai-access",
        refresh: "openai-refresh",
        expires: 1,
      })),
      store.modify("anthropic", async () => ({
        type: "oauth",
        access: "anthropic-access",
        refresh: "anthropic-refresh",
        expires: 1,
      })),
    ]);

    assert.equal((await store.read("openai-codex"))?.access, "openai-access");
    assert.equal((await store.read("anthropic"))?.access, "anthropic-access");
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});
