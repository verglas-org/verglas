import assert from "node:assert/strict";
import { test } from "node:test";
import {
  assertImmutableManifest,
  releaseManifestKey,
} from "./release/upload-release.mjs";

test("release manifests publish below the OS namespace", () => {
  assert.equal(
    releaseManifestKey("r000123-abc1234", false),
    "os/releases/r000123-abc1234/manifest.json",
  );
  assert.equal(
    releaseManifestKey("r000123-abc1234", true),
    "os/candidates/r000123-abc1234/manifest.json",
  );
  assert.throws(() => releaseManifestKey("../escape", false), /invalid release ID/);
});

test("an identical manifest is an idempotent publish", () => {
  const manifest = new TextEncoder().encode('{"releaseId":"r1-abc"}\n');
  assert.doesNotThrow(() => assertImmutableManifest(manifest, manifest, "release manifest"));
});

test("a release ID cannot be overwritten with different content", () => {
  const existing = new TextEncoder().encode('{"releaseId":"r1-abc","commit":"old"}\n');
  const desired = new TextEncoder().encode('{"releaseId":"r1-abc","commit":"new"}\n');
  assert.throws(
    () => assertImmutableManifest(existing, desired, "os/releases/r1-abc/manifest.json"),
    /immutable release already exists with different content/,
  );
});
