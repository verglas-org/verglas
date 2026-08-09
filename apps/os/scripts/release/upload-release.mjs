#!/usr/bin/env node

// Mirrors a built release directory (see build-release.mjs) to R2 via the S3 API.
//
// Blobs are content-addressed, so unchanged files dedupe across releases: each key is HEAD'd
// first and skipped if present. The manifest is uploaded LAST — its presence under
// os/releases/<id>/manifest.json is what marks a release complete, so a crashed upload never
// leaves a manifest pointing at missing blobs. All keys live below `os/` so this
// publisher can share the platform release bucket without colliding with fleet artifacts.
//
// With --candidate the manifest lands under os/candidates/<id>/manifest.json instead — invisible
// to the deploy service (which scans only os/releases/) until promote-release.mjs copies it over
// after the e2e gate passes. Blob handling is identical either way.
//
// Env: R2_ENDPOINT (https://<account>.r2.cloudflarestorage.com), R2_BUCKET,
//      R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY
// Usage: node scripts/release/upload-release.mjs --release <dir> [--candidate]

import { readFileSync, readdirSync } from "node:fs";
import { join, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { AwsClient } from "aws4fetch";
import { assetR2Key, moduleR2Key } from "./manifest-lib.mjs";

const UPLOAD_CONCURRENCY = 8;

function requireEnv(name) {
  const value = process.env[name];
  if (!value) throw new Error(`missing required environment variable: ${name}`);
  return value;
}

function parseArgs(argv) {
  const args = { release: undefined, candidate: false };
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === "--release") args.release = resolve(argv[++i]);
    else if (argv[i] === "--candidate") args.candidate = true;
    else throw new Error(`unknown argument: ${argv[i]}`);
  }
  if (!args.release) throw new Error("--release <dir> is required");
  return args;
}

/** Returns the immutable manifest key for a candidate or published OS release. */
export function releaseManifestKey(releaseId, candidate) {
  if (!/^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/.test(releaseId)) {
    throw new Error(`invalid release ID: ${releaseId}`);
  }
  return `os/${candidate ? "candidates" : "releases"}/${releaseId}/manifest.json`;
}

/** Rejects attempts to reuse a release key for different manifest bytes. */
export function assertImmutableManifest(existing, desired, key) {
  if (Buffer.from(existing).equals(Buffer.from(desired))) return;
  throw new Error(`immutable release already exists with different content: ${key}`);
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const endpoint = requireEnv("R2_ENDPOINT").replace(/\/$/, "");
  const bucket = requireEnv("R2_BUCKET");
  const client = new AwsClient({
    accessKeyId: requireEnv("R2_ACCESS_KEY_ID"),
    secretAccessKey: requireEnv("R2_SECRET_ACCESS_KEY"),
    service: "s3",
    region: "auto",
  });
  const keyUrl = (key) => `${endpoint}/${bucket}/${key}`;

  const manifest = JSON.parse(readFileSync(join(args.release, "manifest.json"), "utf8"));

  const blobs = [
    ...readdirSync(join(args.release, "modules")).map((sha256) => ({
      key: moduleR2Key(sha256),
      path: join(args.release, "modules", sha256),
    })),
    ...readdirSync(join(args.release, "assets")).map((hash) => ({
      key: assetR2Key(hash),
      path: join(args.release, "assets", hash),
    })),
  ];

  let uploaded = 0;
  let skipped = 0;
  const queue = [...blobs];
  async function worker() {
    for (;;) {
      const blob = queue.shift();
      if (!blob) return;
      const head = await client.fetch(keyUrl(blob.key), { method: "HEAD" });
      if (head.status === 200) {
        skipped++;
        continue;
      }
      if (head.status !== 404) {
        throw new Error(`HEAD ${blob.key}: unexpected status ${head.status}`);
      }
      const put = await client.fetch(keyUrl(blob.key), {
        method: "PUT",
        body: readFileSync(blob.path),
      });
      if (!put.ok) {
        throw new Error(`PUT ${blob.key}: ${put.status} ${await put.text()}`);
      }
      uploaded++;
    }
  }
  await Promise.all(Array.from({ length: UPLOAD_CONCURRENCY }, worker));
  console.log(`blobs: ${uploaded} uploaded, ${skipped} already present`);

  const manifestKey = releaseManifestKey(manifest.releaseId, args.candidate);
  const manifestBody = readFileSync(join(args.release, "manifest.json"));
  const existing = await client.fetch(keyUrl(manifestKey), { method: "GET" });
  if (existing.ok) {
    assertImmutableManifest(
      new Uint8Array(await existing.arrayBuffer()),
      manifestBody,
      manifestKey,
    );
    console.log(`release already present: ${manifestKey}`);
    return;
  }
  if (existing.status !== 404) {
    throw new Error(`GET ${manifestKey}: unexpected status ${existing.status}`);
  }
  const put = await client.fetch(keyUrl(manifestKey), {
    method: "PUT",
    body: manifestBody,
    headers: {
      "Content-Type": "application/json",
      "If-None-Match": "*",
    },
  });
  if (put.status === 409 || put.status === 412) {
    const raced = await client.fetch(keyUrl(manifestKey), { method: "GET" });
    if (!raced.ok) {
      throw new Error(`GET ${manifestKey} after publish race: ${raced.status}`);
    }
    assertImmutableManifest(
      new Uint8Array(await raced.arrayBuffer()),
      manifestBody,
      manifestKey,
    );
    console.log(`release already present: ${manifestKey}`);
    return;
  }
  if (!put.ok) {
    throw new Error(`PUT ${manifestKey}: ${put.status} ${await put.text()}`);
  }
  console.log(args.candidate
    ? `candidate uploaded (not yet visible to the deploy service): ${manifestKey}`
    : `release complete: ${manifestKey}`);
}

if (import.meta.url === pathToFileURL(process.argv[1]).href) {
  await main();
}
