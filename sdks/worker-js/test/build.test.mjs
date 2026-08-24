import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

import { buildProject } from '../bin/build.mjs';

const packageDir = new URL('..', import.meta.url);
const jcoPath = new URL('./node_modules/.bin/jco', packageDir);

async function makeProject() {
  const directory = await mkdtemp(join(tmpdir(), 'verglas-worker-js-test-'));
  await writeFile(
    join(directory, 'wrangler.jsonc'),
    `{
      // The parser must accept the wrangler JSONC subset.
      "name": "test-worker",
      "main": "worker.js",
      "durable_objects": {
        "bindings": [{ "name": "COUNTER", "class_name": "Counter" }],
      },
    }\n`,
  );
  await writeFile(
    join(directory, 'worker.js'),
    `export default {
      fetch() { return { status: 200, headers: { "content-type": "text/plain" }, body: "ok" }; },
    };\n`,
  );
  return directory;
}

test('build output is valid and records digest determinism', async (t) => {
  const project = await makeProject();
  const outputOne = await mkdtemp(join(tmpdir(), 'verglas-worker-js-out-'));
  const outputTwo = await mkdtemp(join(tmpdir(), 'verglas-worker-js-out-'));
  t.after(async () => {
    await Promise.all([
      rm(project, { recursive: true, force: true }),
      rm(outputOne, { recursive: true, force: true }),
      rm(outputTwo, { recursive: true, force: true }),
    ]);
  });
  const first = await buildProject(project, outputOne);
  const second = await buildProject(project, outputTwo);

  assert.match(first.componentDigest, /^[0-9a-f]{64}$/);
  assert.match(second.componentDigest, /^[0-9a-f]{64}$/);
  assert.equal(createHash('sha256').update(first.componentBytes).digest('hex'), first.componentDigest);
  assert.equal(createHash('sha256').update(second.componentBytes).digest('hex'), second.componentDigest);
  assert.ok(first.componentBytes.byteLength > 0);
  assert.ok(second.componentBytes.byteLength > 0);
  if (first.componentDigest === second.componentDigest) {
    t.diagnostic('componentize output is deterministic for unchanged source');
  } else {
    t.diagnostic(`componentize output is nondeterministic: ${first.componentDigest} != ${second.componentDigest}`);
  }
  assert.deepEqual(JSON.parse(await readFile(first.manifestPath, 'utf8')), {
    name: 'test-worker',
    component_digest: first.componentDigest,
    bindings: [{ name: 'COUNTER', class_name: 'Counter' }],
  });

  const wit = spawnSync(fileURLToPath(jcoPath), ['wit', first.componentPath], { encoding: 'utf8' });
  assert.equal(wit.status, 0, wit.stderr);
  assert.match(wit.stdout, /verglas:do-worker\/storage@0\.1\.0/);
  assert.match(wit.stdout, /verglas:do-worker\/sockets@0\.1\.0/);
  assert.match(wit.stdout, /verglas:do-worker\/handler@0\.1\.0/);
  assert.doesNotMatch(wit.stdout, /wasi:/);

  t.diagnostic(`component bytes: ${first.componentBytes.byteLength}`);
  t.diagnostic(`component digest: ${first.componentDigest}`);
});
