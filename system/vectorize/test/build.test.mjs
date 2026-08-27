import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';

import { buildProject } from '@verglas/worker-js/build';

const root = resolve(new URL('..', import.meta.url).pathname);

test('Vectorize builds as one Worker and Durable Object component', async (t) => {
  const output = await mkdtemp(join(tmpdir(), 'verglas-vectorize-component-'));
  t.after(() => rm(output, { recursive: true, force: true }));
  const result = await buildProject(root, output);
  const manifest = JSON.parse(await readFile(result.manifestPath, 'utf8'));
  assert.equal(manifest.name, 'verglas-vectorize');
  assert.deepEqual(manifest.durable_objects.bindings, [
    { name: 'VECTORIZE_DO', class_name: 'Vectorize' },
  ]);
  assert.equal(manifest.vars.VECTORIZE_INDEX_NAME, 'documents');
  assert.equal(manifest.vars.VECTORIZE_DIMENSIONS, 3);
  assert.equal(manifest.vars.VECTORIZE_METRIC, 'cosine');
  assert.equal(typeof result.componentDigest, 'string');
  assert.equal(result.componentDigest.length, 64);
});
