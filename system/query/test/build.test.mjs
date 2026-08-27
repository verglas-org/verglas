import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { buildProject } from '@verglas/worker-js/build';

const root = resolve(new URL('..', import.meta.url).pathname);

test('Query builds as one Worker and Durable Object component', async (t) => {
  const output = await mkdtemp(join(tmpdir(), 'verglas-query-component-'));
  t.after(() => rm(output, { recursive: true, force: true }));
  const result = await buildProject(root, output);
  const manifest = JSON.parse(await readFile(result.manifestPath, 'utf8'));
  assert.equal(manifest.name, 'verglas-query');
  assert.deepEqual(manifest.durable_objects.bindings, [{ name: 'QUERY_DO', class_name: 'Query' }]);
  assert.equal(manifest.vars.QUERY_NAME, 'analytics');
  assert.match(result.componentDigest, /^[a-f0-9]{64}$/u);
});
