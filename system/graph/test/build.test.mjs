import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { buildProject } from '@verglas/worker-js/build';

const root = resolve(new URL('..', import.meta.url).pathname);

test('Graph builds as one Worker and Durable Object component', async (t) => {
  const output = await mkdtemp(join(tmpdir(), 'verglas-graph-component-'));
  t.after(() => rm(output, { recursive: true, force: true }));
  const result = await buildProject(root, output);
  const manifest = JSON.parse(await readFile(result.manifestPath, 'utf8'));
  assert.equal(manifest.name, 'verglas-graph');
  assert.deepEqual(manifest.durable_objects.bindings, [{ name: 'GRAPH_DO', class_name: 'Graph' }]);
  assert.equal(manifest.vars.GRAPH_NAME, 'knowledge');
  assert.match(result.componentDigest, /^[a-f0-9]{64}$/u);
});
