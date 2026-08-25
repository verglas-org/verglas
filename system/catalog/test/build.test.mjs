import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile, mkdtemp, rm } from 'node:fs/promises';
import { spawnSync } from 'node:child_process';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';

import { buildProject } from '../../../sdks/worker-js/bin/build.mjs';

const root = resolve(new URL('..', import.meta.url).pathname);
const sdk = resolve(root, '../../sdks/worker-js');
const jco = join(sdk, 'node_modules/.bin/jco');

test('Catalog builds as the Worker/DO service world without WASI', async (t) => {
  const output = await mkdtemp(join(tmpdir(), 'verglas-catalog-component-'));
  t.after(() => rm(output, { recursive: true, force: true }));
  const result = await buildProject(root, output);
  const manifest = JSON.parse(await readFile(result.manifestPath, 'utf8'));
  assert.equal(manifest.name, 'verglas-catalog');
  assert.deepEqual(manifest.durable_objects.bindings, [
    { name: 'CATALOG_DO', class_name: 'Catalog' },
  ]);
  assert.deepEqual(manifest.services, [
    { binding: 'ICEBERG_COMMIT', service: 'verglas-runtime' },
  ]);
  assert.equal(manifest.vars.CATALOG_AUTHORITY_BINDING, undefined);
  assert.equal(manifest.vars.CATALOG_WAREHOUSE, 'warehouse');
  assert.equal(manifest.vars.CATALOG_SINK_ID, 'primary_sink');

  const wit = spawnSync(jco, ['wit', result.componentPath], { encoding: 'utf8' });
  assert.equal(wit.status, 0, wit.stderr);
  assert.match(wit.stdout, /verglas:do-worker\/storage@0\.1\.0/);
  assert.match(wit.stdout, /verglas:do-worker\/bindings@0\.1\.0/);
  assert.match(wit.stdout, /verglas:do-worker\/handler@0\.1\.0/);
  assert.doesNotMatch(wit.stdout, /wasi:/u);
});
