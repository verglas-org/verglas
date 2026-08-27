import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile, mkdtemp, rm } from 'node:fs/promises';
import { spawnSync } from 'node:child_process';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';

import { buildProject } from '@verglas/worker-js/build';
import { workerToolPath } from '@verglas/worker-js/assets';

const root = resolve(new URL('..', import.meta.url).pathname);
const jco = workerToolPath('jco');

test('Pipeline builds as the Worker/DO service world without WASI', async (t) => {
  const output = await mkdtemp(join(tmpdir(), 'verglas-pipeline-component-'));
  t.after(() => rm(output, { recursive: true, force: true }));
  const result = await buildProject(root, output);
  const manifest = JSON.parse(await readFile(result.manifestPath, 'utf8'));
  assert.equal(manifest.name, 'verglas-pipeline');
  assert.deepEqual(manifest.durable_objects.bindings, [
    { name: 'PIPELINE_DO', class_name: 'Pipeline' },
    { name: 'STREAM', class_name: 'Pipeline' },
    { name: 'SINK_A', class_name: 'Pipeline' },
    { name: 'SINK_B', class_name: 'Pipeline' },
  ]);
  assert.equal(manifest.vars.PIPELINE_SOURCE_BINDING, 'STREAM');
  assert.equal(manifest.vars.PIPELINE_BATCH_MAX_ROWS, 1000);

  const wit = spawnSync(jco, ['wit', result.componentPath], { encoding: 'utf8' });
  assert.equal(wit.status, 0, wit.stderr);
  assert.match(wit.stdout, /verglas:do-worker\/storage@0\.1\.0/);
  assert.match(wit.stdout, /verglas:do-worker\/bindings@0\.1\.0/);
  assert.match(wit.stdout, /verglas:do-worker\/handler@0\.1\.0/);
  assert.doesNotMatch(wit.stdout, /wasi:/);
});
