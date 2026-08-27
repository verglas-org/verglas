import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile, mkdtemp, rm } from 'node:fs/promises';
import { spawnSync } from 'node:child_process';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

import { buildProject } from '@verglas/worker-js/build';
import { workerToolPath } from '@verglas/worker-js/assets';

const root = resolve(new URL('..', import.meta.url).pathname);
const jco = workerToolPath('jco');

test('Stream builds against the service world with only Worker capabilities', async (t) => {
  const output = await mkdtemp(join(tmpdir(), 'verglas-stream-component-'));
  t.after(() => rm(output, { recursive: true, force: true }));
  const result = await buildProject(root, output);
  const manifest = JSON.parse(await readFile(result.manifestPath, 'utf8'));
  assert.equal(manifest.name, 'verglas-stream');
  assert.deepEqual(manifest.durable_objects.bindings, [{ name: 'STREAM_DO', class_name: 'Stream' }]);
  assert.deepEqual(manifest.vars, {
    STREAM_NAME: 'main',
    STREAM_SCHEMA: {
      fields: [
        { name: 'kind', type: 'string', required: true },
        { name: 'payload', type: 'json', required: false },
      ],
    },
  });

  const wit = spawnSync(jco, ['wit', result.componentPath], { encoding: 'utf8' });
  assert.equal(wit.status, 0, wit.stderr);
  assert.match(wit.stdout, /verglas:do-worker\/storage@0\.1\.0/);
  assert.match(wit.stdout, /verglas:do-worker\/sockets@0\.1\.0/);
  assert.match(wit.stdout, /verglas:do-worker\/bindings@0\.1\.0/);
  assert.match(wit.stdout, /verglas:do-worker\/worker@0\.1\.0/);
  assert.match(wit.stdout, /verglas:do-worker\/handler@0\.1\.0/);
  assert.doesNotMatch(wit.stdout, /wasi:/);
});
