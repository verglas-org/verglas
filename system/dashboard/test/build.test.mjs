import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { buildProject } from '@verglas/worker-js/build';
import { workerAssetPath } from '@verglas/worker-js/assets';
import { build as bundle } from 'esbuild';

const root = resolve(new URL('..', import.meta.url).pathname);

test('Dashboard builds as a stateless Worker with Query bindings', async (t) => {
  const output = await mkdtemp(join(tmpdir(), 'verglas-dashboard-component-'));
  t.after(() => rm(output, { recursive: true, force: true }));
  const result = await buildProject(root, output);
  const manifest = JSON.parse(await readFile(result.manifestPath, 'utf8'));
  assert.equal(manifest.name, 'verglas-dashboard');
  assert.deepEqual(manifest.durable_objects.bindings, []);
  assert.deepEqual(manifest.queries, [{ binding: 'QUERY', query_name: 'analytics' }]);
  assert.deepEqual(Object.keys(manifest.artifacts), ['worker']);
});

test('Dashboard Worker renders its declared Query as escaped semantic HTML', async (t) => {
  const output = await mkdtemp(join(tmpdir(), 'verglas-dashboard-bundle-'));
  t.after(() => rm(output, { recursive: true, force: true }));
  const result = await bundle({
    entryPoints: [join(root, 'worker.jsx')],
    bundle: true,
    format: 'esm',
    platform: 'node',
    write: false,
    alias: { 'cloudflare:workers': workerAssetPath('cloudflare-workers.js') },
  });
  const modulePath = join(output, 'dashboard.mjs');
  await writeFile(modulePath, result.outputFiles[0].text, 'utf8');
  const project = await import(`${pathToFileURL(modulePath).href}?${Math.random()}`);
  const calls = [];
  const response = await project.default.fetch(new Request('https://dashboard.test/'), {
    QUERY: {
      async query(endpoint, params) {
        calls.push({ endpoint, params });
        return { rows: [{ day: '<Monday>', revenue: 42 }] };
      },
    },
  });
  const html = await response.text();
  assert.equal(response.status, 200);
  assert.match(response.headers.get('content-security-policy'), /default-src 'none'/u);
  assert.match(html, /Analytics/u);
  assert.match(html, /&lt;Monday&gt;/u);
  assert.doesNotMatch(html, /<Monday>/u);
  assert.match(html, /<svg/u);
  assert.deepEqual(calls, [
    { endpoint: 'sales_by_day', params: {} },
    { endpoint: 'sales_by_day', params: {} },
    { endpoint: 'sales_by_day', params: {} },
  ]);
});
