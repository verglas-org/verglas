import test from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';

import { workerAssetPath } from '@verglas/worker-js/assets';
import { createHandler } from '@verglas/worker-js/cloudflare-workers';
import { build as bundle } from 'esbuild';

const encoder = new TextEncoder();
const decoder = new TextDecoder();
const root = resolve(new URL('..', import.meta.url).pathname);

async function loadProject() {
  const result = await bundle({
    entryPoints: [join(root, 'worker.js')], bundle: true, format: 'esm', platform: 'node', write: false,
    alias: { 'cloudflare:workers': workerAssetPath('cloudflare-workers.js') },
  });
  const directory = await mkdtemp(join(tmpdir(), 'verglas-query-bundle-'));
  const path = join(directory, 'worker.mjs');
  await writeFile(path, result.outputFiles[0].text, 'utf8');
  return { directory, project: await import(`${pathToFileURL(path).href}?${Math.random()}`) };
}

class QueryHost {
  constructor(path) { this.database = new DatabaseSync(path); }
  sqlRows(statement) {
    const query = this.database.prepare(statement);
    if (/^\s*(CREATE|INSERT|UPDATE|DELETE|REPLACE|DROP)\b/iu.test(statement)) {
      query.run();
      return '[]';
    }
    return JSON.stringify(query.all());
  }
  close() { this.database.close(); }
}

const definition = {
  sources: [{ name: 'orders' }],
  views: [{
    name: 'daily_sales', source: 'orders',
    dimensions: [{ name: 'day', field: 'day' }, { name: 'region', field: 'region' }],
    measures: [
      { name: 'revenue', op: 'sum', field: 'amount' },
      { name: 'orders', op: 'count' },
      { name: 'minimum', op: 'min', field: 'amount' },
      { name: 'maximum', op: 'max', field: 'amount' },
    ],
  }],
  endpoints: [{
    name: 'sales_by_day', view: 'daily_sales',
    params: [{ name: 'region', type: 'string', dimension: 'region', required: false }],
    order_by: [{ field: 'day', direction: 'asc' }], limit: 100,
  }],
};

function event(path, body) {
  return {
    method: 'POST', uri: `https://verglas.internal${path}`,
    headers: [['content-type', 'application/json']], body: encoder.encode(JSON.stringify(body)), ws: undefined,
  };
}

async function json(response) { return JSON.parse(decoder.decode(response.body)); }

function batch(records, overrides = {}) {
  const value = {
    pipeline_id: 'orders-pipeline', sql_digest: 'a'.repeat(64), source: 'orders', sink: 'analytics',
    first_sequence: 1, last_sequence: 3, records, ...overrides,
  };
  return { ...value, batch_id: JSON.stringify([
    value.pipeline_id, value.sql_digest, value.first_sequence, value.last_sequence, value.sink,
  ]) };
}

async function fixture(t) {
  const directory = await mkdtemp(join(tmpdir(), 'verglas-query-'));
  const loaded = await loadProject();
  const host = new QueryHost(join(directory, 'query.sqlite'));
  const manifest = {
    bindings: [{ name: 'QUERY_DO', class_name: 'Query' }],
    vars: { QUERY_NAME: 'analytics', QUERY_DEFINITION: definition },
  };
  const handler = createHandler(loaded.project, manifest, { transport: host });
  await handler.init();
  t.after(() => {
    host.close();
    return Promise.all([rm(directory, { recursive: true, force: true }), rm(loaded.directory, { recursive: true, force: true })]);
  });
  return { handler, host, manifest, project: loaded.project };
}

test('Query directly materializes Pipeline batches and exact replay is stable', async (t) => {
  const { handler, host } = await fixture(t);
  const payload = batch([
    { day: '2026-08-25', region: 'west', amount: 10 },
    { day: '2026-08-25', region: 'west', amount: 15 },
    { day: '2026-08-26', region: 'east', amount: 7 },
  ]);
  const first = await json(await handler.fetch(event('/sink/batch', payload)));
  assert.deepEqual(await json(await handler.fetch(event('/sink/batch', payload))), first);
  assert.equal(host.database.prepare('SELECT COUNT(*) AS count FROM query_batch_receipts').get().count, 1);

  const result = await json(await handler.fetch(event('/query/run', {
    endpoint: 'sales_by_day', params: { region: 'west' },
  })));
  assert.deepEqual(result.rows, [{
    day: '2026-08-25', region: 'west', revenue: 25, orders: 2, minimum: 10, maximum: 15,
  }]);
  assert.deepEqual(result.watermarks, { orders: 3 });
});

test('Query rejects changed replay and invalid batches without partial state', async (t) => {
  const { handler, host } = await fixture(t);
  const invalid = await handler.fetch(event('/sink/batch', batch([
    { day: '2026-08-25', region: 'west', amount: 10 },
    { day: '2026-08-25', region: 'west', amount: 'wrong' },
  ])));
  assert.equal(invalid.status, 400);
  assert.equal(host.database.prepare('SELECT COUNT(*) AS count FROM query_view_rows').get().count, 0);
  assert.equal(host.database.prepare('SELECT COUNT(*) AS count FROM query_batch_receipts').get().count, 0);

  const valid = batch([{ day: '2026-08-25', region: 'west', amount: 10 }], { last_sequence: 1 });
  await handler.fetch(event('/sink/batch', valid));
  const changed = await handler.fetch(event('/sink/batch', { ...valid, records: [{ day: '2026-08-25', region: 'west', amount: 11 }] }));
  assert.equal(changed.status, 409);
});

test('Query endpoint parameters are typed and configuration is immutable across restart', async (t) => {
  const { handler, host, manifest, project } = await fixture(t);
  await handler.fetch(event('/sink/batch', batch([{ day: '2026-08-25', region: 'west', amount: 10 }], { last_sequence: 1 })));
  const wrongType = await handler.fetch(event('/query/run', { endpoint: 'sales_by_day', params: { region: 3 } }));
  assert.equal(wrongType.status, 400);
  const unknown = await handler.fetch(event('/query/run', { endpoint: 'sales_by_day', params: { surprise: 'x' } }));
  assert.equal(unknown.status, 400);

  const restarted = createHandler(project, manifest, { transport: host });
  await restarted.init();
  assert.equal((await json(await restarted.fetch(event('/query/describe', {})))).name, 'analytics');
  const changed = createHandler(project, {
    ...manifest, vars: { ...manifest.vars, QUERY_NAME: 'changed' },
  }, { transport: host });
  await assert.rejects(changed.init(), /configuration.*immutable/i);
});
