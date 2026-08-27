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
    entryPoints: [join(root, 'worker.js')],
    bundle: true,
    format: 'esm',
    platform: 'node',
    write: false,
    alias: { 'cloudflare:workers': workerAssetPath('cloudflare-workers.js') },
  });
  const directory = await mkdtemp(join(tmpdir(), 'verglas-vectorize-bundle-'));
  const path = join(directory, 'worker.mjs');
  await writeFile(path, result.outputFiles[0].text, 'utf8');
  return { directory, project: await import(`${pathToFileURL(path).href}?${Math.random()}`) };
}

class VectorHost {
  constructor(path) {
    this.database = new DatabaseSync(path);
    this.database.function('vector32', { deterministic: true }, (json) => JSON.stringify(JSON.parse(json).map(Math.fround)));
    this.database.function('vector_extract', { deterministic: true }, (json) => json);
    this.database.function('vector_distance_cos', { deterministic: true }, (left, right) => distance('cosine', left, right));
    this.database.function('vector_distance_l2', { deterministic: true }, (left, right) => distance('euclidean', left, right));
    this.database.function('vector_distance_dot', { deterministic: true }, (left, right) => distance('dot-product', left, right));
  }

  sqlRows(statement) {
    const query = this.database.prepare(statement);
    if (/^\s*(CREATE|INSERT|UPDATE|DELETE|REPLACE|DROP)\b/iu.test(statement)) {
      query.run();
      return '[]';
    }
    return JSON.stringify(query.all());
  }

  close() {
    this.database.close();
  }
}

function distance(metric, left, right) {
  const a = JSON.parse(left);
  const b = JSON.parse(right);
  if (metric === 'euclidean') return Math.sqrt(a.reduce((sum, value, index) => sum + (value - b[index]) ** 2, 0));
  const dot = a.reduce((sum, value, index) => sum + value * b[index], 0);
  if (metric === 'dot-product') return -dot;
  const leftNorm = Math.sqrt(a.reduce((sum, value) => sum + value ** 2, 0));
  const rightNorm = Math.sqrt(b.reduce((sum, value) => sum + value ** 2, 0));
  return 1 - dot / (leftNorm * rightNorm);
}

function request(path, body = {}) {
  return {
    method: 'POST',
    uri: `https://verglas.internal${path}`,
    headers: [['content-type', 'application/json']],
    body: encoder.encode(JSON.stringify(body)),
    ws: undefined,
  };
}

async function json(response) {
  return JSON.parse(decoder.decode(response.body));
}

async function fixture(t, vars = {}) {
  const directory = await mkdtemp(join(tmpdir(), 'verglas-vectorize-'));
  const loaded = await loadProject();
  const host = new VectorHost(join(directory, 'vectorize.sqlite'));
  const handler = createHandler(loaded.project, {
    bindings: [{ name: 'VECTORIZE_DO', class_name: 'Vectorize' }],
    vars: {
      VECTORIZE_INDEX_NAME: 'documents',
      VECTORIZE_DIMENSIONS: 3,
      VECTORIZE_METRIC: 'cosine',
      ...vars,
    },
  }, { transport: host });
  await handler.init();
  t.after(() => {
    host.close();
    return Promise.all([
      rm(directory, { recursive: true, force: true }),
      rm(loaded.directory, { recursive: true, force: true }),
    ]);
  });
  return { handler, host, project: loaded.project };
}

test('insert, upsert, get, delete, query, and queryById match Vectorize semantics', async (t) => {
  const { handler } = await fixture(t);
  const insertBody = { vectors: [
    { id: 'a', values: [1, 0, 0], namespace: 'tenant-a', metadata: { kind: 'doc', rank: 1 } },
    { id: 'b', values: [0, 1, 0], namespace: 'tenant-a', metadata: { kind: 'note', rank: 2 } },
  ] };
  const first = await json(await handler.fetch(request('/vectorize/insert', insertBody)));
  const retry = await json(await handler.fetch(request('/vectorize/insert', insertBody)));
  assert.equal(first.mutationId, retry.mutationId);

  const ignored = await handler.fetch(request('/vectorize/insert', {
    vectors: [{ id: 'a', values: [0, 0, 1] }],
  }));
  assert.equal(ignored.status, 200);
  assert.deepEqual(await json(await handler.fetch(request('/vectorize/get-by-ids', { ids: ['a'] }))), [
    { id: 'a', values: [1, 0, 0], namespace: 'tenant-a', metadata: { kind: 'doc', rank: 1 } },
  ]);

  const queried = await json(await handler.fetch(request('/vectorize/query', {
    vector: [1, 0, 0], topK: 2, returnValues: true, returnMetadata: 'all', namespace: 'tenant-a',
  })));
  assert.deepEqual(queried.matches.map(({ id }) => id), ['a', 'b']);
  assert.ok(queried.matches[0].score > queried.matches[1].score);
  assert.deepEqual(queried.matches[0].values, [1, 0, 0]);

  const byId = await json(await handler.fetch(request('/vectorize/query-by-id', { id: 'a', topK: 1 })));
  assert.equal(byId.matches[0].id, 'a');

  await handler.fetch(request('/vectorize/upsert', {
    vectors: [{ id: 'a', values: [0, 0, 1], metadata: { replaced: true } }],
  }));
  assert.deepEqual(await json(await handler.fetch(request('/vectorize/get-by-ids', { ids: ['a'] }))), [
    { id: 'a', values: [0, 0, 1], metadata: { replaced: true } },
  ]);
  await handler.fetch(request('/vectorize/delete-by-ids', { ids: ['a'] }));
  assert.deepEqual(await json(await handler.fetch(request('/vectorize/get-by-ids', { ids: ['a'] }))), []);
});

test('metadata indexes gate filters and every comparison operator executes before topK', async (t) => {
  const { handler } = await fixture(t);
  await handler.fetch(request('/vectorize/insert', { vectors: [
    { id: 'a', values: [1, 0, 0], metadata: { kind: 'doc', rank: 1, active: true } },
    { id: 'b', values: [0.9, 0.1, 0], metadata: { kind: 'note', rank: 2, active: false } },
    { id: 'c', values: [0.8, 0.2, 0], metadata: { kind: 'doc', rank: 3, active: true } },
  ] }));
  const rejected = await handler.fetch(request('/vectorize/query', {
    vector: [1, 0, 0], filter: { kind: 'doc' },
  }));
  assert.equal(rejected.status, 400);
  for (const [propertyName, indexType] of [['kind', 'string'], ['rank', 'number'], ['active', 'boolean']]) {
    assert.equal((await handler.fetch(request('/vectorize/metadata-index/create', { propertyName, indexType }))).status, 200);
  }
  const filters = [
    [{ kind: { $eq: 'doc' } }, ['a', 'c']],
    [{ kind: { $ne: 'doc' } }, ['b']],
    [{ kind: { $in: ['note'] } }, ['b']],
    [{ kind: { $nin: ['note'] } }, ['a', 'c']],
    [{ rank: { $lt: 2 } }, ['a']],
    [{ rank: { $lte: 2 } }, ['a', 'b']],
    [{ rank: { $gt: 2 } }, ['c']],
    [{ rank: { $gte: 2 } }, ['b', 'c']],
  ];
  for (const [filter, ids] of filters) {
    const result = await json(await handler.fetch(request('/vectorize/query', {
      vector: [1, 0, 0], topK: 10, filter,
    })));
    assert.deepEqual(result.matches.map(({ id }) => id), ids);
  }
  const injected = await json(await handler.fetch(request('/vectorize/query', {
    vector: [1, 0, 0], topK: 10, filter: { kind: { $eq: "doc' OR 1=1 --" } },
  })));
  assert.deepEqual(injected.matches, []);
  const list = await json(await handler.fetch(request('/vectorize/metadata-index/list')));
  assert.equal(list.metadataIndexes.length, 3);
});

test('euclidean and dot-product use Cloudflare score ordering', async (t) => {
  for (const metric of ['euclidean', 'dot-product']) {
    const { handler } = await fixture(t, { VECTORIZE_METRIC: metric });
    await handler.fetch(request('/vectorize/insert', { vectors: [
      { id: 'near', values: [1, 0, 0] },
      { id: 'far', values: [0, 2, 0] },
    ] }));
    const result = await json(await handler.fetch(request('/vectorize/query', {
      vector: [1, 0, 0], topK: 2,
    })));
    assert.deepEqual(result.matches.map(({ id }) => id), ['near', 'far']);
    assert.ok(result.matches[0].score < result.matches[1].score, metric);
  }
});

test('query fails instead of returning partial matches above the exact-scan ceiling', async (t) => {
  const { handler, host } = await fixture(t);
  host.database.exec(`WITH RECURSIVE sequence(value) AS (
    SELECT 1 UNION ALL SELECT value + 1 FROM sequence WHERE value <= 10000
  ) INSERT INTO vectorize_vectors (external_id, embedding, mutation_id)
    SELECT 'id-' || value, vector32('[1,0,0]'), 'seed' FROM sequence`);
  const response = await handler.fetch(request('/vectorize/query', { vector: [1, 0, 0] }));
  assert.equal(response.status, 400);
  assert.match((await json(response)).error, /exact-search ceiling/i);
});

test('configuration is immutable and invalid batches make no partial writes', async (t) => {
  const { handler, host, project } = await fixture(t);
  const invalid = await handler.fetch(request('/vectorize/upsert', { vectors: [
    { id: 'valid', values: [1, 0, 0] },
    { id: 'wrong', values: [1, 0] },
  ] }));
  assert.equal(invalid.status, 400);
  assert.equal(host.database.prepare('SELECT COUNT(*) AS count FROM vectorize_vectors').get().count, 0);

  const describe = await json(await handler.fetch(request('/vectorize/describe')));
  assert.equal(describe.dimensions, 3);
  assert.equal(describe.metric, 'cosine');

  await handler.fetch(request('/vectorize/metadata-index/create', {
    propertyName: 'kind', indexType: 'string',
  }));
  await handler.fetch(request('/vectorize/upsert', {
    vectors: [{ id: 'persisted', values: [1, 0, 0], metadata: { kind: 'doc' } }],
  }));
  const restarted = createHandler(project, {
    bindings: [{ name: 'VECTORIZE_DO', class_name: 'Vectorize' }],
    vars: { VECTORIZE_INDEX_NAME: 'documents', VECTORIZE_DIMENSIONS: 3, VECTORIZE_METRIC: 'cosine' },
  }, { transport: host });
  await restarted.init();
  const afterRestart = await json(await restarted.fetch(request('/vectorize/query', {
    vector: [1, 0, 0], filter: { kind: 'doc' }, returnMetadata: 'indexed',
  })));
  assert.equal(afterRestart.matches[0].id, 'persisted');
  assert.deepEqual(afterRestart.matches[0].metadata, { kind: 'doc' });
  assert.deepEqual(
    (await json(await restarted.fetch(request('/vectorize/metadata-index/list')))).metadataIndexes,
    [{ propertyName: 'kind', indexType: 'string' }],
  );

  const other = createHandler(project, {
    bindings: [{ name: 'VECTORIZE_DO', class_name: 'Vectorize' }],
    vars: { VECTORIZE_INDEX_NAME: 'documents', VECTORIZE_DIMENSIONS: 4, VECTORIZE_METRIC: 'cosine' },
  }, { transport: host });
  await assert.rejects(other.init(), /configuration.*immutable/i);
});
