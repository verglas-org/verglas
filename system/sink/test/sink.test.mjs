import test from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';

import { build as bundle } from '../../../sdks/worker-js/node_modules/esbuild/lib/main.js';
import { createHandler, createWorker } from '../../../sdks/worker-js/src/cloudflare-workers.js';

const root = resolve(new URL('..', import.meta.url).pathname);
const sinkSource = join(root, 'worker.js');
const cloudflareWorkersPath = resolve(root, '../../sdks/worker-js/src/cloudflare-workers.js');
const encoder = new TextEncoder();
const decoder = new TextDecoder();
const BATCH = {
  batch_id: '["orders","aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",1,2,"primary"]',
  pipeline_id: 'orders',
  sql_digest: 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
  source: 'events',
  sink: 'primary',
  first_sequence: 1,
  last_sequence: 2,
  records: [{ id: 1 }, { id: 2 }],
};

class PersistedHost {
  constructor(path) {
    this.database = new DatabaseSync(path);
    this.catalogCalls = [];
    this.catalogRecords = new Map();
    this.catalogFailure = undefined;
    this.loseCatalogResponse = false;
    this.sinkHandler = undefined;
  }

  sqlRows(statement, ...bindings) {
    const query = this.database.prepare(statement);
    if (/^\s*(CREATE|INSERT|UPDATE|DELETE|REPLACE|BEGIN|COMMIT|ROLLBACK)\b/iu.test(statement)) {
      query.run(...bindings);
      return '[]';
    }
    return JSON.stringify(query.all(...bindings));
  }

  doFetch(binding, object, request) {
    if (binding === 'CATALOG') return this.catalogFetch(request);
    if (binding === 'SINK_DO') {
      if (!this.sinkHandler) throw new Error('Sink handler is not attached');
      return this.sinkHandler.fetch(request);
    }
    throw new Error(`unexpected binding ${binding} object ${object}`);
  }

  catalogFetch(request) {
    const payload = JSON.parse(decoder.decode(request.body));
    this.catalogCalls.push({ request, payload });
    if (this.catalogFailure) return response(this.catalogFailure, { error: 'catalog unavailable' });
    const prior = this.catalogRecords.get(payload.batch_id);
    const receipt = prior ?? {
      committed: true,
      batch_id: payload.batch_id,
      file_id: payload.file_id,
      snapshot_id: '42',
      rows_committed: payload.records.length,
    };
    this.catalogRecords.set(payload.batch_id, receipt);
    if (this.loseCatalogResponse) {
      this.loseCatalogResponse = false;
      throw new Error('catalog committed but response was lost');
    }
    return response(200, receipt);
  }

  close() {
    this.database.close();
  }
}

function response(status, value) {
  const body = typeof value === 'string' ? value : JSON.stringify(value);
  return { status, headers: [['content-type', 'application/json']], body: encoder.encode(body) };
}

function request(method, uri, body = '', headers = []) {
  return {
    method,
    uri,
    headers,
    body: encoder.encode(body),
    ws: undefined,
  };
}

function batchRequest(batch = BATCH, headers = {}) {
  const merged = {
    'content-type': 'application/json',
    'x-verglas-pipeline-id': batch.pipeline_id,
    'x-verglas-sql-digest': batch.sql_digest,
    'x-verglas-batch-id': batch.batch_id,
    ...headers,
  };
  return request('POST', 'https://verglas.internal/sink/batch', JSON.stringify(batch), Object.entries(merged));
}

async function loadProject() {
  const result = await bundle({
    entryPoints: [sinkSource],
    bundle: true,
    format: 'esm',
    platform: 'node',
    write: false,
    alias: { 'cloudflare:workers': cloudflareWorkersPath },
  });
  const directory = await mkdtemp(join(tmpdir(), 'verglas-sink-bundle-'));
  const path = join(directory, 'worker.mjs');
  await writeFile(path, result.outputFiles[0].text, 'utf8');
  const project = await import(`${pathToFileURL(path).href}?${Date.now()}-${Math.random()}`);
  return { directory, project };
}

function manifest(vars = {}) {
  return {
    bindings: [
      { name: 'SINK_DO', class_name: 'Sink' },
      { name: 'CATALOG', class_name: 'Sink' },
    ],
    vars: {
      SINK_ID: 'primary',
      SINK_TYPE: 'iceberg',
      SINK_CATALOG_BINDING: 'CATALOG',
      SINK_CATALOG_OBJECT: 'warehouse',
      SINK_BUCKET: 'lake',
      SINK_NAMESPACE: 'analytics',
      SINK_TABLE: 'events',
      SINK_COMPRESSION: 'zstd',
      SINK_ROLL_INTERVAL_SECONDS: 60,
      SINK_ROLL_SIZE_BYTES: 5 * 1024 * 1024,
      ...vars,
    },
  };
}

async function makeHandler(project, host, vars = {}, objectId) {
  const handler = createHandler(project, manifest(vars), { transport: host, ...(objectId ? { objectId } : {}) });
  await handler.init();
  return handler;
}

async function readJson(result) {
  return JSON.parse(decoder.decode(result.body));
}

async function fixture(t, vars = {}) {
  const loaded = await loadProject();
  const directory = await mkdtemp(join(tmpdir(), 'verglas-sink-state-'));
  const path = join(directory, 'sink.sqlite');
  const host = new PersistedHost(path);
  const handler = await makeHandler(loaded.project, host, vars);
  host.sinkHandler = handler;
  t.after(() => {
    host.close();
    return Promise.all([
      rm(directory, { recursive: true, force: true }),
      rm(loaded.directory, { recursive: true, force: true }),
    ]);
  });
  return { loaded, directory, path, host, handler };
}

test('commits a valid batch through the Catalog binding and returns its receipt', async (t) => {
  const fixtureValue = await fixture(t);
  const result = await fixtureValue.handler.fetch(batchRequest());
  assert.equal(result.status, 200);
  const receipt = await readJson(result);
  assert.deepEqual(receipt, {
    accepted: 2,
    batch_id: BATCH.batch_id,
    file_id: fixtureValue.host.catalogCalls[0].payload.file_id,
    snapshot_id: '42',
  });
  assert.equal(fixtureValue.host.catalogCalls.length, 1);
  assert.deepEqual(fixtureValue.host.catalogCalls[0].payload, {
    batch_id: BATCH.batch_id,
    file_id: receipt.file_id,
    sink_id: 'primary',
    pipeline_id: 'orders',
    sql_digest: BATCH.sql_digest,
    source: 'events',
    first_sequence: 1,
    last_sequence: 2,
    bucket: 'lake',
    namespace: 'analytics',
    table: 'events',
    format: 'parquet',
    compression: 'zstd',
    roll_interval_seconds: 60,
    roll_size_bytes: 5 * 1024 * 1024,
    records: BATCH.records,
  });
  assert.equal(fixtureValue.host.catalogCalls[0].request.uri, 'https://verglas.internal/catalog/commit');
});

test('a confirmed duplicate returns the identical receipt without a Catalog call', async (t) => {
  const fixtureValue = await fixture(t);
  const first = await fixtureValue.handler.fetch(batchRequest());
  const retry = await fixtureValue.handler.fetch(batchRequest());
  assert.equal(first.status, 200);
  assert.equal(retry.status, 200);
  assert.deepEqual(await readJson(retry), await readJson(first));
  assert.equal(fixtureValue.host.catalogCalls.length, 1);
});

test('a lost Catalog response and restart retries the same idempotent request', async (t) => {
  const loaded = await loadProject();
  const directory = await mkdtemp(join(tmpdir(), 'verglas-sink-restart-'));
  const path = join(directory, 'sink.sqlite');
  const firstHost = new PersistedHost(path);
  const first = await makeHandler(loaded.project, firstHost);
  firstHost.loseCatalogResponse = true;
  const lost = await first.fetch(batchRequest());
  assert.equal(lost.status, 502);
  firstHost.close();

  const secondHost = new PersistedHost(path);
  secondHost.catalogRecords = firstHost.catalogRecords;
  const second = await makeHandler(loaded.project, secondHost);
  const retried = await second.fetch(batchRequest());
  assert.equal(retried.status, 200);
  assert.equal(secondHost.catalogCalls.length, 1);
  assert.equal(secondHost.catalogCalls[0].payload.batch_id, BATCH.batch_id);
  assert.equal(secondHost.catalogCalls[0].payload.file_id, JSON.parse(decoder.decode(firstHost.catalogCalls[0]?.request.body ?? encoder.encode('{}'))).file_id);
  t.after(() => {
    secondHost.close();
    return Promise.all([rm(directory, { recursive: true, force: true }), rm(loaded.directory, { recursive: true, force: true })]);
  });
});

test('Catalog failure never records a ledger receipt', async (t) => {
  const fixtureValue = await fixture(t);
  fixtureValue.host.catalogFailure = 503;
  const failed = await fixtureValue.handler.fetch(batchRequest());
  assert.equal(failed.status, 502);
  fixtureValue.host.catalogFailure = undefined;
  const retried = await fixtureValue.handler.fetch(batchRequest());
  assert.equal(retried.status, 200);
  assert.equal(fixtureValue.host.catalogCalls.length, 2);
});

test('configuration is immutable and requires the Iceberg roll minimum', async (t) => {
  const loaded = await loadProject();
  const directory = await mkdtemp(join(tmpdir(), 'verglas-sink-config-'));
  const path = join(directory, 'sink.sqlite');
  const firstHost = new PersistedHost(path);
  await makeHandler(loaded.project, firstHost);
  firstHost.close();
  const changedHost = new PersistedHost(path);
  await assert.rejects(async () => {
    const changed = await makeHandler(loaded.project, changedHost, { SINK_TABLE: 'other' });
    await changed.fetch(request('GET', 'https://verglas.internal/sink/status'));
  }, /immutable|delete and recreate/i);
  changedHost.close();
  const invalidHost = new PersistedHost(join(directory, 'invalid.sqlite'));
  await assert.rejects(makeHandler(loaded.project, invalidHost, { SINK_ROLL_INTERVAL_SECONDS: 59 }), /between 60|roll interval/i);
  invalidHost.close();
  t.after(() => Promise.all([rm(directory, { recursive: true, force: true }), rm(loaded.directory, { recursive: true, force: true })]));
});

test('malformed identity, body, and hard-ceiling requests fail before Catalog', async (t) => {
  const fixtureValue = await fixture(t, { SINK_ROLL_SIZE_BYTES: 1024 });
  const cases = [
    [batchRequest(BATCH, { 'x-verglas-pipeline-id': 'other' }), /pipeline/i],
    [batchRequest(BATCH, { 'x-verglas-sql-digest': 'b'.repeat(64) }), /sql digest/i],
    [batchRequest(BATCH, { 'x-verglas-batch-id': '["wrong"]' }), /batch/i],
    [batchRequest({ ...BATCH, sink: 'other' }), /sink/i],
    [batchRequest({ ...BATCH, first_sequence: 0 }), /sequence|range/i],
    [batchRequest({ ...BATCH, last_sequence: 1 }), /batch|range/i],
    [request('POST', 'https://verglas.internal/sink/batch', '{', Object.entries({
      'content-type': 'application/json',
      'x-verglas-pipeline-id': BATCH.pipeline_id,
      'x-verglas-sql-digest': BATCH.sql_digest,
      'x-verglas-batch-id': BATCH.batch_id,
    })), /JSON|body/i],
  ];
  for (const [badRequest, expected] of cases) {
    const result = await fixtureValue.handler.fetch(badRequest);
    assert.equal(result.status, 400);
    assert.match(decoder.decode(result.body), expected);
  }
  const oversized = { ...BATCH, records: [{ value: 'x'.repeat(8 * 1024 * 1024) }] };
  const oversizedResponse = await fixtureValue.handler.fetch(batchRequest(oversized));
  assert.equal(oversizedResponse.status, 413);
  assert.equal(fixtureValue.host.catalogCalls.length, 0);
});

test('Sink exposes only internal batch and status routes', async (t) => {
  const fixtureValue = await fixture(t);
  assert.equal((await fixtureValue.handler.fetch(request('GET', 'https://verglas.internal/'))).status, 404);
  assert.equal((await fixtureValue.handler.fetch(request('POST', 'https://verglas.internal/sink/other'))).status, 404);
  const status = await fixtureValue.handler.fetch(request('GET', 'https://verglas.internal/sink/status'));
  assert.equal(status.status, 200);
  const statusValue = await readJson(status);
  assert.equal(statusValue.sink_id, 'primary');
  assert.equal(statusValue.sink_type, 'iceberg');
  assert.match(statusValue.config_digest, /^[a-f0-9]{64}$/u);
  assert.equal(statusValue.confirmed_batches, 0);
});

test('Worker routes only the internal Sink controls', async (t) => {
  const fixtureValue = await fixture(t);
  const worker = createWorker(fixtureValue.loaded.project, manifest(), { transport: fixtureValue.host });
  assert.equal((await worker.fetch(request('GET', 'https://sink.example/'))).status, 404);
  const delivered = await worker.fetch(batchRequest());
  assert.equal(delivered.status, 200);
  assert.equal(fixtureValue.host.catalogCalls.length, 1);
});

test('Sink has no Stream consumer, Pipeline cursor, or local Iceberg authority', async (t) => {
  const files = ['worker.js', 'wrangler.jsonc', 'package.json'];
  const source = (await Promise.all(files.map((file) => readFile(join(root, file), 'utf8')))).join('\n');
  assert.match(source, /CATALOG/);
  for (const term of ['stream\\/read', 'STREAM', 'cursor', 'offload', 'scheduler', 'R2Object', 'parquet-writer']) {
    assert.doesNotMatch(source, new RegExp(term, 'iu'));
  }
  assert.match(source, /catalog\/commit/iu);
  assert.match(source, /cloudflare:workers/);
  assert.doesNotMatch(source, /from ['"](?:node:|npm:|@)/u);
  t.diagnostic(`scanned ${files.join(', ')}`);
});
