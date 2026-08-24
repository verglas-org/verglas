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
const source = join(root, 'worker.js');
const cloudflareWorkersPath = resolve(root, '../../sdks/worker-js/src/cloudflare-workers.js');
const encoder = new TextEncoder();
const decoder = new TextDecoder();
const DIGEST = 'a'.repeat(64);
const BATCH_ID = `[\"orders\",\"${DIGEST}\",1,2,\"primary\"]`;
const FILE_ID = 'verglas/primary/batch-8a99034b7b97cd6a8ec9d413c3ba498644887a81832676e62497b72a49a691d1.parquet';

class PersistedHost {
  constructor(path) {
    this.database = new DatabaseSync(path);
    this.authorityCalls = [];
    this.authorityRecords = new Map();
    this.authorityFailure = undefined;
    this.loseAuthorityResponse = false;
    this.authorityMismatch = undefined;
    this.catalogHandler = undefined;
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
    if (binding === 'CATALOG_DO') {
      if (!this.catalogHandler) throw new Error('Catalog handler is not attached');
      return this.catalogHandler.fetch(request);
    }
    if (binding === 'AUTHORITY') return this.authorityFetch(request, object);
    throw new Error(`unexpected binding ${binding} object ${object}`);
  }

  authorityFetch(request, object) {
    const body = decoder.decode(request.body);
    const payload = JSON.parse(body);
    this.authorityCalls.push({ request, object, body, payload });
    if (!Object.hasOwn(payload, 'batch_id')) return response(200, { ok: true });
    if (this.authorityFailure) return response(this.authorityFailure, { error: 'authority unavailable' });
    const prior = this.authorityRecords.get(payload.batch_id);
    const receipt = prior ?? {
      committed: true,
      batch_id: payload.batch_id,
      file_id: payload.file_id,
      snapshot_id: 'snapshot-42',
      rows_committed: payload.records.length,
    };
    this.authorityRecords.set(payload.batch_id, receipt);
    if (this.authorityMismatch) return response(200, { ...receipt, ...this.authorityMismatch });
    if (this.loseAuthorityResponse) {
      this.loseAuthorityResponse = false;
      throw new Error('authority committed but response was lost');
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

function commitBody(overrides = {}) {
  return {
    batch_id: BATCH_ID,
    file_id: FILE_ID,
    sink_id: 'primary',
    pipeline_id: 'orders',
    sql_digest: DIGEST,
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
    records: [{ id: 1 }, { id: 2 }],
    ...overrides,
  };
}

function commitRequest(body = commitBody(), headers = {}) {
  const merged = {
    'content-type': 'application/json',
    'x-verglas-sink-id': body.sink_id,
    'x-verglas-batch-id': body.batch_id,
    'x-verglas-file-id': body.file_id,
    'x-verglas-pipeline-id': body.pipeline_id,
    'x-verglas-sql-digest': body.sql_digest,
    ...headers,
  };
  return request('POST', 'https://verglas.internal/catalog/commit', JSON.stringify(body), Object.entries(merged));
}

function publicRequest(method, uri, body = '', headers = {}) {
  return request(method, uri, body, Object.entries(headers));
}

async function loadProject() {
  const result = await bundle({
    entryPoints: [source],
    bundle: true,
    format: 'esm',
    platform: 'node',
    write: false,
    alias: { 'cloudflare:workers': cloudflareWorkersPath },
  });
  const directory = await mkdtemp(join(tmpdir(), 'verglas-catalog-bundle-'));
  const path = join(directory, 'worker.mjs');
  await writeFile(path, result.outputFiles[0].text, 'utf8');
  const project = await import(`${pathToFileURL(path).href}?${Date.now()}-${Math.random()}`);
  return { directory, project };
}

function manifest(vars = {}) {
  return {
    bindings: [
      { name: 'CATALOG_DO', class_name: 'Catalog' },
      { name: 'AUTHORITY', class_name: 'Catalog' },
    ],
    vars: {
      CATALOG_ID: 'warehouse',
      CATALOG_AUTHORITY_BINDING: 'AUTHORITY',
      CATALOG_AUTHORITY_OBJECT: 'warehouse',
      CATALOG_WAREHOUSE: 'warehouse',
      CATALOG_BUCKET: 'lake',
      CATALOG_NAMESPACE: 'analytics',
      CATALOG_TABLE: 'events',
      CATALOG_SINK_ID: 'primary',
      ...vars,
    },
  };
}

async function makeHandler(project, host, vars = {}, objectId) {
  const handler = createHandler(project, manifest(vars), { transport: host, ...(objectId ? { objectId } : {}) });
  await handler.init();
  host.catalogHandler = handler;
  return handler;
}

async function readJson(result) {
  return JSON.parse(decoder.decode(result.body));
}

async function fixture(t, vars = {}) {
  const loaded = await loadProject();
  const directory = await mkdtemp(join(tmpdir(), 'verglas-catalog-state-'));
  const path = join(directory, 'catalog.sqlite');
  const host = new PersistedHost(path);
  const handler = await makeHandler(loaded.project, host, vars);
  t.after(() => {
    host.close();
    return Promise.all([
      rm(directory, { recursive: true, force: true }),
      rm(loaded.directory, { recursive: true, force: true }),
    ]);
  });
  return { loaded, directory, path, host, handler };
}

test('forwards standard REST methods, path, body, and authorization headers', async (t) => {
  const fixtureValue = await fixture(t);
  const worker = createWorker(fixtureValue.loaded.project, manifest(), { transport: fixtureValue.host });
  const body = JSON.stringify({ table: 'events' });
  const result = await worker.fetch(publicRequest('POST', 'https://catalog.example/v1/namespaces/analytics/tables?detail=true', body, {
    authorization: 'Bearer caller-token',
    'x-request-id': 'request-7',
    'content-type': 'application/json',
  }));
  assert.equal(result.status, 200, decoder.decode(result.body));
  assert.equal(fixtureValue.host.authorityCalls.length, 1);
  const call = fixtureValue.host.authorityCalls[0];
  assert.equal(call.request.method, 'POST');
  assert.equal(call.request.uri, 'https://verglas.internal/v1/namespaces/analytics/tables?detail=true');
  assert.equal(call.request.headers.find(([name]) => name === 'authorization')?.[1], 'Bearer caller-token');
  assert.equal(call.request.headers.find(([name]) => name === 'x-request-id')?.[1], 'request-7');
  assert.equal(call.body, body);
});

test('does not expose commit or status on the public Worker', async (t) => {
  const fixtureValue = await fixture(t);
  const worker = createWorker(fixtureValue.loaded.project, manifest(), { transport: fixtureValue.host });
  for (const [method, uri] of [
    ['POST', 'https://catalog.example/catalog/commit'],
    ['GET', 'https://catalog.example/catalog/status'],
    ['GET', 'https://catalog.example/v1/unknown'],
  ]) {
    const result = await worker.fetch(publicRequest(method, uri));
    assert.equal(result.status, 404);
  }
  assert.equal(fixtureValue.host.authorityCalls.length, 0);
});

test('commits a valid batch and exact ledger replay makes one authority call', async (t) => {
  const fixtureValue = await fixture(t);
  const first = await fixtureValue.handler.fetch(commitRequest());
  const retry = await fixtureValue.handler.fetch(commitRequest());
  assert.equal(first.status, 200);
  assert.equal(retry.status, 200);
  assert.deepEqual(await readJson(retry), await readJson(first));
  assert.deepEqual(await readJson(first), {
    committed: true,
    batch_id: BATCH_ID,
    file_id: FILE_ID,
    snapshot_id: 'snapshot-42',
    rows_committed: 2,
  });
  assert.equal(fixtureValue.host.authorityCalls.length, 1);
  const call = fixtureValue.host.authorityCalls[0];
  assert.equal(call.object, 'warehouse');
  assert.equal(call.request.method, 'POST');
  assert.equal(call.request.uri, 'https://verglas.internal/catalog/commit');
  assert.equal(call.request.headers.find(([name]) => name === 'x-verglas-sink-id')?.[1], 'primary');
  assert.equal(call.request.headers.find(([name]) => name === 'x-verglas-batch-id')?.[1], BATCH_ID);
  assert.equal(call.request.headers.find(([name]) => name === 'x-verglas-file-id')?.[1], FILE_ID);
  assert.deepEqual(call.payload, commitBody());
  const status = await fixtureValue.handler.fetch(request('GET', 'https://verglas.internal/catalog/status'));
  assert.equal((await readJson(status)).confirmed_batches, 1);
});

test('a lost authority response and restart retries the same identity', async (t) => {
  const loaded = await loadProject();
  const directory = await mkdtemp(join(tmpdir(), 'verglas-catalog-restart-'));
  const path = join(directory, 'catalog.sqlite');
  const firstHost = new PersistedHost(path);
  const first = await makeHandler(loaded.project, firstHost);
  firstHost.loseAuthorityResponse = true;
  const lost = await first.fetch(commitRequest());
  assert.equal(lost.status, 502);
  const firstCall = firstHost.authorityCalls[0];
  firstHost.close();

  const secondHost = new PersistedHost(path);
  secondHost.authorityRecords = firstHost.authorityRecords;
  const second = await makeHandler(loaded.project, secondHost);
  const retried = await second.fetch(commitRequest());
  assert.equal(retried.status, 200);
  assert.equal(secondHost.authorityCalls.length, 1);
  assert.equal(secondHost.authorityCalls[0].body, firstCall.body);
  assert.equal(secondHost.authorityCalls[0].request.headers.find(([name]) => name === 'x-verglas-batch-id')?.[1], BATCH_ID);
  t.after(() => {
    secondHost.close();
    return Promise.all([rm(directory, { recursive: true, force: true }), rm(loaded.directory, { recursive: true, force: true })]);
  });
});

test('changed payload under a confirmed batch is a hard conflict', async (t) => {
  const fixtureValue = await fixture(t);
  assert.equal((await fixtureValue.handler.fetch(commitRequest())).status, 200);
  const changed = commitRequest(commitBody({ records: [{ id: 999 }, { id: 2 }] }));
  const result = await fixtureValue.handler.fetch(changed);
  assert.equal(result.status, 409);
  assert.match(decoder.decode(result.body), /different payload|reused/i);
  assert.equal(fixtureValue.host.authorityCalls.length, 1);
});

test('changed immutable deployment configuration requires deleting the object', async (t) => {
  const loaded = await loadProject();
  const directory = await mkdtemp(join(tmpdir(), 'verglas-catalog-config-'));
  const path = join(directory, 'catalog.sqlite');
  const firstHost = new PersistedHost(path);
  await makeHandler(loaded.project, firstHost);
  firstHost.close();
  const changedHost = new PersistedHost(path);
  await assert.rejects(async () => {
    const changed = await makeHandler(loaded.project, changedHost, { CATALOG_TABLE: 'other' });
    await changed.fetch(request('GET', 'https://verglas.internal/catalog/status'));
  }, /immutable|delete and recreate/i);
  changedHost.close();
  await rm(directory, { recursive: true, force: true });
  await rm(loaded.directory, { recursive: true, force: true });
});

test('request-selected destination and sink configuration must match immutable config', async (t) => {
  const fixtureValue = await fixture(t);
  const cases = [
    [commitBody({ sink_id: 'other' }), /sink/i],
    [commitBody({ bucket: 'other' }), /bucket/i],
    [commitBody({ namespace: 'other' }), /namespace/i],
    [commitBody({ table: 'other' }), /table/i],
    [commitBody({ format: 'orc' }), /parquet|format/i],
  ];
  for (const [body, expected] of cases) {
    const result = await fixtureValue.handler.fetch(commitRequest(body));
    assert.equal(result.status, 400);
    assert.match(decoder.decode(result.body), expected);
  }
  assert.equal(fixtureValue.host.authorityCalls.length, 0);
});

test('malformed identities and hard request ceilings fail before authority', async (t) => {
  const fixtureValue = await fixture(t);
  const malformed = [
    [commitRequest(commitBody({ batch_id: '["wrong"]' })), /batch/i],
    [commitRequest(commitBody({ file_id: 'not-a-deterministic-file' })), /file/i],
    [commitRequest(commitBody({ first_sequence: 0 })), /sequence/i],
    [commitRequest(commitBody({ last_sequence: 1 })), /batch|range/i],
    [commitRequest(commitBody(), { 'x-verglas-sink-id': 'other' }), /identity|sink/i],
    [request('POST', 'https://verglas.internal/catalog/commit', '{', [
      ['content-type', 'application/json'],
      ['x-verglas-sink-id', 'primary'],
      ['x-verglas-batch-id', BATCH_ID],
      ['x-verglas-file-id', FILE_ID],
      ['x-verglas-pipeline-id', 'orders'],
      ['x-verglas-sql-digest', DIGEST],
    ]), /JSON|body/i],
  ];
  for (const [badRequest, expected] of malformed) {
    const result = await fixtureValue.handler.fetch(badRequest);
    assert.equal(result.status, 400);
    assert.match(decoder.decode(result.body), expected);
  }
  const tooMany = commitBody({ records: Array.from({ length: 10_001 }, (_, index) => ({ index })) });
  const tooManyResult = await fixtureValue.handler.fetch(commitRequest(tooMany));
  assert.equal(tooManyResult.status, 413);

  const tooLarge = commitBody({ records: [{ value: 'x'.repeat(8 * 1024 * 1024) }] });
  const tooLargeResult = await fixtureValue.handler.fetch(commitRequest(tooLarge));
  assert.equal(tooLargeResult.status, 413);
  assert.equal(fixtureValue.host.authorityCalls.length, 0);
});

test('an authority receipt mismatch is rejected without a ledger row', async (t) => {
  const fixtureValue = await fixture(t);
  fixtureValue.host.authorityMismatch = { rows_committed: 1 };
  const result = await fixtureValue.handler.fetch(commitRequest());
  assert.equal(result.status, 502);
  assert.match(decoder.decode(result.body), /row|receipt/i);
  const status = await fixtureValue.handler.fetch(request('GET', 'https://verglas.internal/catalog/status'));
  assert.equal((await readJson(status)).confirmed_batches, 0);
});

test('Catalog source has no object-store, credentials, Parquet, or alternate authority implementation', async () => {
  const files = ['worker.js', 'wrangler.jsonc', 'package.json'];
  const joined = (await Promise.all(files.map((file) => readFile(join(root, file), 'utf8')))).join('\n');
  assert.match(joined, /CATALOG_AUTHORITY_BINDING/);
  assert.match(joined, /catalog\/commit/iu);
  assert.doesNotMatch(joined, /(?:node:|npm:|@aws-sdk|R2Object|S3Client|parquet-writer|AWS_ACCESS_KEY|SECRET_ACCESS_KEY)/u);
  assert.doesNotMatch(joined, /IcebergCommitter|VerifiedIcebergArchive|storage\.bucket|object-store/iu);
});
