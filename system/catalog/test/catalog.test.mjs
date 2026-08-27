import test from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';

import { workerAssetPath } from '@verglas/worker-js/assets';
import { createHandler, createWorker } from '@verglas/worker-js/cloudflare-workers';
import { build as bundle } from 'esbuild';

const root = resolve(new URL('..', import.meta.url).pathname);
const source = join(root, 'worker.js');
const cloudflareWorkersPath = workerAssetPath('cloudflare-workers.js');
const encoder = new TextEncoder();
const decoder = new TextDecoder();
const DIGEST = 'a'.repeat(64);
const BATCH_ID = `[\"orders\",\"${DIGEST}\",1,2,\"primary\"]`;
const FILE_ID = 'verglas/primary/batch-8a99034b7b97cd6a8ec9d413c3ba498644887a81832676e62497b72a49a691d1.parquet';

class PersistedHost {
  constructor(path) {
    this.database = new DatabaseSync(path);
    this.runtimeCalls = [];
    this.runtimeReceipts = new Map();
    this.metadataRecords = new Map();
    this.runtimeFailure = undefined;
    this.loseAuthorityResponse = false;
    this.runtimeMismatch = undefined;
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
    if (binding === 'ICEBERG_COMMIT' && object === 'verglas-runtime') {
      return this.runtimeFetch(request, object);
    }
    throw new Error(`unexpected binding ${binding} object ${object}`);
  }

  runtimeFetch(request, object) {
    const body = decoder.decode(request.body);
    const payload = JSON.parse(body);
    this.runtimeCalls.push({ request, object, body, payload });
    if (payload.operation === 'create-table') {
      const location = payload.request.location
        ?? `s3://lake/${payload.namespace.join('/')}/${payload.request.name}`;
      const publication = {
        'metadata-location': `${location}/metadata/00000.json`,
        metadata: {
          'format-version': 2,
          'table-uuid': '00000000-0000-4000-8000-000000000001',
          location,
          'last-updated-ms': 1,
          'last-column-id': 0,
          schemas: [payload.request.schema],
          'current-schema-id': payload.request.schema['schema-id'],
          'partition-specs': [{ 'spec-id': 0, fields: [] }],
          'default-spec-id': 0,
          'last-partition-id': 999,
          'sort-orders': [{ 'order-id': 0, fields: [] }],
          'default-sort-order-id': 0,
          properties: payload.request.properties ?? {},
          snapshots: [],
          refs: {},
          'last-sequence-number': 0,
          'snapshot-log': [],
          'metadata-log': [],
        },
      };
      this.metadataRecords.set(publication['metadata-location'], publication.metadata);
      return response(200, publication);
    }
    if (payload.operation === 'register-table') {
      const metadata = this.metadataRecords.get(payload.metadata_location);
      return metadata
        ? response(200, { 'metadata-location': payload.metadata_location, metadata })
        : response(404, { error: 'metadata not found' });
    }
    if (payload.operation === 'commit-table') {
      const previous = this.metadataRecords.get(payload.current_metadata_location);
      if (!previous) return response(404, { error: 'metadata not found' });
      const metadata = structuredClone(previous);
      const tableCommit = JSON.parse(payload.request_json);
      for (const requirement of tableCommit.requirements) {
        if (requirement.type === 'assert-table-uuid' && requirement.uuid !== metadata['table-uuid']) {
          return response(409, { error: { message: 'table UUID requirement failed' } });
        }
      }
      for (const update of tableCommit.updates) {
        if (update.action === 'set-properties') {
          metadata.properties = { ...(metadata.properties ?? {}), ...update.updates };
        } else if (update.action === 'remove-properties') {
          for (const key of update.removals) delete metadata.properties[key];
        } else {
          return response(400, { error: `unsupported test update ${update.action}` });
        }
      }
      metadata['last-updated-ms'] += 1;
      const metadataLocation = metadata.location + '/metadata/00001.json';
      this.metadataRecords.set(metadataLocation, metadata);
      return response(200, { 'metadata-location': metadataLocation, metadata });
    }
    if (payload.operation !== 'commit-sink-batch') return response(400, { error: 'unknown operation' });
    const commit = payload.request;
    if (this.runtimeFailure) return response(this.runtimeFailure, { error: 'runtime unavailable' });
    const prior = this.runtimeReceipts.get(commit.batch_id);
    const receipt = prior ?? {
      committed: true,
      batch_id: commit.batch_id,
      file_id: commit.file_id,
      snapshot_id: 'snapshot-42',
      metadata_location: 's3://lake/analytics/events/metadata/00001.json',
      metadata: {
        'format-version': 2,
        'table-uuid': '00000000-0000-4000-8000-000000000042',
        location: 's3://lake/analytics/events',
        'current-snapshot-id': 42,
        schemas: [{ type: 'struct', 'schema-id': 0, fields: [] }],
        'current-schema-id': 0,
      },
      rows_committed: commit.records.length,
    };
    this.runtimeReceipts.set(commit.batch_id, receipt);
    if (this.runtimeMismatch) return response(200, { ...receipt, ...this.runtimeMismatch });
    if (this.loseAuthorityResponse) {
      this.loseAuthorityResponse = false;
      throw new Error('runtime proposal completed but response was lost');
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
    ],
    services: [
      { binding: 'ICEBERG_COMMIT', service: 'verglas-runtime' },
    ],
    vars: {
      CATALOG_ID: 'warehouse',
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

test('serves the Iceberg REST config and multipart namespace contract from SQLite', async (t) => {
  const fixtureValue = await fixture(t);
  const worker = createWorker(fixtureValue.loaded.project, manifest(), { transport: fixtureValue.host });
  const config = await worker.fetch(publicRequest('GET', 'https://tenant.catalog.verglas.dev/v1/config'));
  assert.equal(config.status, 200);
  const configPayload = await readJson(config);
  assert.deepEqual(configPayload.defaults, { warehouse: 'warehouse' });
  assert.deepEqual(configPayload.overrides, {
    's3.endpoint': 'https://tenant.s3.verglas.dev',
    's3.path-style-access': 'true',
    's3.region': 'auto',
  });
  assert.ok(configPayload.endpoints.includes('GET /v1/{prefix}/namespaces/{namespace}'));

  const directConfig = await worker.fetch(publicRequest('GET', 'https://tenant.fly.dev/v1/config'));
  assert.equal(directConfig.status, 200);
  assert.equal((await readJson(directConfig)).overrides['s3.endpoint'], 'https://tenant.fly.dev:8443');

  const created = await worker.fetch(publicRequest(
    'POST',
    'https://catalog.example/v1/namespaces',
    JSON.stringify({ namespace: ['analytics', 'raw'], properties: { owner: 'test' } }),
    { 'content-type': 'application/json' },
  ));
  assert.equal(created.status, 200, decoder.decode(created.body));
  assert.deepEqual(await readJson(created), {
    namespace: ['analytics', 'raw'],
    properties: { owner: 'test' },
  });

  const loaded = await worker.fetch(publicRequest(
    'GET',
    'https://catalog.example/v1/namespaces/analytics%1Fraw',
  ));
  assert.equal(loaded.status, 200, decoder.decode(loaded.body));
  assert.deepEqual(await readJson(loaded), {
    namespace: ['analytics', 'raw'],
    properties: { owner: 'test' },
  });

  const updated = await worker.fetch(publicRequest(
    'POST',
    'https://catalog.example/v1/namespaces/analytics%1Fraw/properties',
    JSON.stringify({ removals: ['owner'], updates: { domain: 'events' } }),
    { 'content-type': 'application/json' },
  ));
  assert.equal(updated.status, 200, decoder.decode(updated.body));
  assert.deepEqual(await readJson(updated), {
    removed: ['owner'],
    updated: ['domain'],
    missing: [],
  });

  const listed = await worker.fetch(publicRequest(
    'GET',
    'https://catalog.example/v1/namespaces?parent=analytics',
  ));
  assert.equal(listed.status, 200, decoder.decode(listed.body));
  assert.deepEqual(await readJson(listed), { namespaces: [['analytics', 'raw']] });
  const missing = await worker.fetch(publicRequest(
    'GET',
    'https://catalog.example/v1/namespaces/analytics%1Fmissing',
  ));
  assert.equal(missing.status, 404);
  assert.deepEqual(await readJson(missing), {
    error: {
      message: 'namespace does not exist',
      type: 'NoSuchNamespaceException',
      code: 404,
    },
  });
  assert.equal(fixtureValue.host.runtimeCalls.length, 0);
});

test('persists a runtime-authored Iceberg table in SQLite across restart', async (t) => {
  const fixtureValue = await fixture(t);
  const worker = createWorker(fixtureValue.loaded.project, manifest(), { transport: fixtureValue.host });
  const namespace = await worker.fetch(publicRequest(
    'POST',
    'https://catalog.example/v1/namespaces',
    JSON.stringify({ namespace: ['analytics'], properties: { owner: 'test' } }),
    { 'content-type': 'application/json' },
  ));
  assert.equal(namespace.status, 200, decoder.decode(namespace.body));
  const table = await worker.fetch(publicRequest(
    'POST',
    'https://catalog.example/v1/namespaces/analytics/tables',
    JSON.stringify({ name: 'events', schema: { type: 'struct', 'schema-id': 0, fields: [] } }),
    { 'content-type': 'application/json' },
  ));
  assert.equal(table.status, 200, decoder.decode(table.body));
  const createdTable = await readJson(table);
  assert.equal(createdTable['metadata-location'], 's3://lake/analytics/events/metadata/00000.json');
  assert.equal(createdTable.metadata['format-version'], 2);
  assert.equal(createdTable.metadata['table-uuid'], '00000000-0000-4000-8000-000000000001');
  assert.equal(fixtureValue.host.runtimeCalls.length, 1);
  assert.equal(fixtureValue.host.runtimeCalls[0].payload.operation, 'create-table');

  await makeHandler(fixtureValue.loaded.project, fixtureValue.host);
  const restarted = createWorker(fixtureValue.loaded.project, manifest(), { transport: fixtureValue.host });
  const loaded = await restarted.fetch(publicRequest(
    'GET',
    'https://catalog.example/v1/namespaces/analytics/tables/events',
  ));
  assert.equal(loaded.status, 200, decoder.decode(loaded.body));
  const payload = await readJson(loaded);
  assert.equal(payload['metadata-location'], 's3://lake/analytics/events/metadata/00000.json');
  assert.equal(payload.metadata.location, 's3://lake/analytics/events');
  assert.deepEqual(payload.metadata.schemas, [{ type: 'struct', 'schema-id': 0, fields: [] }]);
  assert.equal(fixtureValue.host.runtimeCalls.length, 1);
});

test('commits standard Iceberg requirements and updates through the SQLite head', async (t) => {
  const fixtureValue = await fixture(t);
  const worker = createWorker(fixtureValue.loaded.project, manifest(), { transport: fixtureValue.host });
  await worker.fetch(publicRequest(
    'POST',
    'https://catalog.example/v1/namespaces',
    JSON.stringify({ namespace: ['analytics'] }),
    { 'content-type': 'application/json' },
  ));
  await worker.fetch(publicRequest(
    'POST',
    'https://catalog.example/v1/namespaces/analytics/tables',
    JSON.stringify({ name: 'events', schema: { type: 'struct', 'schema-id': 0, fields: [] } }),
    { 'content-type': 'application/json' },
  ));
  const commitPayload = {
    identifier: { namespace: ['analytics'], name: 'events' },
    requirements: [{
      type: 'assert-table-uuid',
      uuid: '00000000-0000-4000-8000-000000000001',
    }],
    updates: [{ action: 'set-properties', updates: { owner: 'pyiceberg' } }],
  };
  const commitHeaders = {
    'content-type': 'application/json',
    'idempotency-key': '01890f3e-7cc2-7cc2-8000-000000000001',
  };
  const committed = await worker.fetch(publicRequest(
    'POST',
    'https://catalog.example/v1/namespaces/analytics/tables/events',
    JSON.stringify(commitPayload),
    commitHeaders,
  ));
  assert.equal(committed.status, 200, decoder.decode(committed.body));
  const payload = await readJson(committed);
  assert.equal(payload['metadata-location'], 's3://lake/analytics/events/metadata/00001.json');
  assert.equal(payload.metadata.properties.owner, 'pyiceberg');
  assert.equal(fixtureValue.host.runtimeCalls.length, 2);
  assert.equal(fixtureValue.host.runtimeCalls[1].payload.operation, 'commit-table');
  assert.equal(fixtureValue.host.runtimeCalls[1].payload.request_json, JSON.stringify(commitPayload));
  assert.equal('request' in fixtureValue.host.runtimeCalls[1].payload, false);
  const replay = await worker.fetch(publicRequest(
    'POST',
    'https://catalog.example/v1/namespaces/analytics/tables/events',
    JSON.stringify(commitPayload),
    commitHeaders,
  ));
  assert.deepEqual(await readJson(replay), payload);
  assert.equal(fixtureValue.host.runtimeCalls.length, 2);
  const conflict = await worker.fetch(publicRequest(
    'POST',
    'https://catalog.example/v1/namespaces/analytics/tables/events',
    JSON.stringify({ ...commitPayload, updates: [{ action: 'set-properties', updates: { owner: 'other' } }] }),
    commitHeaders,
  ));
  assert.equal(conflict.status, 409);
  assert.equal(fixtureValue.host.runtimeCalls.length, 2);
  const stale = await worker.fetch(publicRequest(
    'POST',
    'https://catalog.example/v1/namespaces/analytics/tables/events',
    JSON.stringify({
      ...commitPayload,
      requirements: [{ type: 'assert-table-uuid', uuid: '00000000-0000-4000-8000-000000000099' }],
    }),
    {
      'content-type': 'application/json',
      'idempotency-key': '01890f3e-7cc2-7cc2-8000-000000000002',
    },
  ));
  assert.equal(stale.status, 409);

  const loaded = await worker.fetch(publicRequest(
    'GET',
    'https://catalog.example/v1/namespaces/analytics/tables/events',
  ));
  assert.deepEqual(await readJson(loaded), payload);
});

test('renames and drops table pointers with standard Iceberg routes', async (t) => {
  const fixtureValue = await fixture(t);
  const worker = createWorker(fixtureValue.loaded.project, manifest(), { transport: fixtureValue.host });
  await worker.fetch(publicRequest(
    'POST', 'https://catalog.example/v1/namespaces',
    JSON.stringify({ namespace: ['analytics'] }), { 'content-type': 'application/json' },
  ));
  await worker.fetch(publicRequest(
    'POST', 'https://catalog.example/v1/namespaces/analytics/tables',
    JSON.stringify({ name: 'events', schema: { type: 'struct', 'schema-id': 0, fields: [] } }),
    { 'content-type': 'application/json' },
  ));
  const renamed = await worker.fetch(publicRequest(
    'POST', 'https://catalog.example/v1/tables/rename',
    JSON.stringify({
      source: { namespace: ['analytics'], name: 'events' },
      destination: { namespace: ['analytics'], name: 'events_archive' },
    }),
    { 'content-type': 'application/json' },
  ));
  assert.equal(renamed.status, 204, decoder.decode(renamed.body));
  const loaded = await worker.fetch(publicRequest(
    'GET', 'https://catalog.example/v1/namespaces/analytics/tables/events_archive',
  ));
  assert.equal(loaded.status, 200, decoder.decode(loaded.body));
  const dropped = await worker.fetch(publicRequest(
    'DELETE', 'https://catalog.example/v1/namespaces/analytics/tables/events_archive',
  ));
  assert.equal(dropped.status, 204, decoder.decode(dropped.body));
  const missing = await worker.fetch(publicRequest(
    'GET', 'https://catalog.example/v1/namespaces/analytics/tables/events_archive',
  ));
  assert.equal(missing.status, 404);
});

test('registers an existing Iceberg metadata location as a SQLite table head', async (t) => {
  const fixtureValue = await fixture(t);
  const worker = createWorker(fixtureValue.loaded.project, manifest(), { transport: fixtureValue.host });
  await worker.fetch(publicRequest(
    'POST', 'https://catalog.example/v1/namespaces',
    JSON.stringify({ namespace: ['analytics'] }), { 'content-type': 'application/json' },
  ));
  const created = await worker.fetch(publicRequest(
    'POST', 'https://catalog.example/v1/namespaces/analytics/tables',
    JSON.stringify({ name: 'events', schema: { type: 'struct', 'schema-id': 0, fields: [] } }),
    { 'content-type': 'application/json' },
  ));
  const metadataLocation = (await readJson(created))['metadata-location'];
  await worker.fetch(publicRequest(
    'DELETE', 'https://catalog.example/v1/namespaces/analytics/tables/events',
  ));
  const registered = await worker.fetch(publicRequest(
    'POST', 'https://catalog.example/v1/namespaces/analytics/register',
    JSON.stringify({ name: 'registered_events', 'metadata-location': metadataLocation }),
    { 'content-type': 'application/json' },
  ));
  assert.equal(registered.status, 200, decoder.decode(registered.body));
  const payload = await readJson(registered);
  assert.equal(payload['metadata-location'], metadataLocation);
  assert.equal(payload.metadata['table-uuid'], '00000000-0000-4000-8000-000000000001');
  const loaded = await worker.fetch(publicRequest(
    'GET', 'https://catalog.example/v1/namespaces/analytics/tables/registered_events',
  ));
  assert.deepEqual(await readJson(loaded), payload);
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
  assert.equal(fixtureValue.host.runtimeCalls.length, 0);
});

test('commits a valid batch and exact ledger replay makes one runtime proposal call', async (t) => {
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
    metadata_location: 's3://lake/analytics/events/metadata/00001.json',
    rows_committed: 2,
  });
  assert.equal(fixtureValue.host.runtimeCalls.length, 1);
  const call = fixtureValue.host.runtimeCalls[0];
  assert.equal(call.object, 'verglas-runtime');
  assert.equal(call.request.method, 'POST');
  assert.equal(call.request.uri, 'https://verglas.internal/catalog/commit');
  assert.equal(call.request.headers.find(([name]) => name === 'x-verglas-sink-id')?.[1], 'primary');
  assert.equal(call.request.headers.find(([name]) => name === 'x-verglas-batch-id')?.[1], BATCH_ID);
  assert.equal(call.request.headers.find(([name]) => name === 'x-verglas-file-id')?.[1], FILE_ID);
  assert.equal(call.payload.operation, 'commit-sink-batch');
  assert.equal(call.payload.current_metadata_location, null);
  assert.deepEqual(call.payload.request, commitBody());
  const status = await fixtureValue.handler.fetch(request('GET', 'https://verglas.internal/catalog/status'));
  assert.equal((await readJson(status)).confirmed_batches, 1);
  const table = await fixtureValue.handler.fetch(request(
    'GET',
    'https://verglas.internal/v1/namespaces/analytics/tables/events',
  ));
  const tablePayload = await readJson(table);
  assert.equal(tablePayload['metadata-location'], 's3://lake/analytics/events/metadata/00001.json');
  assert.equal(tablePayload.metadata['current-snapshot-id'], 42);
});

test('a lost runtime response and restart retries the same identity', async (t) => {
  const loaded = await loadProject();
  const directory = await mkdtemp(join(tmpdir(), 'verglas-catalog-restart-'));
  const path = join(directory, 'catalog.sqlite');
  const firstHost = new PersistedHost(path);
  const first = await makeHandler(loaded.project, firstHost);
  firstHost.loseAuthorityResponse = true;
  const lost = await first.fetch(commitRequest());
  assert.equal(lost.status, 502);
  const firstCall = firstHost.runtimeCalls[0];
  firstHost.close();

  const secondHost = new PersistedHost(path);
  secondHost.runtimeReceipts = firstHost.runtimeReceipts;
  const second = await makeHandler(loaded.project, secondHost);
  const retried = await second.fetch(commitRequest());
  assert.equal(retried.status, 200);
  assert.equal(secondHost.runtimeCalls.length, 1);
  assert.equal(secondHost.runtimeCalls[0].body, firstCall.body);
  assert.equal(secondHost.runtimeCalls[0].request.headers.find(([name]) => name === 'x-verglas-batch-id')?.[1], BATCH_ID);
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
  assert.equal(fixtureValue.host.runtimeCalls.length, 1);
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
  assert.equal(fixtureValue.host.runtimeCalls.length, 0);
});

test('malformed identities and hard request ceilings fail before runtime', async (t) => {
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
  assert.equal(fixtureValue.host.runtimeCalls.length, 0);
});

test('a runtime receipt mismatch is rejected without a ledger row', async (t) => {
  const fixtureValue = await fixture(t);
  fixtureValue.host.runtimeMismatch = { rows_committed: 1 };
  const result = await fixtureValue.handler.fetch(commitRequest());
  assert.equal(result.status, 502);
  assert.match(decoder.decode(result.body), /row|receipt/i);
  const status = await fixtureValue.handler.fetch(request('GET', 'https://verglas.internal/catalog/status'));
  assert.equal((await readJson(status)).confirmed_batches, 0);
});

test('Catalog declares one runtime commit capability and no recursive authority object', async () => {
  const manifest = JSON.parse(await readFile(join(root, 'wrangler.jsonc'), 'utf8'));
  assert.deepEqual(manifest.durable_objects.bindings, [
    { name: 'CATALOG_DO', class_name: 'Catalog' },
  ]);
  assert.deepEqual(manifest.services, [
    { binding: 'ICEBERG_COMMIT', service: 'verglas-runtime' },
  ]);
  assert.equal(manifest.vars.CATALOG_AUTHORITY_BINDING, undefined);
  assert.equal(manifest.vars.CATALOG_AUTHORITY_OBJECT, undefined);
});

test('Catalog source has no object-store, credentials, Parquet, or alternate authority implementation', async () => {
  const files = ['worker.js', 'wrangler.jsonc', 'package.json', 'README.md'];
  const joined = (await Promise.all(files.map((file) => readFile(join(root, file), 'utf8')))).join('\n');
  assert.match(joined, /ICEBERG_COMMIT/);
  assert.match(joined, /catalog\/commit/iu);
  assert.doesNotMatch(joined, /CATALOG_AUTHORITY_(?:BINDING|OBJECT)|cache-node/iu);
  assert.doesNotMatch(joined, /(?:node:|npm:|@aws-sdk|R2Object|S3Client|parquet-writer|AWS_ACCESS_KEY|SECRET_ACCESS_KEY)/u);
  assert.doesNotMatch(joined, /IcebergCommitter|VerifiedIcebergArchive|storage\.bucket|object-store/iu);
});
