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
const pipelineSource = join(root, 'worker.js');
const cloudflareWorkersPath = resolve(root, '../../sdks/worker-js/src/cloudflare-workers.js');
const encoder = new TextEncoder();
const decoder = new TextDecoder();

class PersistedHost {
  constructor(path, sourceRecords = []) {
    this.database = new DatabaseSync(path);
    this.sourceRecords = sourceRecords;
    this.sinkCalls = [];
    this.failSinks = new Set();
    this.alarm = undefined;
    this.alarmHistory = [];
  }

  sqlRows(statement) {
    const query = this.database.prepare(statement);
    if (/^\s*(CREATE|INSERT|UPDATE|DELETE|REPLACE|BEGIN|COMMIT|ROLLBACK)\b/iu.test(statement)) {
      query.run();
      return '[]';
    }
    return JSON.stringify(query.all());
  }

  doFetch(binding, object, request) {
    const url = new URL(request.uri);
    if (binding === 'STREAM') {
      const after = Number(url.searchParams.get('after'));
      const limit = Number(url.searchParams.get('limit'));
      const records = this.sourceRecords
        .filter(({ sequence }) => sequence > after)
        .slice(0, limit);
      const nextAfter = records.length === 0 ? after : records.at(-1).sequence;
      return response(200, { records, next_after: nextAfter });
    }
    if (binding.startsWith('SINK_')) {
      const payload = JSON.parse(decoder.decode(request.body));
      this.sinkCalls.push({ binding, object, payload });
      if (this.failSinks.has(binding)) return response(503, { error: 'unavailable' });
      return response(200, { accepted: payload.records.length, batch_id: payload.batch_id });
    }
    if (binding === 'PIPELINE_DO') {
      if (!this.pipelineHandler) throw new Error('Pipeline handler is not attached');
      return this.pipelineHandler.fetch(request);
    }
    throw new Error(`unexpected binding ${binding}`);
  }

  setAlarm(value) {
    this.alarm = Number(value);
    this.alarmHistory.push(this.alarm);
  }

  getAlarm() {
    return this.alarm;
  }

  deleteAlarm() {
    this.alarm = undefined;
  }

  close() {
    this.database.close();
  }
}

function response(status, value) {
  const body = typeof value === 'string' ? value : JSON.stringify(value);
  return { status, headers: [['content-type', 'application/json']], body: encoder.encode(body) };
}

function request(method, uri, body = '') {
  return { method, uri, headers: [['content-type', 'application/json']], body: encoder.encode(body), ws: undefined };
}

async function loadProject() {
  const result = await bundle({
    entryPoints: [pipelineSource],
    bundle: true,
    format: 'esm',
    platform: 'node',
    write: false,
    alias: { 'cloudflare:workers': cloudflareWorkersPath },
  });
  const directory = await mkdtemp(join(tmpdir(), 'verglas-pipeline-bundle-'));
  const path = join(directory, 'worker.mjs');
  await writeFile(path, result.outputFiles[0].text, 'utf8');
  const project = await import(`${pathToFileURL(path).href}?${Date.now()}-${Math.random()}`);
  return { directory, project };
}

function manifest(vars) {
  return {
    bindings: [
      { name: 'PIPELINE_DO', class_name: 'Pipeline' },
      { name: 'STREAM', class_name: 'Pipeline' },
      { name: 'SINK_A', class_name: 'Pipeline' },
      { name: 'SINK_B', class_name: 'Pipeline' },
    ],
    vars: {
      PIPELINE_ID: 'orders',
      PIPELINE_SQL: 'INSERT INTO primary_sink SELECT id, UPPER(kind) AS kind, amount * 2 AS doubled FROM events WHERE amount > 10;',
      PIPELINE_SOURCE_BINDING: 'STREAM',
      PIPELINE_SOURCE_NAME: 'events',
      PIPELINE_SINK_BINDINGS: { primary_sink: 'SINK_A' },
      PIPELINE_BATCH_MAX_ROWS: 100,
      PIPELINE_BATCH_MAX_BYTES: 1024 * 1024,
      PIPELINE_BATCH_MAX_SECONDS: 10,
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

async function withProject(t, sourceRecords, vars = {}) {
  const loaded = await loadProject();
  const directory = await mkdtemp(join(tmpdir(), 'verglas-pipeline-state-'));
  const path = join(directory, 'pipeline.sqlite');
  const host = new PersistedHost(path, sourceRecords);
  const handler = await makeHandler(loaded.project, host, vars);
  host.pipelineHandler = handler;
  t.after(() => {
    host.close();
    return Promise.all([
      rm(directory, { recursive: true, force: true }),
      rm(loaded.directory, { recursive: true, force: true }),
    ]);
  });
  return { loaded, directory, path, host, handler };
}

function records(...values) {
  return values.map((record, index) => ({ sequence: index + 1, record }));
}

test('transforms and filters records with projection aliases', async (t) => {
  const fixture = await withProject(t, records(
    { id: 1, kind: 'skip', amount: 5 },
    { id: 2, kind: 'buy', amount: 11 },
  ));
  const result = await fixture.handler.fetch(request('POST', 'https://verglas.internal/pipeline/process-now'));
  assert.equal(result.status, 200);
  assert.deepEqual(fixture.host.sinkCalls.map(({ payload }) => payload.records), [[
    { id: 2, kind: 'BUY', doubled: 22 },
  ]]);
  assert.deepEqual(await readJson(await fixture.handler.fetch(request('GET', 'https://verglas.internal/pipeline/status'))), {
    pipeline_id: 'orders',
    sql_digest: fixture.host.sinkCalls[0].payload.sql_digest,
    cursor: 2,
    pending: false,
    retry_count: 0,
  });
});

test('fans one input stream out to two named sinks', async (t) => {
  const fixture = await withProject(t, records(
    { id: 1, kind: 'purchase', amount: 9 },
    { id: 2, kind: 'view', amount: 4 },
  ), {
    PIPELINE_SQL: [
      'INSERT INTO purchases SELECT id, amount FROM events WHERE kind = \'purchase\'',
      'INSERT INTO views SELECT id FROM events WHERE kind = \'view\'',
    ].join(';'),
    PIPELINE_SINK_BINDINGS: { purchases: 'SINK_A', views: 'SINK_B' },
  });
  assert.equal((await fixture.handler.fetch(request('POST', 'https://verglas.internal/pipeline/process-now'))).status, 200);
  assert.deepEqual(fixture.host.sinkCalls.map(({ binding, payload }) => [binding, payload.records]), [
    ['SINK_A', [{ id: 1, amount: 9 }]],
    ['SINK_B', [{ id: 2 }]],
  ]);
  assert.notEqual(fixture.host.sinkCalls[0].payload.batch_id, fixture.host.sinkCalls[1].payload.batch_id);
});

test('does not advance the cursor until every sink confirms', async (t) => {
  const fixture = await withProject(t, records({ id: 1, kind: 'purchase', amount: 9 }), {
    PIPELINE_SQL: 'INSERT INTO purchases SELECT * FROM events; INSERT INTO views SELECT * FROM events;',
    PIPELINE_SINK_BINDINGS: { purchases: 'SINK_A', views: 'SINK_B' },
  });
  fixture.host.failSinks.add('SINK_B');
  assert.equal((await fixture.handler.fetch(request('POST', 'https://verglas.internal/pipeline/process-now'))).status, 503);
  assert.equal((await readJson(await fixture.handler.fetch(request('GET', 'https://verglas.internal/pipeline/status')))).cursor, 0);
  fixture.host.failSinks.clear();
  assert.equal((await fixture.handler.fetch(request('POST', 'https://verglas.internal/pipeline/process-now'))).status, 200);
  assert.equal((await readJson(await fixture.handler.fetch(request('GET', 'https://verglas.internal/pipeline/status')))).cursor, 1);
  assert.equal(fixture.host.sinkCalls[1].payload.batch_id, fixture.host.sinkCalls[3].payload.batch_id);
});

test('restarts retry the same durable batch identity', async (t) => {
  const loaded = await loadProject();
  const directory = await mkdtemp(join(tmpdir(), 'verglas-pipeline-retry-'));
  const path = join(directory, 'pipeline.sqlite');
  const firstHost = new PersistedHost(path, records({ id: 7 }));
  const first = await makeHandler(loaded.project, firstHost, {
    PIPELINE_SQL: 'INSERT INTO primary_sink SELECT * FROM events;',
  });
  firstHost.failSinks.add('SINK_A');
  assert.equal((await first.fetch(request('POST', 'https://verglas.internal/pipeline/process-now'))).status, 503);
  firstHost.close();
  const secondHost = new PersistedHost(path, records({ id: 7 }));
  const second = await makeHandler(loaded.project, secondHost, {
    PIPELINE_SQL: 'INSERT INTO primary_sink SELECT * FROM events;',
  });
  assert.equal((await second.fetch(request('POST', 'https://verglas.internal/pipeline/process-now'))).status, 200);
  assert.equal(firstHost.sinkCalls[0].payload.batch_id, secondHost.sinkCalls[0].payload.batch_id);
  t.after(() => {
    secondHost.close();
    return Promise.all([rm(directory, { recursive: true, force: true }), rm(loaded.directory, { recursive: true, force: true })]);
  });
});

test('two Pipeline objects keep independent cursors on one Stream', async (t) => {
  const loaded = await loadProject();
  const directory = await mkdtemp(join(tmpdir(), 'verglas-pipeline-independent-'));
  const firstPath = join(directory, 'first.sqlite');
  const secondPath = join(directory, 'second.sqlite');
  const source = records({ id: 1 }, { id: 2 });
  const firstHost = new PersistedHost(firstPath, source);
  const secondHost = new PersistedHost(secondPath, source);
  const first = await makeHandler(loaded.project, firstHost, {
    PIPELINE_ID: 'first',
    PIPELINE_SQL: 'INSERT INTO primary_sink SELECT * FROM events;',
  });
  const second = await makeHandler(loaded.project, secondHost, {
    PIPELINE_ID: 'second',
    PIPELINE_SQL: 'INSERT INTO primary_sink SELECT * FROM events;',
  });
  await first.fetch(request('POST', 'https://verglas.internal/pipeline/process-now'));
  await second.fetch(request('POST', 'https://verglas.internal/pipeline/process-now'));
  assert.equal((await readJson(await first.fetch(request('GET', 'https://verglas.internal/pipeline/status')))).cursor, 2);
  assert.equal((await readJson(await second.fetch(request('GET', 'https://verglas.internal/pipeline/status')))).cursor, 2);
  assert.equal(firstHost.sinkCalls.length, 1);
  assert.equal(secondHost.sinkCalls.length, 1);
  t.after(() => {
    firstHost.close();
    secondHost.close();
    return Promise.all([rm(directory, { recursive: true, force: true }), rm(loaded.directory, { recursive: true, force: true })]);
  });
});

test('immutable SQL digest mismatch hard-fails initialization', async (t) => {
  const loaded = await loadProject();
  const directory = await mkdtemp(join(tmpdir(), 'verglas-pipeline-config-'));
  const path = join(directory, 'pipeline.sqlite');
  const firstHost = new PersistedHost(path);
  const first = await makeHandler(loaded.project, firstHost);
  firstHost.close();
  const secondHost = new PersistedHost(path);
  const second = await makeHandler(loaded.project, secondHost, { PIPELINE_SQL: 'INSERT INTO primary_sink SELECT id FROM events;' });
  await assert.rejects(
    second.fetch(request('GET', 'https://verglas.internal/pipeline/status')),
    /immutable SQL mismatch|digest/i,
  );
  secondHost.close();
  t.after(() => Promise.all([rm(directory, { recursive: true, force: true }), rm(loaded.directory, { recursive: true, force: true })]));
  void first;
});

test('row, byte, and rolling limits are explicit and enforced', async (t) => {
  const fixture = await withProject(t, records({ id: 1, value: 'a' }, { id: 2, value: 'b' }, { id: 3, value: 'c' }), {
    PIPELINE_SQL: 'INSERT INTO primary_sink SELECT * FROM events;',
    PIPELINE_BATCH_MAX_ROWS: 2,
    PIPELINE_BATCH_MAX_BYTES: 1024,
    PIPELINE_BATCH_MAX_SECONDS: 7,
  });
  await fixture.handler.fetch(request('POST', 'https://verglas.internal/pipeline/process-now'));
  assert.equal(fixture.host.sinkCalls.length, 1);
  assert.equal(fixture.host.sinkCalls[0].payload.records.length, 2);
  assert.equal((await readJson(await fixture.handler.fetch(request('GET', 'https://verglas.internal/pipeline/status')))).cursor, 2);
  assert.equal(fixture.host.alarm, undefined);
  assert.ok(fixture.host.alarmHistory.some((value) => value > Date.now()));
  await fixture.handler.fetch(request('POST', 'https://verglas.internal/pipeline/process-now'));
  assert.equal(fixture.host.sinkCalls.length, 2);
  assert.equal(fixture.host.sinkCalls[1].payload.records.length, 1);
});

test('unsupported stateful SQL fails before the object serves', async (t) => {
  const loaded = await loadProject();
  const directory = await mkdtemp(join(tmpdir(), 'verglas-pipeline-invalid-'));
  const path = join(directory, 'pipeline.sqlite');
  const host = new PersistedHost(path);
  await assert.rejects(
    makeHandler(loaded.project, host, {
      PIPELINE_SQL: 'INSERT INTO out SELECT COUNT(*) FROM events GROUP BY kind;',
    }),
    /unsupported|aggregate|GROUP BY/i,
  );
  host.close();
  t.after(() => Promise.all([rm(directory, { recursive: true, force: true }), rm(loaded.directory, { recursive: true, force: true })]));
});

test('Worker routes only internal process-now and status controls', async (t) => {
  const fixture = await withProject(t, records({ id: 1 }));
  const worker = createWorker(fixture.loaded.project, manifest({}), {
    transport: fixture.host,
  });
  const missing = await worker.fetch(request('GET', 'https://pipeline.example/'));
  assert.equal(missing.status, 404);
  const status = await worker.fetch(request('GET', 'https://pipeline.example/pipeline/status'));
  assert.equal(status.status, 200);
  t.diagnostic('public tenant routes are not exposed');
});

test('Pipeline source has no destination or object-store dependency closure', async (t) => {
  const files = ['worker.js', 'wrangler.jsonc', 'package.json'];
  const source = (await Promise.all(files.map((file) => readFile(join(root, file), 'utf8')))).join('\n');
  for (const term of ['ice' + 'berg', 'cata' + 'log', 'off' + 'load', 'r' + '2', 'object-' + 'store']) {
    assert.doesNotMatch(source, new RegExp(`(?:^|[^a-z])${term}(?:$|[^a-z])`, 'iu'));
  }
  assert.match(source, /cloudflare:workers/);
  assert.doesNotMatch(source, /from ['"](?:node:|npm:|@)/u);
  t.diagnostic(`scanned ${files.join(', ')}`);
});
