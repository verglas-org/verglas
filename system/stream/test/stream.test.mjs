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
const streamSource = join(root, 'worker.js');
const cloudflareWorkersPath = resolve(root, '../../sdks/worker-js/src/cloudflare-workers.js');
const encoder = new TextEncoder();
const decoder = new TextDecoder();
const MAX_READ_LIMIT = 1000;

class PersistedHost {
  constructor(path) {
    this.database = new DatabaseSync(path);
  }

  sqlRows(statement) {
    const query = this.database.prepare(statement);
    if (/^\s*(CREATE|INSERT|UPDATE|DELETE|REPLACE|BEGIN|COMMIT|ROLLBACK)\b/iu.test(statement)) {
      query.run();
      return '[]';
    }
    return JSON.stringify(query.all());
  }

  close() {
    this.database.close();
  }
}

async function loadProject() {
  const result = await bundle({
    entryPoints: [streamSource],
    bundle: true,
    format: 'esm',
    platform: 'node',
    write: false,
    alias: { 'cloudflare:workers': cloudflareWorkersPath },
  });
  const directory = await mkdtemp(join(tmpdir(), 'verglas-stream-bundle-'));
  const path = join(directory, 'worker.mjs');
  await writeFile(path, result.outputFiles[0].text, 'utf8');
  const project = await import(`${pathToFileURL(path).href}?${Date.now()}-${Math.random()}`);
  return { directory, project };
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

async function body(response) {
  return JSON.parse(decoder.decode(response.body));
}

async function append(handler, records, eventId) {
  const headers = [['content-type', 'application/json']];
  if (eventId !== undefined) headers.push(['x-verglas-producer-event-id', eventId]);
  return handler.fetch(request('POST', 'https://verglas.internal/stream/append', JSON.stringify(records), headers));
}

async function readRecords(handler, after, limit) {
  const response = await handler.fetch(request('GET', `https://verglas.internal/stream/read?after=${after}&limit=${limit}`));
  return { response, body: await body(response) };
}

async function makeHandler(project, host) {
  const handler = createHandler(project, {
    bindings: [{ name: 'STREAM_DO', class_name: 'Stream' }],
    vars: {},
  }, { transport: host });
  await handler.init();
  return handler;
}

test('Stream appends ordered records and gives independent bounded reads', async (t) => {
  const directory = await mkdtemp(join(tmpdir(), 'verglas-stream-state-'));
  const path = join(directory, 'stream.sqlite');
  const loaded = await loadProject();
  const host = new PersistedHost(path);
  const handler = await makeHandler(loaded.project, host);
  t.after(() => {
    host.close();
    return Promise.all([rm(directory, { recursive: true, force: true }), rm(loaded.directory, { recursive: true, force: true })]);
  });

  assert.equal((await append(handler, [{ value: 1 }, { value: 2 }])).status, 200);
  assert.deepEqual(await body(await append(handler, [{ value: 3 }])), {
    accepted: 1,
    sequences: [3],
  });
  const first = await readRecords(handler, 0, 2);
  assert.equal(first.response.status, 200);
  assert.deepEqual(first.body.records, [
    { sequence: 1, record: { value: 1 } },
    { sequence: 2, record: { value: 2 } },
  ]);
  const second = await readRecords(handler, 2, 2);
  assert.deepEqual(second.body.records, [{ sequence: 3, record: { value: 3 } }]);
  const independent = await readRecords(handler, 0, 10);
  assert.deepEqual(independent.body.records.map(({ sequence }) => sequence), [1, 2, 3]);
});

test('producer event identity deduplicates with a stable acknowledgement', async (t) => {
  const directory = await mkdtemp(join(tmpdir(), 'verglas-stream-dedupe-'));
  const path = join(directory, 'stream.sqlite');
  const loaded = await loadProject();
  const host = new PersistedHost(path);
  const handler = await makeHandler(loaded.project, host);
  t.after(() => {
    host.close();
    return Promise.all([rm(directory, { recursive: true, force: true }), rm(loaded.directory, { recursive: true, force: true })]);
  });

  const first = await append(handler, [{ event: 'one' }], 'producer-1');
  const retry = await append(handler, [{ event: 'one' }], 'producer-1');
  assert.equal(first.status, 200);
  const firstAck = await body(first);
  const retryAck = await body(retry);
  assert.deepEqual(firstAck, {
    accepted: 1,
    sequences: [1],
  });
  assert.deepEqual(retryAck, firstAck);
  const rows = await readRecords(handler, 0, 10);
  assert.deepEqual(rows.body.records, [{ sequence: 1, record: { event: 'one' }, producer_event_id: 'producer-1' }]);
});

test('ordered state survives a handler restart using the same persisted database', async (t) => {
  const directory = await mkdtemp(join(tmpdir(), 'verglas-stream-restart-'));
  const path = join(directory, 'stream.sqlite');
  const loaded = await loadProject();
  const firstHost = new PersistedHost(path);
  const firstHandler = await makeHandler(loaded.project, firstHost);
  await append(firstHandler, [{ value: 'before-restart' }]);
  firstHost.close();
  const secondHost = new PersistedHost(path);
  const secondHandler = await makeHandler(loaded.project, secondHost);
  t.after(() => {
    secondHost.close();
    return Promise.all([rm(directory, { recursive: true, force: true }), rm(loaded.directory, { recursive: true, force: true })]);
  });

  const appended = await append(secondHandler, [{ value: 'after-restart' }]);
  assert.deepEqual(await body(appended), {
    accepted: 1,
    sequences: [2],
  });
  const rows = await readRecords(secondHandler, 0, 10);
  assert.deepEqual(rows.body.records.map(({ sequence, record }) => ({ sequence, record })), [
    { sequence: 1, record: { value: 'before-restart' } },
    { sequence: 2, record: { value: 'after-restart' } },
  ]);
});

test('read validation rejects malformed ranges and the hard maximum', async (t) => {
  const directory = await mkdtemp(join(tmpdir(), 'verglas-stream-validation-'));
  const path = join(directory, 'stream.sqlite');
  const loaded = await loadProject();
  const host = new PersistedHost(path);
  const handler = await makeHandler(loaded.project, host);
  t.after(() => {
    host.close();
    return Promise.all([rm(directory, { recursive: true, force: true }), rm(loaded.directory, { recursive: true, force: true })]);
  });

  for (const uri of [
    `https://verglas.internal/stream/read?after=0&limit=${MAX_READ_LIMIT + 1}`,
    'https://verglas.internal/stream/read?after=-1&limit=1',
    'https://verglas.internal/stream/read?after=0&limit=0',
    'https://verglas.internal/stream/read?after=nope&limit=1',
  ]) {
    const response = await handler.fetch(request('GET', uri));
    assert.equal(response.status, 400, uri);
  }
});

test('Worker endpoint fails closed for configured auth and emits configured CORS', async (t) => {
  const directory = await mkdtemp(join(tmpdir(), 'verglas-stream-http-'));
  const path = join(directory, 'stream.sqlite');
  const loaded = await loadProject();
  const host = new PersistedHost(path);
  const handler = await makeHandler(loaded.project, host);
  const worker = createWorker(loaded.project, {
    bindings: [{ name: 'STREAM_DO', class_name: 'Stream' }],
    vars: {
      STREAM_NAME: 'main',
      STREAM_AUTH_TOKEN: 'secret',
      STREAM_CORS_ORIGIN: 'https://client.example',
    },
  }, {
    transport: {
      doFetch: async (_binding, _object, internalRequest) => handler.fetch(internalRequest),
    },
  });
  t.after(() => {
    host.close();
    return Promise.all([rm(directory, { recursive: true, force: true }), rm(loaded.directory, { recursive: true, force: true })]);
  });

  const makeExternal = (headers = []) => request('POST', 'https://stream.example/', '[{"value":9}]', headers);
  const preflight = await worker.fetch(request('OPTIONS', 'https://stream.example/'));
  assert.equal(preflight.status, 204);
  assert.equal(preflight.headers.find(([name]) => name === 'access-control-allow-origin')[1], 'https://client.example');
  const missing = await worker.fetch(makeExternal());
  assert.equal(missing.status, 401);
  assert.equal(missing.headers.find(([name]) => name === 'access-control-allow-origin')[1], 'https://client.example');
  assert.equal((await readRecords(handler, 0, 10)).body.records.length, 0);
  assert.equal((await worker.fetch(makeExternal([['authorization', 'Bearer wrong']]))).status, 401);
  const accepted = await worker.fetch(makeExternal([
    ['authorization', 'Bearer secret'],
    ['content-type', 'application/json'],
  ]));
  assert.equal(accepted.status, 200);
  assert.equal((await readRecords(handler, 0, 10)).body.records.length, 1);
});

test('Stream source has no disallowed integration imports or bindings', async (t) => {
  const files = ['worker.js', 'wrangler.jsonc'];
  const source = (await Promise.all(files.map((file) => readFile(join(root, file), 'utf8')))).join('\n');
  const forbidden = ['ice' + 'berg', 's' + 'ink', 'cata' + 'log', 'off' + 'load', 'r' + '2', 'object-' + 'store'];
  for (const term of forbidden) assert.doesNotMatch(source, new RegExp(`(?:^|[^a-z])${term}(?:$|[^a-z])`, 'iu'));
  assert.match(source, /cloudflare:workers/);
  assert.doesNotMatch(source, /from ['"](?:node:|npm:|@)/u);
  t.diagnostic(`scanned ${files.join(', ')}`);
});
