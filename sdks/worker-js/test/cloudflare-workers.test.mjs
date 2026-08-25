import test from 'node:test';
import assert from 'node:assert/strict';

import {
  createWorker,
  DurableObject,
  DurableObjectId,
  DurableObjectNamespace,
  DurableObjectState,
  SqlStorageCursor,
} from '../src/cloudflare-workers.js';

class MockHost {
  constructor() {
    this.values = new Map();
    this.calls = [];
    this.alarm = undefined;
  }

  get(key) {
    this.calls.push(['get', key]);
    return this.values.get(key);
  }

  put(key, value) {
    this.calls.push(['put', key, value]);
    this.values.set(key, new Uint8Array(value));
  }

  delete(key) {
    this.calls.push(['delete', key]);
    return this.values.delete(key);
  }

  list(prefix, limit) {
    this.calls.push(['list', prefix, limit]);
    return [...this.values.keys()].filter((key) => key.startsWith(prefix)).slice(0, limit);
  }

  sqlRows(statement) {
    this.calls.push(['sql-rows', statement]);
    return JSON.stringify([{ value: 7 }, { value: 8 }]);
  }

  setAlarm(value) {
    this.calls.push(['set-alarm', value]);
    this.alarm = value;
  }

  getAlarm() {
    this.calls.push(['get-alarm']);
    return this.alarm;
  }

  deleteAlarm() {
    this.calls.push(['delete-alarm']);
    this.alarm = undefined;
  }
}

class Counter extends DurableObject {
  fetch() {
    return new Response(String(this.ctx.id));
  }
}

class AlarmObject extends DurableObject {
  fired = 0;

  async schedule() {
    await this.ctx.storage.setAlarm(Date.now() + 2);
  }

  async alarm() {
    this.fired += 1;
  }

  fetch() {
    return new Response(String(this.fired));
  }
}

test('Cloudflare IDs use lowercase SHA-256 hex and preserve named identity', () => {
  const namespace = new DurableObjectNamespace(Counter);
  const id = namespace.idFromName('alice');

  assert.equal(id.toString(), '2bd806c97f0e00af1a1fc3328fa763a9269723c8db8fac4f93af71db186d6e90');
  assert.equal(id.name, 'alice');
  assert.equal(namespace.idFromName('alice'), id);
  assert.equal(namespace.idFromString(id.toString()).toString(), id.toString());
});

test('local namespace stubs dispatch fetch and storage alarms', async () => {
  const host = new MockHost();
  const namespace = new DurableObjectNamespace(AlarmObject, { transport: host });
  const stub = namespace.get(namespace.idFromName('alarm'));
  await stub.schedule();
  await new Promise((resolve) => setTimeout(resolve, 12));
  assert.equal(await (await stub.fetch('https://worker.test/')).text(), '1');
});

test('worker export builds Cloudflare bindings and awaits waitUntil before completion', async () => {
  const events = [];
  const worker = createWorker({
    default: {
      async fetch(request, env, ctx) {
        ctx.waitUntil(Promise.resolve().then(() => events.push('wait')));
        return env.COUNTER.get(env.COUNTER.idFromName('alice')).fetch(request);
      },
    },
  }, {
    bindings: [{ name: 'COUNTER', class_name: 'Counter' }],
    vars: { GREETING: 'hello' },
  }, {
    transport: {
      doFetch(binding, object, request) {
        events.push(`${binding}:${object}:${request.method}`);
        return {
          status: 200,
          headers: [['content-type', 'text/plain']],
          body: new TextEncoder().encode('ok'),
          acceptWs: undefined,
        };
      },
    },
  });
  const response = await worker.fetch({ method: 'GET', uri: 'https://worker.test/', headers: [], body: [] });
  assert.equal(new TextDecoder().decode(response.body), 'ok');
  assert.deepEqual(events.sort(), ['COUNTER:alice:GET', 'wait']);
});

test('service binding fetch preserves declared binding and service target', async () => {
  const calls = [];
  const worker = createWorker({
    default: {
      async fetch(request, env) {
        return env.ICEBERG_COMMIT.fetch(request);
      },
    },
  }, {
    services: [{ binding: 'ICEBERG_COMMIT', service: 'verglas-runtime' }],
  }, {
    transport: {
      doFetch(binding, object, request) {
        calls.push([binding, object, request.method]);
        return {
          status: 204,
          headers: [],
          body: new Uint8Array(),
          acceptWs: undefined,
        };
      },
    },
  });
  const response = await worker.fetch({ method: 'POST', uri: 'https://worker.test/commit', headers: [], body: [] });
  assert.equal(response.status, 204);
  assert.deepEqual(calls, [['ICEBERG_COMMIT', 'verglas-runtime', 'POST']]);
});

test('storage adapter uses WIT verbs and structured-clone bytes', async () => {
  const host = new MockHost();
  const state = new DurableObjectState(new DurableObjectId('1'.repeat(64)), host);

  await state.storage.put('value', { count: 7, big: 9n, bytes: new Uint8Array([1, 2]) });
  assert.deepEqual(await state.storage.get('value'), {
    count: 7,
    big: 9n,
    bytes: new Uint8Array([1, 2]).buffer,
  });
  assert.equal(await state.storage.delete('value'), true);
  assert.deepEqual(host.calls.map(([operation]) => operation), ['put', 'get', 'delete']);
  assert.equal(host.calls[0][2] instanceof Uint8Array, true);
});

test('SQL rows hydrate the Cloudflare cursor and alarm calls cross the WIT seam', async () => {
  const host = new MockHost();
  const state = new DurableObjectState(new DurableObjectId('2'.repeat(64)), host);

  const cursor = state.storage.sql.exec('SELECT value FROM counter WHERE id = ?', 'alice');
  assert.ok(cursor instanceof SqlStorageCursor);
  assert.deepEqual(cursor.toArray(), [{ value: 7 }, { value: 8 }]);
  assert.deepEqual(cursor.raw(), [[7], [8]]);
  assert.equal(cursor.columnNames[0], 'value');

  await state.storage.setAlarm(1234);
  assert.equal(await state.storage.getAlarm(), 1234);
  await state.storage.deleteAlarm();
  assert.equal(await state.storage.getAlarm(), undefined);
  assert.match(host.calls.find(([operation]) => operation === 'sql-rows')[1], /'alice'/);
});

test('blockConcurrencyWhile gates object dispatch and waitUntil is awaited', async () => {
  const state = new DurableObjectState(new DurableObjectId('3'.repeat(64)), new MockHost());
  const events = [];
  state.blockConcurrencyWhile(async () => {
    events.push('begin');
    await new Promise((resolve) => setTimeout(resolve, 2));
    events.push('end');
  });
  state.waitUntil(Promise.resolve().then(() => events.push('wait')));
  await state.waitForConcurrency();
  await state.waitForWaitUntil();
  assert.deepEqual(events, ['begin', 'wait', 'end']);
});
