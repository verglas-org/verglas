import test from 'node:test';
import assert from 'node:assert/strict';

import { createHandler, createStreamBinding, createWorker } from '../src/cloudflare-workers.js';

const MAX_REQUEST_BYTES = 5 * 1024 * 1024;

function response(status, body = '') {
  return { status, headers: [], body: new TextEncoder().encode(body) };
}

test('Stream.send sends the named object a canonical JSON POST and awaits a 2xx ACK', async () => {
  const calls = [];
  const stream = createStreamBinding('STREAM', 'stream-id', {
    doFetch(binding, object, request) {
      calls.push({ binding, object, request });
      return response(202, '{"accepted":2}');
    },
  });

  assert.equal(typeof stream.send, 'function');
  assert.equal(stream.idFromName, undefined);
  await stream.send([{ event: 'one' }, { event: 'two' }]);

  assert.equal(calls.length, 1);
  assert.equal(calls[0].binding, 'STREAM');
  assert.equal(calls[0].object, 'stream-id');
  assert.deepEqual(calls[0].request.headers, [['content-type', 'application/json']]);
  assert.equal(calls[0].request.method, 'POST');
  assert.equal(calls[0].request.uri, 'https://verglas.internal/stream/append');
  assert.deepEqual(JSON.parse(new TextDecoder().decode(calls[0].request.body)), [
    { event: 'one' },
    { event: 'two' },
  ]);
});

test('Durable Object Stream.send stages records through storage before event commit', async () => {
  const calls = [];
  const stream = createStreamBinding('STREAM', 'stream-id', {
    streamSend(binding, object, records) {
      calls.push({ binding, object, records });
    },
  }, { transactional: true });

  await stream.send([{ event: 'staged' }]);
  assert.deepEqual(calls, [{ binding: 'STREAM', object: 'stream-id', records: '[{"event":"staged"}]' }]);
});

test('Durable Object handlers receive transactional Stream bindings from the manifest', async () => {
  const calls = [];
  let stream;
  class Counter {
    constructor(_state, env) {
      stream = env.STREAM;
    }
  }
  const handler = createHandler({ Counter }, {
    bindings: [{ name: 'COUNTER', class_name: 'Counter' }],
    pipelines: [{ binding: 'STREAM', stream: 'stream-id' }],
  }, {
    objectId: { toString: () => '0'.repeat(64) },
    transport: {
      streamSend(binding, object, records) {
        calls.push({ binding, object, records });
      },
    },
  });
  await handler.init();
  await stream.send([{ event: 'handler' }]);
  assert.deepEqual(calls, [{ binding: 'STREAM', object: 'stream-id', records: '[{"event":"handler"}]' }]);
});

test('Stream.send rejects non-arrays, non-JSON values, and payloads over 5 MiB', async () => {
  let calls = 0;
  const stream = createStreamBinding('STREAM', 'stream-id', {
    doFetch() {
      calls += 1;
      return response(200);
    },
  });

  await assert.rejects(stream.send({ event: 'not-an-array' }), /array/i);
  await assert.rejects(stream.send([undefined]), /JSON-serializable/i);
  await assert.rejects(stream.send([1n]), /JSON-serializable/i);
  await assert.rejects(stream.send([{ payload: 'x'.repeat(MAX_REQUEST_BYTES) }]), /5 MiB/i);
  assert.equal(calls, 0);
});

test('Stream.send rejects non-2xx responses and host failures without fallback', async () => {
  let calls = 0;
  const rejected = createStreamBinding('STREAM', 'stream-id', {
    doFetch() {
      calls += 1;
      return response(503, 'not durable');
    },
  });
  await assert.rejects(rejected.send([{ value: 1 }]), /503/);
  assert.equal(calls, 1);

  const hostFailure = createStreamBinding('STREAM', 'stream-id', {
    doFetch() {
      throw new Error('host unavailable');
    },
  });
  await assert.rejects(hostFailure.send([{ value: 1 }]), /host unavailable/);
});

test('pipeline bindings are exposed as Stream bindings without namespace methods', async () => {
  const events = [];
  const worker = createWorker({
    default: {
      async fetch(_request, env) {
        await env.STREAM.send([{ value: 7 }]);
        return new Response('ok');
      },
    },
  }, {
    pipelines: [{ binding: 'STREAM', stream: 'stream-id' }],
    vars: { GREETING: 'hello' },
  }, {
    transport: {
      doFetch(binding, object, request) {
        events.push({ binding, object, request });
        return response(200);
      },
    },
  });

  assert.equal(typeof worker.env.STREAM.send, 'function');
  assert.equal(worker.env.STREAM.get, undefined);
  const result = await worker.fetch({ method: 'GET', uri: 'https://worker.test/', headers: [], body: [] });
  assert.equal(new TextDecoder().decode(result.body), 'ok');
  assert.equal(events.length, 1);
  assert.equal(events[0].object, 'stream-id');
});
