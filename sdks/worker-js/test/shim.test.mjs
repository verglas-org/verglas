import test from 'node:test';
import assert from 'node:assert/strict';

import {
  makeRequest,
  makeResponse,
  requestToRecord,
  responseFromRecord,
  bytesFromValue,
  valueFromBytes,
} from '../src/http.js';

test('request conversion exposes a real WHATWG Request and Headers surface', async () => {
  const request = makeRequest({
    method: 'POST',
    uri: 'https://example.test/incr',
    headers: [['content-type', 'application/json']],
    body: new TextEncoder().encode('{"n":2}'),
  });

  assert.equal(request instanceof Request, true);
  assert.equal(request.method, 'POST');
  assert.equal(request.url, 'https://example.test/incr');
  assert.equal(request.headers instanceof Headers, true);
  assert.equal(request.headers.get('content-type'), 'application/json');
  assert.deepEqual(await request.clone().json(), { n: 2 });
  assert.equal(await request.text(), '{"n":2}');
});

test('pending WebSocket identity survives Worker-to-DO binding conversion', async () => {
  const request = makeRequest({
    method: 'GET',
    uri: 'https://example.test/socket',
    headers: [],
    body: [],
    ws: { tag: 'some', val: 9n },
  });
  const record = await requestToRecord(request);
  assert.equal(record.ws, 9n);
});

test('response conversion accepts a real WHATWG Response and round-trips WIT bytes', async () => {
  const response = await makeResponse(new Response('created', {
    status: 201,
    headers: { 'content-type': 'text/plain' },
  }), 9n);

  assert.equal(response.status, 201);
  assert.equal(response.acceptWs, undefined);
  assert.deepEqual(response.headers, [['content-type', 'text/plain']]);
  assert.equal(new TextDecoder().decode(response.body), 'created');
  const roundTrip = responseFromRecord(response);
  assert.equal(await roundTrip.text(), 'created');
});

test('binding request conversion preserves method, URL, headers, and body', async () => {
  const record = await requestToRecord('https://example.test/incr', {
    method: 'POST',
    headers: { 'x-test': 'yes' },
    body: 'body',
  });
  assert.equal(record.method, 'POST');
  assert.equal(record.uri, 'https://example.test/incr');
  assert.deepEqual(record.headers, [['content-type', 'text/plain;charset=UTF-8'], ['x-test', 'yes']]);
  assert.equal(new TextDecoder().decode(record.body), 'body');
});

test('byte helpers preserve strings and byte arrays', () => {
  assert.deepEqual(bytesFromValue('hello'), new TextEncoder().encode('hello'));
  const bytes = new Uint8Array([0, 1, 255]);
  assert.deepEqual(bytesFromValue(bytes), bytes);
  assert.equal(valueFromBytes(bytes, 'string'), '\u0000\u0001�');
  assert.deepEqual(valueFromBytes(new TextEncoder().encode('{"ok":true}'), 'json'), { ok: true });
});
