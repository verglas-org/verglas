import test from 'node:test';
import assert from 'node:assert/strict';

import {
  makeRequest,
  makeResponse,
  bytesFromValue,
  valueFromBytes,
} from '../src/http.js';

test('request conversion exposes method, URL, headers, text, and json', async () => {
  const request = makeRequest({
    method: 'POST',
    uri: 'https://example.test/incr',
    headers: [['content-type', 'application/json']],
    body: new TextEncoder().encode('{"n":2}'),
  });

  assert.equal(request.method, 'POST');
  assert.equal(request.url, 'https://example.test/incr');
  assert.equal(request.headers.get('content-type'), 'application/json');
  assert.equal(await request.text(), '{"n":2}');
  assert.deepEqual(await request.json(), { n: 2 });
});

test('response conversion accepts common response-like values', async () => {
  const response = await makeResponse({
    status: 201,
    headers: { 'content-type': 'text/plain' },
    body: 'created',
  });

  assert.equal(response.status, 201);
  assert.deepEqual(response.headers, [['content-type', 'text/plain']]);
  assert.equal(new TextDecoder().decode(response.body), 'created');
});

test('byte helpers preserve strings and byte arrays', () => {
  assert.deepEqual(bytesFromValue('hello'), new TextEncoder().encode('hello'));
  const bytes = new Uint8Array([0, 1, 255]);
  assert.deepEqual(bytesFromValue(bytes), bytes);
  assert.equal(valueFromBytes(bytes, 'string'), '\u0000\u0001�');
  assert.deepEqual(valueFromBytes(new TextEncoder().encode('{"ok":true}'), 'json'), { ok: true });
});
