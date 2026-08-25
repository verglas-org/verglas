import test from 'node:test';
import assert from 'node:assert/strict';

import { createTransport } from '../src/transport-core.js';

test('transport adapter routes structured WIT verbs without envelopes', async () => {
  const calls = [];
  const imports = {
    get: (key) => { calls.push(['get', key]); return { tag: 'ok', val: { tag: 'some', val: new Uint8Array([1]) } }; },
    put: (key, value) => { calls.push(['put', key, value]); return { tag: 'ok', val: undefined }; },
    delete: (key) => { calls.push(['delete', key]); return { tag: 'ok', val: true }; },
    list: (prefix, limit) => { calls.push(['list', prefix, limit]); return { tag: 'ok', val: ['a'] }; },
    sqlRows: (statement) => { calls.push(['sql-rows', statement]); return { tag: 'ok', val: '[]' }; },
    streamSend: (binding, stream, records) => { calls.push(['stream-send', binding, stream, records]); return { tag: 'ok', val: undefined }; },
    setAlarm: (value) => { calls.push(['set-alarm', value]); return { tag: 'ok', val: undefined }; },
    getAlarm: () => { calls.push(['get-alarm']); return { tag: 'ok', val: { tag: 'some', val: 10n } }; },
    deleteAlarm: () => { calls.push(['delete-alarm']); return { tag: 'ok', val: undefined }; },
    send: (socket, value) => { calls.push(['send', socket, value]); return { tag: 'ok', val: undefined }; },
    close: (socket, code, reason) => { calls.push(['close', socket, code, reason]); return { tag: 'ok', val: undefined }; },
    setAttachment: (socket, value) => { calls.push(['set-attachment', socket, value]); return { tag: 'ok', val: undefined }; },
    getAttachment: () => ({ tag: 'ok', val: { tag: 'none' } }),
    attached: () => ({ tag: 'ok', val: [2n] }),
    doFetch: (binding, object, request) => ({ tag: 'ok', val: { binding, object, request } }),
  };
  const transport = createTransport(imports);

  assert.deepEqual(transport.get('value'), new Uint8Array([1]));
  transport.put('value', 'hello');
  assert.equal(transport.delete('value'), true);
  assert.deepEqual(transport.list('prefix', 3), ['a']);
  assert.equal(transport.sqlRows('SELECT 1'), '[]');
  transport.streamSend('STREAM', 'stream-id', '[{"value":1}]');
  transport.setAlarm(10);
  assert.equal(transport.getAlarm(), 10n);
  transport.deleteAlarm();
  transport.send(2, 'message');
  const result = transport.doFetch('COUNTER', 'alice', { method: 'GET' });
  assert.equal(result.object, 'alice');
  assert.deepEqual(transport.attached(), [2n]);
  assert.deepEqual(calls.map(([operation]) => operation), [
    'get', 'put', 'delete', 'list', 'sql-rows', 'stream-send', 'set-alarm', 'get-alarm',
    'delete-alarm', 'send',
  ]);
  assert.equal(calls.some(([operation]) => /commit|query|begin|statement/i.test(operation)), false);
});

test('transport adapter raises host errors at the named WIT operation', () => {
  const imports = {
    get: () => ({ tag: 'err', val: { message: 'denied' } }),
    put: () => undefined,
    delete: () => undefined,
    list: () => undefined,
    sqlRows: () => undefined,
    streamSend: () => undefined,
    setAlarm: () => undefined,
    getAlarm: () => undefined,
    deleteAlarm: () => undefined,
    send: () => undefined,
    close: () => undefined,
    setAttachment: () => undefined,
    getAttachment: () => undefined,
    attached: () => [],
    doFetch: () => undefined,
  };
  assert.throws(() => createTransport(imports).get('value'), /storage\.get: denied/);
});
