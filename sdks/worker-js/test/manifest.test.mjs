import test from 'node:test';
import assert from 'node:assert/strict';

import { parseWranglerManifest } from '../src/manifest.js';

test('accepts the supported wrangler manifest subset', () => {
  const manifest = parseWranglerManifest({
    name: 'counter',
    main: 'worker.js',
    durable_objects: {
      bindings: [{ name: 'COUNTER', class_name: 'Counter' }],
    },
  });

  assert.deepEqual(manifest, {
    name: 'counter',
    main: 'worker.js',
    bindings: [{ name: 'COUNTER', class_name: 'Counter' }],
  });
});

test('rejects an unknown top-level manifest key by name', () => {
  assert.throws(
    () => parseWranglerManifest({ name: 'counter', main: 'worker.js', compatibility_date: '2025-01-01' }),
    /unknown top-level key.*compatibility_date/i,
  );
});

test('rejects a missing name', () => {
  assert.throws(() => parseWranglerManifest({ main: 'worker.js' }), /name.*required/i);
});

test('rejects a missing main', () => {
  assert.throws(() => parseWranglerManifest({ name: 'counter' }), /main.*required/i);
});

test('rejects malformed durable object bindings', () => {
  assert.throws(
    () => parseWranglerManifest({
      name: 'counter',
      main: 'worker.js',
      durable_objects: { bindings: [{ name: 'COUNTER' }] },
    }),
    /class_name.*required/i,
  );
});
