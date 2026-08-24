import test from 'node:test';
import assert from 'node:assert/strict';

import { parseWranglerManifest } from '../src/manifest.js';

test('accepts the Cloudflare Wrangler manifest subset', () => {
  const manifest = parseWranglerManifest({
    name: 'counter',
    main: 'worker.js',
    compatibility_date: '2025-01-01',
    compatibility_flags: ['nodejs_compat'],
    durable_objects: {
      bindings: [{ name: 'COUNTER', class_name: 'Counter' }],
    },
    migrations: [{ tag: 'v1', new_sqlite_classes: ['Counter'] }],
    vars: { GREETING: 'hello' },
  });

  assert.deepEqual(manifest, {
    name: 'counter',
    main: 'worker.js',
    compatibility_date: '2025-01-01',
    compatibility_flags: ['nodejs_compat'],
    bindings: [{ name: 'COUNTER', class_name: 'Counter' }],
    migrations: [{ tag: 'v1', new_classes: [], new_sqlite_classes: ['Counter'] }],
    vars: { GREETING: 'hello' },
  });
});

test('accepts omitted optional Wrangler fields with explicit empty values', () => {
  assert.deepEqual(parseWranglerManifest({
    name: 'counter',
    main: 'worker.js',
    durable_objects: { bindings: [] },
  }), {
    name: 'counter',
    main: 'worker.js',
    compatibility_flags: [],
    bindings: [],
    migrations: [],
    vars: {},
  });
});

test('rejects an unknown top-level manifest key by name', () => {
  assert.throws(
    () => parseWranglerManifest({ name: 'counter', main: 'worker.js', unknown_field: true }),
    /unknown top-level key.*unknown_field/i,
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

test('rejects unsupported migration keys by name', () => {
  assert.throws(
    () => parseWranglerManifest({
      name: 'counter',
      main: 'worker.js',
      migrations: [{ tag: 'v1', deleted_classes: ['Counter'] }],
    }),
    /unknown migrations\[0\] key.*deleted_classes/i,
  );
});
