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

test('accepts exact pipeline bindings', () => {
  assert.deepEqual(parseWranglerManifest({
    name: 'stream-worker',
    main: 'worker.js',
    durable_objects: { bindings: [{ name: 'OBJECTS', class_name: 'Object' }] },
    pipelines: [{ binding: 'STREAM', stream: 'stream-id' }],
  }), {
    name: 'stream-worker',
    main: 'worker.js',
    compatibility_flags: [],
    bindings: [{ name: 'OBJECTS', class_name: 'Object' }],
    migrations: [],
    vars: {},
    pipelines: [{ binding: 'STREAM', stream: 'stream-id' }],
  });
});

test('accepts exact service bindings', () => {
  const manifest = parseWranglerManifest({
    name: 'catalog',
    main: 'worker.js',
    services: [{ binding: 'ICEBERG_COMMIT', service: 'verglas-runtime' }],
  });
  assert.deepEqual(manifest.services, [
    { binding: 'ICEBERG_COMMIT', service: 'verglas-runtime' },
  ]);
});

test('rejects malformed services and cross-kind duplicate binding names', () => {
  assert.throws(
    () => parseWranglerManifest({
      name: 'catalog',
      main: 'worker.js',
      services: [{ binding: 'ICEBERG_COMMIT', service: 'verglas-runtime', extra: true }],
    }),
    /unknown services\[0\] key.*extra/i,
  );
  assert.throws(
    () => parseWranglerManifest({
      name: 'catalog',
      main: 'worker.js',
      durable_objects: { bindings: [{ name: 'ICEBERG_COMMIT', class_name: 'Catalog' }] },
      services: [{ binding: 'ICEBERG_COMMIT', service: 'verglas-runtime' }],
    }),
    /duplicate binding name.*ICEBERG_COMMIT/i,
  );
});

test('rejects unknown pipeline keys and duplicate binding names', () => {
  assert.throws(
    () => parseWranglerManifest({
      name: 'stream-worker',
      main: 'worker.js',
      pipelines: [{ binding: 'STREAM', stream: 'stream-id', extra: true }],
    }),
    /unknown pipelines\[0\] key.*extra/i,
  );
  assert.throws(
    () => parseWranglerManifest({
      name: 'stream-worker',
      main: 'worker.js',
      durable_objects: { bindings: [{ name: 'STREAM', class_name: 'Object' }] },
      pipelines: [{ binding: 'STREAM', stream: 'stream-id' }],
    }),
    /duplicate binding name.*STREAM/i,
  );
});
