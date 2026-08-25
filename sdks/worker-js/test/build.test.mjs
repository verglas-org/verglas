import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

import { buildProject } from '../bin/build.mjs';

const packageDir = new URL('..', import.meta.url);
const jcoPath = new URL('./node_modules/.bin/jco', packageDir);

async function makeProject(withGateway = false) {
  const directory = await mkdtemp(join(tmpdir(), 'verglas-worker-js-test-'));
  await writeFile(
    join(directory, 'wrangler.jsonc'),
    `{
      // The parser must accept the wrangler JSONC subset.
      "name": "test-worker",
      "main": "worker.js",
      "compatibility_date": "2025-01-01",
      "durable_objects": {
        "bindings": [{ "name": "COUNTER", "class_name": "Counter" }],
      },
      "vars": { "GREETING": "hello" },
    }\n`,
  );
  await writeFile(
    join(directory, 'worker.js'),
    `import { DurableObject } from "cloudflare:workers";
export class Counter extends DurableObject {
  fetch() { return new Response("ok"); }
}
export default {
  fetch() { return new Response("ok"); },
};\n`,
  );
  if (withGateway) {
    await writeFile(
      join(directory, 'gateway.json'),
      `${JSON.stringify({
        name: 'test-worker',
        main: 'worker.js',
        durable_objects: { bindings: [{ name: 'COUNTER', class_name: 'Counter' }] },
        artifacts: {
          worker: { digest: '0'.repeat(64), component_dir: '/stale/output' },
          durable_object: { digest: '0'.repeat(64), component_dir: '/stale/output' },
        },
        data_root: '/persistent/data',
      }, null, 2)}\n`,
    );
  }
  return directory;
}

test('build output is valid and records digest determinism', async (t) => {
  const project = await makeProject();
  const outputOne = await mkdtemp(join(tmpdir(), 'verglas-worker-js-out-'));
  const outputTwo = await mkdtemp(join(tmpdir(), 'verglas-worker-js-out-'));
  t.after(async () => {
    await Promise.all([
      rm(project, { recursive: true, force: true }),
      rm(outputOne, { recursive: true, force: true }),
      rm(outputTwo, { recursive: true, force: true }),
    ]);
  });
  const first = await buildProject(project, outputOne);
  const second = await buildProject(project, outputTwo);

  assert.match(first.componentDigest, /^[0-9a-f]{64}$/);
  assert.match(second.componentDigest, /^[0-9a-f]{64}$/);
  assert.equal(createHash('sha256').update(first.componentBytes).digest('hex'), first.componentDigest);
  assert.equal(createHash('sha256').update(second.componentBytes).digest('hex'), second.componentDigest);
  assert.ok(first.componentBytes.byteLength > 0);
  assert.ok(second.componentBytes.byteLength > 0);
  if (first.componentDigest === second.componentDigest) {
    t.diagnostic('componentize output is deterministic for unchanged source');
  } else {
    t.diagnostic(`componentize output is nondeterministic: ${first.componentDigest} != ${second.componentDigest}`);
  }
  assert.deepEqual(JSON.parse(await readFile(first.manifestPath, 'utf8')), {
    name: 'test-worker',
    main: 'worker.js',
    compatibility_date: '2025-01-01',
    compatibility_flags: [],
    durable_objects: { bindings: [{ name: 'COUNTER', class_name: 'Counter' }] },
    migrations: [],
    vars: { GREETING: 'hello' },
    artifacts: {
      worker: { digest: first.componentDigest, component_dir: outputOne },
      durable_object: { digest: first.componentDigest, component_dir: outputOne },
    },
  });

  const wit = spawnSync(fileURLToPath(jcoPath), ['wit', first.componentPath], { encoding: 'utf8' });
  assert.equal(wit.status, 0, wit.stderr);
  assert.match(wit.stdout, /verglas:do-worker\/storage@0\.1\.0/);
  assert.match(wit.stdout, /verglas:do-worker\/sockets@0\.1\.0/);
  assert.match(wit.stdout, /verglas:do-worker\/bindings@0\.1\.0/);
  assert.match(wit.stdout, /verglas:do-worker\/worker@0\.1\.0/);
  assert.match(wit.stdout, /verglas:do-worker\/handler@0\.1\.0/);
  assert.doesNotMatch(wit.stdout, /wasi:/);

  t.diagnostic(`component bytes: ${first.componentBytes.byteLength}`);
  t.diagnostic(`component digest: ${first.componentDigest}`);
});

test('build rejects retired top-level artifact fields', async (t) => {
  const project = await makeProject();
  const output = await mkdtemp(join(tmpdir(), 'verglas-worker-js-legacy-gateway-'));
  const gateway = join(project, 'gateway.json');
  await writeFile(gateway, `${JSON.stringify({
    name: 'test-worker',
    main: 'worker.js',
    durable_objects: { bindings: [{ name: 'COUNTER', class_name: 'Counter' }] },
    component_digest: '0'.repeat(64),
    component_dir: '/stale/output',
  }, null, 2)}\n`);
  t.after(async () => {
    await Promise.all([
      rm(project, { recursive: true, force: true }),
      rm(output, { recursive: true, force: true }),
    ]);
  });

  await assert.rejects(buildProject(project, output, gateway), /top-level component_/);
});

test('build rejects malformed nested artifact descriptors', async (t) => {
  const project = await makeProject();
  const output = await mkdtemp(join(tmpdir(), 'verglas-worker-js-malformed-gateway-'));
  const gateway = join(project, 'gateway.json');
  await writeFile(gateway, `${JSON.stringify({
    name: 'test-worker',
    main: 'worker.js',
    durable_objects: { bindings: [{ name: 'COUNTER', class_name: 'Counter' }] },
    artifacts: { worker: {}, durable_object: {} },
  }, null, 2)}\n`);
  t.after(async () => {
    await Promise.all([
      rm(project, { recursive: true, force: true }),
      rm(output, { recursive: true, force: true }),
    ]);
  });

  await assert.rejects(
    buildProject(project, output, gateway),
    /artifacts\.worker must contain digest and component_dir/,
  );
});

test('build updates an example gateway manifest to the emitted artifact', async (t) => {
  const project = await makeProject(true);
  const output = await mkdtemp(join(tmpdir(), 'verglas-worker-js-gateway-'));
  t.after(async () => {
    await Promise.all([
      rm(project, { recursive: true, force: true }),
      rm(output, { recursive: true, force: true }),
    ]);
  });

  const result = await buildProject(project, output);
  const gatewaySource = await readFile(join(project, 'gateway.json'), 'utf8');
  const gateway = JSON.parse(gatewaySource);
  assert.equal(gatewaySource.endsWith('\n'), true);
  assert.equal(Object.hasOwn(gateway, 'component_digest'), false);
  assert.equal(Object.hasOwn(gateway, 'component_dir'), false);
  assert.deepEqual(gateway.artifacts, {
    worker: { digest: result.componentDigest, component_dir: output },
    durable_object: { digest: result.componentDigest, component_dir: output },
  });
  assert.equal(gateway.data_root, '/persistent/data');
  for (const descriptor of Object.values(gateway.artifacts)) {
    assert.equal(
      createHash('sha256').update(await readFile(join(descriptor.component_dir, `${descriptor.digest}.wasm`))).digest('hex'),
      descriptor.digest,
    );
  }
});
