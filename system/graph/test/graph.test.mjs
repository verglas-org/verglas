import test from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';

import { workerAssetPath } from '@verglas/worker-js/assets';
import { createHandler } from '@verglas/worker-js/cloudflare-workers';
import { build as bundle } from 'esbuild';

const encoder = new TextEncoder();
const decoder = new TextDecoder();
const root = resolve(new URL('..', import.meta.url).pathname);

async function loadProject() {
  const result = await bundle({
    entryPoints: [join(root, 'worker.js')],
    bundle: true,
    format: 'esm',
    platform: 'node',
    write: false,
    alias: { 'cloudflare:workers': workerAssetPath('cloudflare-workers.js') },
  });
  const directory = await mkdtemp(join(tmpdir(), 'verglas-graph-bundle-'));
  const path = join(directory, 'worker.mjs');
  await writeFile(path, result.outputFiles[0].text, 'utf8');
  return { directory, project: await import(`${pathToFileURL(path).href}?${Math.random()}`) };
}

class GraphHost {
  constructor(path) {
    this.database = new DatabaseSync(path);
  }

  sqlRows(statement) {
    const query = this.database.prepare(statement);
    if (/^\s*(CREATE|INSERT|UPDATE|DELETE|REPLACE|DROP)\b/iu.test(statement)) {
      query.run();
      return '[]';
    }
    return JSON.stringify(query.all());
  }

  close() {
    this.database.close();
  }
}

function request(path, body = {}) {
  return {
    method: 'POST',
    uri: `https://verglas.internal${path}`,
    headers: [['content-type', 'application/json']],
    body: encoder.encode(JSON.stringify(body)),
    ws: undefined,
  };
}

async function json(response) {
  return JSON.parse(decoder.decode(response.body));
}

async function fixture(t) {
  const directory = await mkdtemp(join(tmpdir(), 'verglas-graph-'));
  const loaded = await loadProject();
  const host = new GraphHost(join(directory, 'graph.sqlite'));
  const manifest = {
    bindings: [{ name: 'GRAPH_DO', class_name: 'Graph' }],
    vars: { GRAPH_NAME: 'knowledge' },
  };
  const handler = createHandler(loaded.project, manifest, { transport: host });
  await handler.init();
  t.after(() => {
    host.close();
    return Promise.all([
      rm(directory, { recursive: true, force: true }),
      rm(loaded.directory, { recursive: true, force: true }),
    ]);
  });
  return { handler, host, project: loaded.project, manifest };
}

test('graph CRUD, cyclic traversal, shortest path, and incident deletion are atomic', async (t) => {
  const { handler } = await fixture(t);
  const nodes = [
    { id: 'a', kind: 'person', properties: { rank: 1 } },
    { id: 'b', kind: 'person', properties: { rank: 2 } },
    { id: 'c', kind: 'person', properties: { rank: 3 } },
  ];
  const first = await json(await handler.fetch(request('/graph/upsert-nodes', { nodes })));
  assert.deepEqual(await json(await handler.fetch(request('/graph/upsert-nodes', { nodes }))), first);
  await handler.fetch(request('/graph/upsert-edges', { edges: [
    { id: 'ab', from: 'a', to: 'b', kind: 'knows' },
    { id: 'bc', from: 'b', to: 'c', kind: 'knows' },
    { id: 'ca', from: 'c', to: 'a', kind: 'knows' },
  ] }));
  const neighbors = await json(await handler.fetch(request('/graph/neighbors', {
    id: 'a', direction: 'out', depth: 2, returnNodes: true, returnEdges: true,
  })));
  assert.deepEqual(neighbors.nodes.map(({ id }) => id), ['b', 'c']);
  assert.deepEqual(neighbors.edges.map(({ id }) => id), ['ab', 'bc']);
  const path = await json(await handler.fetch(request('/graph/shortest-path', {
    from: 'a', to: 'c', direction: 'out', maxDepth: 3,
  })));
  assert.equal(path.found, true);
  assert.deepEqual(path.nodes.map(({ id }) => id), ['a', 'b', 'c']);
  assert.deepEqual(path.edges.map(({ id }) => id), ['ab', 'bc']);

  await handler.fetch(request('/graph/delete-nodes', { ids: ['b'] }));
  assert.deepEqual(await json(await handler.fetch(request('/graph/get-edges', { ids: ['ab', 'bc', 'ca'] }))), [
    { id: 'ca', from: 'c', to: 'a', kind: 'knows' },
  ]);
});

test('missing endpoints and invalid batches commit no partial graph state', async (t) => {
  const { handler, host } = await fixture(t);
  await handler.fetch(request('/graph/upsert-nodes', {
    nodes: [{ id: 'a', kind: 'person' }],
  }));
  const response = await handler.fetch(request('/graph/upsert-edges', { edges: [
    { id: 'valid', from: 'a', to: 'a', kind: 'self' },
    { id: 'invalid', from: 'a', to: 'missing', kind: 'knows' },
  ] }));
  assert.equal(response.status, 400);
  assert.equal(host.database.prepare('SELECT COUNT(*) AS count FROM graph_edges').get().count, 0);
});

test('declared node and edge property filters execute before expansion without SQL injection', async (t) => {
  const { handler } = await fixture(t);
  await handler.fetch(request('/graph/upsert-nodes', { nodes: [
    { id: 'a', kind: 'person', properties: { rank: 1, name: 'root' } },
    { id: 'b', kind: 'person', properties: { rank: 2, name: 'safe' } },
    { id: 'c', kind: 'person', properties: { rank: 3, name: 'other' } },
    { id: 'd', kind: 'person', properties: { rank: 4, name: 'leaf' } },
  ] }));
  await handler.fetch(request('/graph/upsert-edges', { edges: [
    { id: 'ab', from: 'a', to: 'b', kind: 'knows', properties: { trust: 0.9 } },
    { id: 'ac', from: 'a', to: 'c', kind: 'knows', properties: { trust: 0.7 } },
    { id: 'cd', from: 'c', to: 'd', kind: 'knows', properties: { trust: 0.95 } },
  ] }));
  const undeclared = await handler.fetch(request('/graph/neighbors', {
    id: 'a', nodeFilter: { rank: { $gte: 2 } },
  }));
  assert.equal(undeclared.status, 400);
  await handler.fetch(request('/graph/property-index/create', {
    scope: 'node', propertyName: 'rank', indexType: 'number',
  }));
  await handler.fetch(request('/graph/property-index/create', {
    scope: 'node', propertyName: 'name', indexType: 'string',
  }));
  await handler.fetch(request('/graph/property-index/create', {
    scope: 'edge', propertyName: 'trust', indexType: 'number',
  }));
  const filtered = await json(await handler.fetch(request('/graph/neighbors', {
    id: 'a', depth: 2, returnEdges: true,
    nodeFilter: { rank: { $gte: 2 } }, edgeFilter: { trust: { $gte: 0.8 } },
  })));
  assert.deepEqual(filtered.nodes.map(({ id }) => id), ['b']);
  assert.deepEqual(filtered.edges.map(({ id }) => id), ['ab']);
  const injected = await json(await handler.fetch(request('/graph/neighbors', {
    id: 'a', nodeFilter: { name: { $eq: "safe' OR 1=1 --" } },
  })));
  assert.deepEqual(injected.nodes, []);
  const indexes = await json(await handler.fetch(request('/graph/property-index/list')));
  assert.deepEqual(indexes.propertyIndexes, [
    { scope: 'edge', propertyName: 'trust', indexType: 'number' },
    { scope: 'node', propertyName: 'name', indexType: 'string' },
    { scope: 'node', propertyName: 'rank', indexType: 'number' },
  ]);
});

test('inbound and both-direction traversal are deterministic across restart', async (t) => {
  const { handler, host, project, manifest } = await fixture(t);
  await handler.fetch(request('/graph/upsert-nodes', { nodes: [
    { id: 'a', kind: 'node' }, { id: 'b', kind: 'node' },
    { id: 'c', kind: 'node' }, { id: 'd', kind: 'node' },
  ] }));
  await handler.fetch(request('/graph/upsert-edges', { edges: [
    { id: 'ba', from: 'b', to: 'a', kind: 'link' },
    { id: 'ac', from: 'a', to: 'c', kind: 'link' },
    { id: 'da', from: 'd', to: 'a', kind: 'link' },
  ] }));
  await handler.fetch(request('/graph/property-index/create', {
    scope: 'node', propertyName: 'rank', indexType: 'number',
  }));
  const inbound = await json(await handler.fetch(request('/graph/neighbors', {
    id: 'a', direction: 'in', returnEdges: true,
  })));
  assert.deepEqual(inbound.nodes.map(({ id }) => id), ['b', 'd']);
  const both = await json(await handler.fetch(request('/graph/neighbors', {
    id: 'a', direction: 'both', returnEdges: true,
  })));
  assert.deepEqual(both.nodes.map(({ id }) => id), ['b', 'c', 'd']);

  const restarted = createHandler(project, manifest, { transport: host });
  await restarted.init();
  assert.deepEqual(
    await json(await restarted.fetch(request('/graph/neighbors', { id: 'a', direction: 'in' }))),
    { nodes: inbound.nodes, edges: [], depthReached: 1 },
  );
  assert.equal(
    (await json(await restarted.fetch(request('/graph/property-index/list')))).propertyIndexes.length,
    1,
  );
  const changed = createHandler(project, {
    ...manifest,
    vars: { GRAPH_NAME: 'changed' },
  }, { transport: host });
  await assert.rejects(changed.init(), /configuration.*immutable/i);
});

test('traversal fails instead of returning partial results above the scanned-edge ceiling', async (t) => {
  const { handler, host } = await fixture(t);
  await handler.fetch(request('/graph/upsert-nodes', { nodes: [{ id: 'root', kind: 'node' }] }));
  host.database.exec(`WITH RECURSIVE sequence(value) AS (
    SELECT 1 UNION ALL SELECT value + 1 FROM sequence WHERE value <= 50000
  ) INSERT INTO graph_nodes (external_id, kind, mutation_id)
    SELECT 'n-' || value, 'node', 'seed' FROM sequence`);
  host.database.exec(`WITH RECURSIVE sequence(value) AS (
    SELECT 1 UNION ALL SELECT value + 1 FROM sequence WHERE value <= 50000
  ) INSERT INTO graph_edges (external_id, from_id, to_id, kind, mutation_id)
    SELECT 'e-' || value, 'root', 'n-' || value, 'link', 'seed' FROM sequence`);
  const response = await handler.fetch(request('/graph/neighbors', {
    id: 'root', limit: 1,
  }));
  assert.equal(response.status, 400);
  assert.match((await json(response)).error, /scanned edges/i);
});
