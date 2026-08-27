import { workerAssetPath } from '@verglas/worker-js/assets';
import { createHandler, createWorker } from '@verglas/worker-js/cloudflare-workers';
import { build as bundle } from 'esbuild';
import { DatabaseSync } from 'node:sqlite';
import { createServer } from 'node:http';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';

const root = resolve(new URL('..', import.meta.url).pathname);
const source = join(root, 'worker.js');
const cloudflareWorkersPath = workerAssetPath('cloudflare-workers.js');
const encoder = new TextEncoder();
const decoder = new TextDecoder();

class SqlHost {
  constructor() {
    this.database = new DatabaseSync(':memory:');
    this.catalogHandler = undefined;
    this.metadata = new Map();
  }

  sqlRows(statement) {
    const query = this.database.prepare(statement);
    if (/^\s*(CREATE|INSERT|UPDATE|DELETE|REPLACE|BEGIN|COMMIT|ROLLBACK)\b/iu.test(statement)) {
      query.run();
      return '[]';
    }
    return JSON.stringify(query.all());
  }

  doFetch(binding, object, request) {
    if (binding === 'CATALOG_DO') {
      if (!this.catalogHandler) throw new Error('Catalog handler is not attached');
      return this.catalogHandler.fetch(request);
    }
    if (binding === 'ICEBERG_COMMIT' && object === 'verglas-runtime') {
      return this.icebergFetch(request);
    }
    throw new Error(`unexpected binding ${binding} object ${object}`);
  }

  async icebergFetch(request) {
    const payload = JSON.parse(decoder.decode(request.body));
    if (payload.operation === 'commit-table') {
      const previous = this.metadata.get(payload.current_metadata_location);
      if (!previous) return response(404, { error: 'metadata not found' });
      const metadata = structuredClone(previous);
      const tableCommit = JSON.parse(payload.request_json);
      for (const update of tableCommit.updates) {
        if (update.action === 'set-properties') {
          metadata.properties = { ...(metadata.properties ?? {}), ...update.updates };
        } else if (update.action === 'remove-properties') {
          for (const key of update.removals) delete metadata.properties[key];
        } else {
          return response(400, { error: `unsupported fixture update ${update.action}` });
        }
      }
      metadata['last-updated-ms'] += 1;
      const metadataLocation = `${metadata.location}/metadata/00001-interop.json`;
      this.metadata.set(metadataLocation, metadata);
      return response(200, { 'metadata-location': metadataLocation, metadata });
    }
    if (payload.operation !== 'create-table') return response(400, { error: 'unknown operation' });

    const tableRequest = payload.request;
    const namespace = payload.namespace;
    const tableName = tableRequest.name;
    const location = tableRequest.location ?? `s3://lake/${namespace.join('/')}/${tableName}`;
    const schema = tableRequest.schema;
    const partitionSpec = normalizePartitionSpec(tableRequest['partition-spec']);
    const writeOrder = normalizeSortOrder(tableRequest['write-order']);
    const publication = {
      'metadata-location': `${location}/metadata/00000-interop.json`,
      metadata: {
        'format-version': 2,
        'table-uuid': '00000000-0000-4000-8000-000000000001',
        location,
        'last-updated-ms': 1,
        'last-column-id': highestFieldId(schema),
        schemas: [schema],
        'current-schema-id': schema['schema-id'] ?? 0,
        'partition-specs': [partitionSpec],
        'default-spec-id': partitionSpec['spec-id'],
        'last-partition-id': highestPartitionFieldId(partitionSpec),
        'sort-orders': [writeOrder],
        'default-sort-order-id': writeOrder['order-id'],
        properties: tableRequest.properties ?? {},
        'current-snapshot-id': null,
        snapshots: [],
        refs: {},
        'last-sequence-number': 0,
        'snapshot-log': [],
        'metadata-log': [],
      },
    };
    this.metadata.set(publication['metadata-location'], publication.metadata);
    return response(200, publication);
  }

  close() {
    this.database.close();
  }
}

function normalizePartitionSpec(value) {
  if (!value) return { 'spec-id': 0, fields: [] };
  if (Array.isArray(value)) return { 'spec-id': 0, fields: value };
  return {
    'spec-id': value['spec-id'] ?? 0,
    fields: value.fields ?? [],
  };
}

function normalizeSortOrder(value) {
  if (!value) return { 'order-id': 0, fields: [] };
  if (Array.isArray(value)) return { 'order-id': 0, fields: value };
  return {
    'order-id': value['order-id'] ?? 0,
    fields: value.fields ?? [],
  };
}

function highestFieldId(schema) {
  const fields = schema?.fields;
  if (!Array.isArray(fields)) return 0;
  return fields.reduce((highest, field) => Math.max(highest, Number(field.id ?? 0)), 0);
}

function highestPartitionFieldId(spec) {
  const fields = spec.fields;
  if (!Array.isArray(fields)) return 999;
  return fields.reduce((highest, field) => Math.max(highest, Number(field['field-id'] ?? 999)), 999);
}

function response(status, value) {
  const body = typeof value === 'string' ? value : JSON.stringify(value);
  return { status, headers: [['content-type', 'application/json']], body: encoder.encode(body) };
}

function manifest() {
  return {
    bindings: [{ name: 'CATALOG_DO', class_name: 'Catalog' }],
    services: [{ binding: 'ICEBERG_COMMIT', service: 'verglas-runtime' }],
    vars: {
      CATALOG_ID: 'interop',
      CATALOG_WAREHOUSE: 'warehouse',
      CATALOG_BUCKET: 'lake',
      CATALOG_NAMESPACE: 'interop',
      CATALOG_TABLE: 'events',
      CATALOG_SINK_ID: 'primary',
    },
  };
}

async function loadProject() {
  const result = await bundle({
    entryPoints: [source],
    bundle: true,
    format: 'esm',
    platform: 'node',
    write: false,
    alias: { 'cloudflare:workers': cloudflareWorkersPath },
  });
  const directory = await mkdtemp(join(tmpdir(), 'verglas-catalog-interop-bundle-'));
  const path = join(directory, 'worker.mjs');
  await writeFile(path, result.outputFiles[0].text, 'utf8');
  const project = await import(`${pathToFileURL(path).href}?${Date.now()}-${Math.random()}`);
  return { directory, project };
}

function enqueue(queue, operation) {
  const result = queue.then(operation, operation);
  return result.catch((error) => {
    throw error;
  });
}

async function main() {
  const loaded = await loadProject();
  const host = new SqlHost();
  const handler = createHandler(loaded.project, manifest(), { transport: host });
  await handler.init();
  host.catalogHandler = handler;
  const worker = createWorker(loaded.project, manifest(), { transport: host });
  let queue = Promise.resolve();
  const server = createServer((request, client) => {
    const operation = async () => {
      const chunks = [];
      for await (const chunk of request) chunks.push(chunk);
      const body = Buffer.concat(chunks);
      const hostHeader = request.headers.host ?? 'catalog.invalid';
      const record = {
        method: request.method ?? 'GET',
        uri: `http://${hostHeader}${request.url ?? '/'}`,
        headers: Object.entries(request.headers).flatMap(([name, value]) => {
          if (Array.isArray(value)) return value.map((item) => [name, item]);
          return [[name, value ?? '']];
        }),
        body: new Uint8Array(body),
        ws: undefined,
      };
      const result = await worker.fetch(record);
      client.statusCode = result.status;
      for (const [name, value] of result.headers) {
        if (name.toLowerCase() !== 'content-length') client.setHeader(name, value);
      }
      client.end(Buffer.from(result.body));
    };
    queue = enqueue(queue, operation).catch((error) => {
      console.error(error.stack ?? error);
      if (!client.headersSent) client.statusCode = 500;
      client.end('interoperability adapter failure');
    });
  });

  const shutdown = async () => {
    server.close();
    host.close();
    await rm(loaded.directory, { recursive: true, force: true });
  };
  process.once('SIGTERM', () => {
    void shutdown().finally(() => process.exit(0));
  });
  process.once('SIGINT', () => {
    void shutdown().finally(() => process.exit(130));
  });
  server.on('error', (error) => {
    console.error(error.stack ?? error);
    process.exitCode = 1;
  });
  server.listen(0, '127.0.0.1', () => {
    const address = server.address();
    if (!address || typeof address === 'string') throw new Error('interoperability adapter did not bind a TCP port');
    process.stdout.write(`${JSON.stringify({ ready: true, url: `http://127.0.0.1:${address.port}` })}\n`);
  });
}

await main();
