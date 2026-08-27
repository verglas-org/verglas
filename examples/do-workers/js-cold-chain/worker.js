import { DurableObject } from 'cloudflare:workers';

const COUNTER_NAME = 'global';

/**
 * Publishes one ordered Stream record from the same event as the durable count.
 */
export class Counter extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.ready = ctx.blockConcurrencyWhile(async () => {
      this.ctx.storage.sql.exec(
        'CREATE TABLE IF NOT EXISTS counter (id TEXT NOT NULL, count INTEGER NOT NULL)',
      );
    });
  }

  async fetch(request) {
    await this.ready;
    const url = new URL(request.url);
    if (request.method === 'POST' && url.pathname === '/incr') {
      this.ctx.storage.sql.exec(
        "INSERT INTO counter (id, count) VALUES ('global', 1)",
      );
      await this.env.STREAM.send([{
        kind: 'increment',
        payload: COUNTER_NAME,
      }]);
      return this.jsonCount();
    }
    if (request.method === 'GET' && url.pathname === '/') return this.jsonCount();
    return Response.json({ error: 'not found' }, { status: 404 });
  }

  jsonCount() {
    const row = this.ctx.storage.sql.exec(
      "SELECT COUNT(*) AS count FROM counter WHERE id = 'global'",
    ).one();
    return Response.json({ count: Number(row.count ?? 0) });
  }
}

export default {
  async scheduled(_controller, env) {
    const id = env.COUNTER.idFromName(COUNTER_NAME);
    const response = await env.COUNTER.get(id).fetch(new Request(
      'https://verglas.internal/incr',
      { method: 'POST' },
    ));
    if (!response.ok) throw new Error(`scheduled increment failed: ${response.status}`);
  },

  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === '/incr' || url.pathname === '/') {
      const id = env.COUNTER.idFromName(COUNTER_NAME);
      return env.COUNTER.get(id).fetch(request);
    }
    if (url.pathname === '/process' && request.method === 'POST') {
      return env.PIPELINE.fetch(new Request(
        'https://verglas.internal/pipeline/enqueue',
        { method: 'POST' },
      ));
    }
    if (url.pathname === '/pipeline-status' && request.method === 'GET') {
      return env.PIPELINE.fetch(new Request(
        'https://verglas.internal/pipeline/status',
        { method: 'GET' },
      ));
    }
    if (url.pathname === '/sink-status' && request.method === 'GET') {
      return env.SINK_A.fetch(new Request(
        'https://verglas.internal/sink/status',
        { method: 'GET' },
      ));
    }
    if (url.pathname === '/catalog-status' && request.method === 'GET') {
      return env.CATALOG.fetch(new Request(
        'https://verglas.internal/catalog/status',
        { method: 'GET' },
      ));
    }
    return Response.json({ error: 'not found' }, { status: 404 });
  },
};
