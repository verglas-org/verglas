import { DurableObject } from 'cloudflare:workers';

export class Counter extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    void ctx.blockConcurrencyWhile(async () => {
      this.ctx.storage.sql.exec(
        'CREATE TABLE IF NOT EXISTS counter (id VARCHAR NOT NULL, count BIGINT NOT NULL)',
      );
    });
  }

  async fetch(request) {
    const url = new URL(request.url);
    if (request.method === 'POST' && url.pathname === '/incr') {
      this.ctx.storage.sql.exec("INSERT INTO counter (id, count) VALUES ('global', 1)");
      return this.jsonCount();
    }
    if (request.method === 'GET' && url.pathname === '/') return this.jsonCount();
    return new Response(JSON.stringify({ error: 'not found' }), {
      status: 404,
      headers: { 'content-type': 'application/json' },
    });
  }

  jsonCount() {
    const row = this.ctx.storage.sql.exec(
      "SELECT COUNT(*) AS count FROM counter WHERE id = 'global'",
    ).one();
    return new Response(JSON.stringify({ count: Number(row.count ?? 0) }), {
      headers: { 'content-type': 'application/json' },
    });
  }
}

export default {
  async fetch(request, env, ctx) {
    const id = env.COUNTER.idFromName('global');
    return env.COUNTER.get(id).fetch(request);
  },
};
