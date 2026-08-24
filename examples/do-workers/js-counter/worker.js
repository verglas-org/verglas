const COUNTER_ID = 'global';

function json(value, status = 200) {
  return {
    status,
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(value),
  };
}

function count(env) {
  const rows = env.sql("SELECT COUNT(*) AS count FROM counter WHERE id = 'global'");
  return Number(rows[0]?.count ?? 0);
}

export default {
  init(env) {
    env.sql(
      'CREATE TABLE IF NOT EXISTS counter (id VARCHAR NOT NULL, count BIGINT NOT NULL)',
    );
  },

  fetch(request, env) {
    const path = request.url.split('?', 1)[0];

    if (request.method === 'POST' && path === '/incr') {
      env.sql(`INSERT INTO counter (id, count) VALUES ('${COUNTER_ID}', 1)`);
      return json({ count: count(env) });
    }

    if (request.method === 'GET' && path === '/') {
      return json({ count: count(env) });
    }

    return json({ error: 'not found' }, 404);
  },

  webSocketMessage(socketId, message, env) {
    env.sockets.send(socketId, message);
    env.sockets.send(socketId, JSON.stringify({ count: count(env) }));
  },
};
