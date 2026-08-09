import { SQL } from "bun";

function json(value) {
  return JSON.stringify(value);
}

function parseJson(value) {
  return typeof value === "string" ? JSON.parse(value) : value;
}

function decodeChat(row) {
  return row ? {
    ...row,
    model_profile: parseJson(row.model_profile),
    model_config: parseJson(row.model_config),
  } : null;
}

function decodeMessage(row) {
  return row ? {...row, author: parseJson(row.author), body: parseJson(row.body)} : null;
}

export class AgentStore {
  constructor(databaseUrl) {
    this.sql = new SQL(databaseUrl, { max: 8 });
  }

  async migrate() {
    await this.sql.unsafe(`
      CREATE TABLE IF NOT EXISTS verglas_agent_workspaces (
        id TEXT PRIMARY KEY,
        tenant_id TEXT NOT NULL,
        owner_id TEXT NOT NULL,
        title TEXT NOT NULL,
        pinned BOOLEAN NOT NULL DEFAULT FALSE,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
      );
      CREATE TABLE IF NOT EXISTS verglas_agent_chats (
        workspace_id TEXT NOT NULL REFERENCES verglas_agent_workspaces(id) ON DELETE CASCADE,
        id BIGINT NOT NULL,
        title TEXT NOT NULL,
        model_profile JSONB,
        model_config JSONB,
        active BOOLEAN NOT NULL DEFAULT FALSE,
        started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (workspace_id, id)
      );
      CREATE TABLE IF NOT EXISTS verglas_agent_messages (
        workspace_id TEXT NOT NULL,
        chat_id BIGINT NOT NULL,
        sequence BIGINT NOT NULL,
        timestamp TIMESTAMPTZ NOT NULL DEFAULT now(),
        author JSONB NOT NULL,
        body JSONB NOT NULL,
        PRIMARY KEY (workspace_id, chat_id, sequence),
        FOREIGN KEY (workspace_id, chat_id)
          REFERENCES verglas_agent_chats(workspace_id, id) ON DELETE CASCADE
      );
      CREATE INDEX IF NOT EXISTS verglas_agent_messages_time
        ON verglas_agent_messages(workspace_id, timestamp);
      CREATE TABLE IF NOT EXISTS verglas_agent_runs (
        id TEXT PRIMARY KEY,
        workspace_id TEXT NOT NULL,
        chat_id BIGINT NOT NULL,
        principal_id TEXT NOT NULL,
        token_hash TEXT,
        state TEXT NOT NULL,
        error TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        started_at TIMESTAMPTZ,
        completed_at TIMESTAMPTZ,
        cleaned_at TIMESTAMPTZ,
        FOREIGN KEY (workspace_id, chat_id)
          REFERENCES verglas_agent_chats(workspace_id, id) ON DELETE CASCADE
      );
      ALTER TABLE verglas_agent_runs ADD COLUMN IF NOT EXISTS token_hash TEXT;
      ALTER TABLE verglas_agent_runs ADD COLUMN IF NOT EXISTS cleaned_at TIMESTAMPTZ;
      UPDATE verglas_agent_chats
        SET model_profile = (model_profile #>> '{}')::jsonb
        WHERE jsonb_typeof(model_profile) = 'string';
      UPDATE verglas_agent_chats
        SET model_config = (model_config #>> '{}')::jsonb
        WHERE jsonb_typeof(model_config) = 'string';
      UPDATE verglas_agent_messages
        SET author = (author #>> '{}')::jsonb
        WHERE jsonb_typeof(author) = 'string';
      UPDATE verglas_agent_messages
        SET body = (body #>> '{}')::jsonb
        WHERE jsonb_typeof(body) = 'string';
    `);
  }

  async createWorkspace({ id, tenantId, ownerId, title }) {
    const rows = await this.sql`
      INSERT INTO verglas_agent_workspaces (id, tenant_id, owner_id, title)
      VALUES (${id}, ${tenantId}, ${ownerId}, ${title})
      ON CONFLICT (id) DO NOTHING
      RETURNING *
    `;
    return rows[0] ?? this.getWorkspace(id, ownerId);
  }

  async getWorkspace(id, userId) {
    const rows = await this.sql`
      SELECT * FROM verglas_agent_workspaces WHERE id = ${id} AND owner_id = ${userId}
    `;
    return rows[0] ?? null;
  }

  async updateWorkspace(id, ownerId, patch) {
    const existing = await this.getWorkspace(id, ownerId);
    if (!existing) return null;
    const title = typeof patch.title === "string" ? patch.title : existing.title;
    const pinned = typeof patch.pinned === "boolean" ? patch.pinned : existing.pinned;
    const rows = await this.sql`
      UPDATE verglas_agent_workspaces
      SET title = ${title}, pinned = ${pinned}, updated_at = now()
      WHERE id = ${id} AND owner_id = ${ownerId}
      RETURNING *
    `;
    return rows[0] ?? null;
  }

  async deleteWorkspace(id, ownerId) {
    await this.sql`DELETE FROM verglas_agent_workspaces WHERE id = ${id} AND owner_id = ${ownerId}`;
  }

  async createChat({ workspaceId, profile, modelProfile, modelConfig, prompt }) {
    return this.sql.begin(async transaction => {
      await transaction`SELECT pg_advisory_xact_lock(hashtext(${workspaceId}))`;
      const idRows = await transaction`
        SELECT COALESCE(MAX(id), -1) + 1 AS id
        FROM verglas_agent_chats WHERE workspace_id = ${workspaceId}
      `;
      const chatId = Number(idRows[0].id);
      await transaction`
        INSERT INTO verglas_agent_chats
          (workspace_id, id, title, model_profile, model_config, active)
        VALUES (
          ${workspaceId}, ${chatId}, 'New Chat',
          ${modelProfile ? json(modelProfile) : null}::text::jsonb,
          ${modelConfig ? json(modelConfig) : null}::text::jsonb,
          ${Boolean(modelConfig)}
        )
      `;
      await transaction`
        INSERT INTO verglas_agent_messages
          (workspace_id, chat_id, sequence, author, body)
        VALUES (
          ${workspaceId}, ${chatId}, 0,
          ${json(profile)}::text::jsonb,
          ${json({ type: "message", message: prompt })}::text::jsonb
        )
      `;
      return chatId;
    });
  }

  async appendUserMessage({ workspaceId, chatId, profile, modelProfile, modelConfig, prompt }) {
    await this.sql.begin(async transaction => {
      await transaction`
        UPDATE verglas_agent_chats SET
          model_profile = ${modelProfile ? json(modelProfile) : null}::text::jsonb,
          model_config = ${modelConfig ? json(modelConfig) : null}::text::jsonb,
          active = ${Boolean(modelConfig)}, updated_at = now()
        WHERE workspace_id = ${workspaceId} AND id = ${chatId}
      `;
      const sequence = await this.nextSequence(transaction, workspaceId, chatId);
      await transaction`
        INSERT INTO verglas_agent_messages
          (workspace_id, chat_id, sequence, author, body)
        VALUES (
          ${workspaceId}, ${chatId}, ${sequence},
          ${json(profile)}::text::jsonb,
          ${json({ type: "message", message: prompt })}::text::jsonb
        )
      `;
    });
  }

  async nextSequence(transaction, workspaceId, chatId) {
    await transaction`
      SELECT pg_advisory_xact_lock(hashtext(${`${workspaceId}:${chatId}`}))
    `;
    const rows = await transaction`
      SELECT COALESCE(MAX(sequence), -1) + 1 AS sequence
      FROM verglas_agent_messages
      WHERE workspace_id = ${workspaceId} AND chat_id = ${chatId}
    `;
    return Number(rows[0].sequence);
  }

  async createRun({ id, workspaceId, chatId, principalId, tokenHash }) {
    await this.sql`
      INSERT INTO verglas_agent_runs
        (id, workspace_id, chat_id, principal_id, token_hash, state)
      VALUES (${id}, ${workspaceId}, ${chatId}, ${principalId}, ${tokenHash}, 'pending')
    `;
  }

  async claimRun(id) {
    const rows = await this.sql`
      UPDATE verglas_agent_runs SET state = 'running', started_at = now()
      WHERE id = ${id} AND state = 'pending'
      RETURNING *
    `;
    return rows[0] ? await this.getRun(id) : null;
  }

  async getRun(id) {
    const rows = await this.sql`
      SELECT r.*, c.model_profile, c.model_config
      FROM verglas_agent_runs r
      JOIN verglas_agent_chats c ON c.workspace_id = r.workspace_id AND c.id = r.chat_id
      WHERE r.id = ${id}
    `;
    return rows[0] ? decodeChat(rows[0]) : null;
  }

  async getGatewayRun(id) {
    const rows = await this.sql`
      SELECT id, workspace_id, chat_id, principal_id, token_hash, state
      FROM verglas_agent_runs WHERE id = ${id}
    `;
    return rows[0] ?? null;
  }

  async finishRun(id, error = null) {
    await this.sql.begin(async transaction => {
      const rows = await transaction`
        UPDATE verglas_agent_runs
        SET state = ${error ? "failed" : "completed"}, error = ${error}, completed_at = now()
        WHERE id = ${id} AND state IN ('pending', 'running')
        RETURNING workspace_id, chat_id
      `;
      if (rows[0]) {
        await transaction`
          UPDATE verglas_agent_chats SET active = FALSE, updated_at = now()
          WHERE workspace_id = ${rows[0].workspace_id} AND id = ${rows[0].chat_id}
        `;
      }
    });
  }

  async listRunsForCleanup(limit = 50) {
    return await this.sql`
      SELECT id FROM verglas_agent_runs
      WHERE state IN ('completed', 'failed', 'cancelled') AND cleaned_at IS NULL
      ORDER BY completed_at ASC LIMIT ${limit}
    `;
  }

  async listActiveRuns(limit = 50) {
    return await this.sql`
      SELECT id FROM verglas_agent_runs
      WHERE state = 'running' ORDER BY started_at ASC LIMIT ${limit}
    `;
  }

  async markRunCleaned(id) {
    await this.sql`UPDATE verglas_agent_runs SET cleaned_at = now() WHERE id = ${id}`;
  }

  async cancelActiveRun(workspaceId, chatId) {
    const rows = await this.sql`
      UPDATE verglas_agent_runs
      SET state = 'cancelled', completed_at = now()
      WHERE workspace_id = ${workspaceId} AND chat_id = ${chatId}
        AND state IN ('pending', 'running')
      RETURNING id
    `;
    await this.sql`
      UPDATE verglas_agent_chats SET active = FALSE, updated_at = now()
      WHERE workspace_id = ${workspaceId} AND id = ${chatId}
    `;
    return rows.map(row => row.id);
  }

  async appendAssistantMessage(workspaceId, chatId, author, body) {
    await this.sql.begin(async transaction => {
      const sequence = await this.nextSequence(transaction, workspaceId, chatId);
      await transaction`
        INSERT INTO verglas_agent_messages
          (workspace_id, chat_id, sequence, author, body)
        VALUES (
          ${workspaceId}, ${chatId}, ${sequence},
          ${json(author)}::text::jsonb, ${json(body)}::text::jsonb
        )
      `;
      await transaction`
        UPDATE verglas_agent_chats SET updated_at = now()
        WHERE workspace_id = ${workspaceId} AND id = ${chatId}
      `;
    });
  }

  async listChats(workspaceId) {
    const rows = await this.sql`
      SELECT * FROM verglas_agent_chats WHERE workspace_id = ${workspaceId}
      ORDER BY updated_at DESC
    `;
    return rows.map(decodeChat);
  }

  async listMessages(workspaceId, chatId, afterSequence = -1) {
    const rows = await this.sql`
      SELECT * FROM verglas_agent_messages
      WHERE workspace_id = ${workspaceId} AND chat_id = ${chatId}
        AND sequence > ${afterSequence}
      ORDER BY sequence ASC
    `;
    return rows.map(decodeMessage);
  }

  async getPermissionRequest(workspaceId, requestId) {
    const rows = await this.sql`
      SELECT * FROM verglas_agent_messages
      WHERE workspace_id = ${workspaceId}
        AND body->>'type' = 'permissionRequest'
        AND body->>'requestId' = ${requestId}
      LIMIT 1
    `;
    return decodeMessage(rows[0]);
  }

  async decidePermissionRequest(workspaceId, requestId, state) {
    const rows = await this.sql`
      UPDATE verglas_agent_messages
      SET body = jsonb_set(body, '{state}', to_jsonb(${state}::text)), timestamp = now()
      WHERE workspace_id = ${workspaceId}
        AND body->>'type' = 'permissionRequest'
        AND body->>'requestId' = ${requestId}
        AND body->>'state' = 'pending'
      RETURNING *
    `;
    return decodeMessage(rows[0]);
  }

  async historyForModel(workspaceId, chatId) {
    const rows = await this.sql`
      SELECT author, body FROM verglas_agent_messages
      WHERE workspace_id = ${workspaceId} AND chat_id = ${chatId}
      ORDER BY sequence ASC
    `;
    return rows.map(decodeMessage);
  }

  async setChatTitle(workspaceId, chatId, title) {
    await this.sql`
      UPDATE verglas_agent_chats SET title = ${title}, updated_at = now()
      WHERE workspace_id = ${workspaceId} AND id = ${chatId}
    `;
  }

  async setChatModel(workspaceId, chatId, modelProfile, modelConfig) {
    const rows = await this.sql`
      UPDATE verglas_agent_chats SET
        model_profile = ${modelProfile ? json(modelProfile) : null}::text::jsonb,
        model_config = ${modelConfig ? json(modelConfig) : null}::text::jsonb,
        active = ${Boolean(modelConfig)}, updated_at = now()
      WHERE workspace_id = ${workspaceId} AND id = ${chatId}
      RETURNING id
    `;
    return rows[0] ?? null;
  }

  async deleteChat(workspaceId, chatId) {
    await this.sql`
      DELETE FROM verglas_agent_chats WHERE workspace_id = ${workspaceId} AND id = ${chatId}
    `;
  }
}
