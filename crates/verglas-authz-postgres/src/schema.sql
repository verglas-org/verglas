CREATE SCHEMA IF NOT EXISTS verglas_authz;

CREATE TABLE IF NOT EXISTS verglas_authz.principals (
    tenant_id TEXT NOT NULL,
    id TEXT NOT NULL,
    kind TEXT NOT NULL,
    parent_id TEXT,
    PRIMARY KEY (tenant_id, id),
    FOREIGN KEY (tenant_id, parent_id)
        REFERENCES verglas_authz.principals (tenant_id, id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS verglas_authz.resources (
    tenant_id TEXT NOT NULL,
    id TEXT NOT NULL,
    kind TEXT NOT NULL,
    parent_id TEXT,
    PRIMARY KEY (tenant_id, id),
    FOREIGN KEY (tenant_id, parent_id)
        REFERENCES verglas_authz.resources (tenant_id, id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS verglas_authz.grants (
    tenant_id TEXT NOT NULL,
    id TEXT NOT NULL,
    principal_id TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    PRIMARY KEY (tenant_id, id),
    FOREIGN KEY (tenant_id, principal_id)
        REFERENCES verglas_authz.principals (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, resource_id)
        REFERENCES verglas_authz.resources (tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS grants_principal_resource
    ON verglas_authz.grants (tenant_id, principal_id, resource_id);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'grants_one_principal_resource'
          AND connamespace = 'verglas_authz'::regnamespace
    ) THEN
        ALTER TABLE verglas_authz.grants
            ADD CONSTRAINT grants_one_principal_resource
            UNIQUE (tenant_id, principal_id, resource_id);
    END IF;
END
$$;

CREATE TABLE IF NOT EXISTS verglas_authz.grant_actions (
    tenant_id TEXT NOT NULL,
    grant_id TEXT NOT NULL,
    action TEXT NOT NULL,
    PRIMARY KEY (tenant_id, grant_id, action),
    FOREIGN KEY (tenant_id, grant_id)
        REFERENCES verglas_authz.grants (tenant_id, id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS verglas_authz.policy_versions (
    tenant_id TEXT PRIMARY KEY,
    version BIGINT NOT NULL CHECK (version >= 0)
);

-- Token rows intentionally contain no bearer value, signature, or signing key.
CREATE TABLE IF NOT EXISTS verglas_authz.access_tokens (
    tenant_id TEXT NOT NULL,
    id TEXT NOT NULL,
    principal_id TEXT NOT NULL,
    parent_principal_id TEXT NOT NULL,
    name TEXT NOT NULL,
    audience TEXT NOT NULL,
    policy_version BIGINT NOT NULL CHECK (policy_version >= 0),
    run_id TEXT,
    created_at BIGINT NOT NULL CHECK (created_at >= 0),
    expires_at BIGINT NOT NULL CHECK (expires_at > created_at),
    last_used_at BIGINT,
    revoked_at BIGINT,
    PRIMARY KEY (tenant_id, id),
    FOREIGN KEY (tenant_id, principal_id)
        REFERENCES verglas_authz.principals (tenant_id, id) ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, parent_principal_id)
        REFERENCES verglas_authz.principals (tenant_id, id) ON DELETE RESTRICT,
    CHECK (principal_id <> parent_principal_id),
    CHECK (last_used_at IS NULL OR (last_used_at >= created_at AND last_used_at <= expires_at)),
    CHECK (revoked_at IS NULL OR revoked_at >= created_at)
);

CREATE INDEX IF NOT EXISTS access_tokens_parent_inventory
    ON verglas_authz.access_tokens (tenant_id, parent_principal_id, created_at DESC, id);

CREATE SCHEMA IF NOT EXISTS verglas_secrets;

CREATE TABLE IF NOT EXISTS verglas_secrets.secrets (
    tenant_id TEXT NOT NULL,
    id TEXT NOT NULL,
    kind TEXT NOT NULL,
    scope TEXT NOT NULL,
    current_version BIGINT NOT NULL CHECK (current_version > 0),
    PRIMARY KEY (tenant_id, id),
    FOREIGN KEY (tenant_id, id)
        REFERENCES verglas_authz.resources (tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS secrets_scope_lookup
    ON verglas_secrets.secrets (tenant_id, kind, scope);

CREATE TABLE IF NOT EXISTS verglas_secrets.secret_versions (
    tenant_id TEXT NOT NULL,
    secret_id TEXT NOT NULL,
    version BIGINT NOT NULL CHECK (version > 0),
    ciphertext BYTEA NOT NULL,
    PRIMARY KEY (tenant_id, secret_id, version),
    FOREIGN KEY (tenant_id, secret_id)
        REFERENCES verglas_secrets.secrets (tenant_id, id) ON DELETE CASCADE
);
