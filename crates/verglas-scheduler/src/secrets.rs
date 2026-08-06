//! Postgres-backed secret bindings resolved only by the worker scheduler.

use sqlx::Row;
use sqlx::postgres::{PgPool, PgPoolOptions};

use crate::SchedulerError;

/// Durable scheduler-owned secret values for local worker execution.
#[derive(Clone)]
pub struct PgSecretStore {
    pool: PgPool,
}

impl PgSecretStore {
    /// Connects to Postgres and creates the secret table when absent.
    pub async fn connect(database_url: &str) -> Result<Self, SchedulerError> {
        let pool = PgPoolOptions::new()
            .max_connections(4)
            .connect(database_url)
            .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS verglas_scheduler_secrets (\
             name TEXT PRIMARY KEY, value TEXT NOT NULL, \
             updated_at TIMESTAMPTZ NOT NULL DEFAULT now())",
        )
        .execute(&pool)
        .await?;
        Ok(Self { pool })
    }

    /// Creates or replaces one secret without exposing its value afterward.
    pub async fn put(&self, name: &str, value: &str) -> Result<(), SchedulerError> {
        validate_secret_name(name)?;
        if value.is_empty() {
            return Err(SchedulerError::Invalid(
                "secret value must not be empty".to_owned(),
            ));
        }
        sqlx::query(
            "INSERT INTO verglas_scheduler_secrets (name, value, updated_at) \
             VALUES ($1, $2, now()) ON CONFLICT (name) DO UPDATE \
             SET value=EXCLUDED.value, updated_at=now()",
        )
        .bind(name)
        .bind(value)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Resolves one value for injection into a worker subprocess.
    pub async fn get(&self, name: &str) -> Result<Option<String>, SchedulerError> {
        validate_secret_name(name)?;
        let row = sqlx::query("SELECT value FROM verglas_scheduler_secrets WHERE name=$1")
            .bind(name)
            .fetch_optional(&self.pool)
            .await?;
        row.map(|row| row.try_get("value"))
            .transpose()
            .map_err(Into::into)
    }

    /// Lists configured names without returning secret values.
    pub async fn names(&self) -> Result<Vec<String>, SchedulerError> {
        let rows = sqlx::query("SELECT name FROM verglas_scheduler_secrets ORDER BY name")
            .fetch_all(&self.pool)
            .await?;
        rows.iter()
            .map(|row| row.try_get("name").map_err(Into::into))
            .collect()
    }

    /// Removes one secret and reports whether it existed.
    pub async fn delete(&self, name: &str) -> Result<bool, SchedulerError> {
        validate_secret_name(name)?;
        let result = sqlx::query("DELETE FROM verglas_scheduler_secrets WHERE name=$1")
            .bind(name)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected() == 1)
    }
}

fn validate_secret_name(name: &str) -> Result<(), SchedulerError> {
    if name.is_empty()
        || name.len() > 255
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
    {
        return Err(SchedulerError::Invalid(
            "secret name must contain only letters, numbers, '.', '-', or '_'".to_owned(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secret_names_are_bounded_path_like_identifiers() {
        assert!(validate_secret_name("workspace.7.source.ais.API_TOKEN").is_ok());
        assert!(validate_secret_name("").is_err());
        assert!(validate_secret_name("source token").is_err());
        assert!(validate_secret_name("../token").is_err());
        assert!(validate_secret_name("token:$VALUE").is_err());
    }
}
