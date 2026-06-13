//! PostgreSQL schema management for namespace isolation.
//!
//! Each PangeaNamespace gets its own PostgreSQL schema, providing
//! complete isolation of state data between namespaces.

use crate::error::{Error, Result};
use sqlx::PgPool;
use std::sync::Arc;
use tracing::{debug, info, warn};

/// Manages PostgreSQL schemas for Pangea namespaces.
#[derive(Clone)]
pub struct SchemaManager {
    pool: Arc<PgPool>,
}

impl SchemaManager {
    /// Create a new schema manager.
    pub fn new(pool: Arc<PgPool>) -> Self {
        Self { pool }
    }

    /// Ensure a schema exists for the given namespace.
    pub async fn ensure_schema(&self, schema_name: &str) -> Result<()> {
        // Validate schema name (prevent SQL injection)
        if !is_valid_identifier(schema_name) {
            return Err(Error::Config(format!("Invalid schema name: {}", schema_name)));
        }

        debug!(schema_name, "Ensuring schema exists");

        // Create schema if not exists
        let query = format!("CREATE SCHEMA IF NOT EXISTS {}", schema_name);
        sqlx::query(&query)
            .execute(self.pool.as_ref())
            .await
            .map_err(|e| Error::Database(e))?;

        info!(schema_name, "Schema ready");
        Ok(())
    }

    /// Create the state table for a template within a schema.
    ///
    /// This follows OpenTofu's pg backend table structure.
    pub async fn ensure_state_table(&self, schema_name: &str, template_name: &str) -> Result<()> {
        if !is_valid_identifier(schema_name) || !is_valid_identifier(template_name) {
            return Err(Error::Config("Invalid schema or template name".into()));
        }

        let table_name = format!("{}.{}_states", schema_name, template_name);

        debug!(table_name, "Ensuring state table exists");

        // Create table matching OpenTofu pg backend format
        let create_table = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {} (
                id BIGSERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL DEFAULT 'default',
                data BYTEA,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            "#,
            table_name
        );

        sqlx::query(&create_table)
            .execute(self.pool.as_ref())
            .await
            .map_err(|e| Error::Database(e))?;

        // Create unique index on name
        let create_index = format!(
            "CREATE UNIQUE INDEX IF NOT EXISTS {}_name_idx ON {} (name)",
            template_name,
            table_name
        );

        sqlx::query(&create_index)
            .execute(self.pool.as_ref())
            .await
            .map_err(|e| Error::Database(e))?;

        info!(table_name, "State table ready");
        Ok(())
    }

    /// Check if a schema exists.
    pub async fn schema_exists(&self, schema_name: &str) -> Result<bool> {
        let row: (bool,) = sqlx::query_as(
            "SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = $1)",
        )
        .bind(schema_name)
        .fetch_one(self.pool.as_ref())
        .await
        .map_err(|e| Error::Database(e))?;

        Ok(row.0)
    }

    /// List all schemas managed by Pangea (matching prefix).
    pub async fn list_schemas(&self, prefix: &str) -> Result<Vec<String>> {
        let pattern = format!("{}%", prefix);

        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT schema_name FROM information_schema.schemata WHERE schema_name LIKE $1",
        )
        .bind(&pattern)
        .fetch_all(self.pool.as_ref())
        .await
        .map_err(|e| Error::Database(e))?;

        Ok(rows.into_iter().map(|(name,)| name).collect())
    }

    /// Drop a schema and all its contents.
    ///
    /// **Warning**: This is destructive and cannot be undone.
    pub async fn drop_schema(&self, schema_name: &str) -> Result<()> {
        if !is_valid_identifier(schema_name) {
            return Err(Error::Config(format!("Invalid schema name: {}", schema_name)));
        }

        warn!(schema_name, "Dropping schema (destructive operation)");

        let query = format!("DROP SCHEMA IF EXISTS {} CASCADE", schema_name);
        sqlx::query(&query)
            .execute(self.pool.as_ref())
            .await
            .map_err(|e| Error::Database(e))?;

        info!(schema_name, "Schema dropped");
        Ok(())
    }

    /// Get table count in a schema.
    pub async fn table_count(&self, schema_name: &str) -> Result<i64> {
        let row: (i64,) = sqlx::query_as(
            r#"
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = $1 AND table_type = 'BASE TABLE'
            "#,
        )
        .bind(schema_name)
        .fetch_one(self.pool.as_ref())
        .await
        .map_err(|e| Error::Database(e))?;

        Ok(row.0)
    }
}

/// Validate a PostgreSQL identifier (schema/table name).
///
/// `pub(crate)` so other backend modules (e.g. `artifact_store`) can
/// reuse the one canonical identifier guard rather than re-implementing
/// it — the SQL-injection defense lives in exactly one place.
pub(crate) fn is_valid_identifier(name: &str) -> bool {
    if name.is_empty() || name.len() > 63 {
        return false;
    }

    let first = name.chars().next().unwrap();
    if !first.is_ascii_lowercase() && first != '_' {
        return false;
    }

    name.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_identifier() {
        assert!(is_valid_identifier("pangea_production"));
        assert!(is_valid_identifier("my_schema"));
        assert!(is_valid_identifier("_private"));

        assert!(!is_valid_identifier("")); // Empty
        assert!(!is_valid_identifier("1starts_with_number"));
        assert!(!is_valid_identifier("has-dashes"));
        assert!(!is_valid_identifier("HasCaps"));
        assert!(!is_valid_identifier("has spaces"));
        assert!(!is_valid_identifier("has;semicolon"));
    }

    #[test]
    fn test_valid_identifier_max_length() {
        let name = "a".repeat(63);
        assert!(is_valid_identifier(&name));

        let too_long = "a".repeat(64);
        assert!(!is_valid_identifier(&too_long));
    }

    #[test]
    fn test_valid_identifier_single_char() {
        assert!(is_valid_identifier("a"));
        assert!(is_valid_identifier("_"));
    }

    #[test]
    fn test_valid_identifier_with_digits() {
        assert!(is_valid_identifier("schema_v2"));
        assert!(is_valid_identifier("_123"));
        assert!(is_valid_identifier("a1b2c3"));
    }

    #[test]
    fn test_valid_identifier_sql_injection_attempts() {
        assert!(!is_valid_identifier("schema; DROP TABLE"));
        assert!(!is_valid_identifier("schema' OR '1'='1"));
        assert!(!is_valid_identifier("schema--comment"));
        assert!(!is_valid_identifier("schema/*comment*/"));
        assert!(!is_valid_identifier("schema.other"));
    }

    #[test]
    fn test_valid_identifier_unicode() {
        assert!(!is_valid_identifier("schéma"));
        assert!(!is_valid_identifier("表"));
    }

    #[test]
    fn test_valid_identifier_uppercase_rejected() {
        assert!(!is_valid_identifier("Schema"));
        assert!(!is_valid_identifier("SCHEMA"));
        assert!(!is_valid_identifier("schemA"));
    }
}
