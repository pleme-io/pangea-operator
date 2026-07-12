//! State storage operations for OpenTofu state.
//!
//! Provides read/write access to OpenTofu state stored in PostgreSQL,
//! following the pg backend table format.

use super::schema::is_valid_identifier;
use crate::error::{Error, Result};
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use std::sync::Arc;
use tracing::{debug, info};

/// Manages OpenTofu state in PostgreSQL.
#[derive(Clone)]
pub struct StateStore {
    pool: Arc<PgPool>,
}

/// Represents a stored state entry.
#[derive(Debug, Clone)]
pub struct StateEntry {
    pub id: i64,
    pub name: String,
    pub data: Option<Vec<u8>>,
    pub created_at: DateTime<Utc>,
}

/// Parsed Terraform/OpenTofu state.
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct TerraformState {
    pub version: u32,
    pub terraform_version: String,
    pub serial: u64,
    pub lineage: String,
    #[serde(default)]
    pub outputs: serde_json::Value,
    #[serde(default)]
    pub resources: Vec<serde_json::Value>,
}

impl StateStore {
    /// Create a new state store.
    pub fn new(pool: Arc<PgPool>) -> Self {
        Self { pool }
    }

    /// Assemble + validate the double-quoted `"{schema}"."{template}_states"`
    /// table identifier for a `(schema, template)` pair.
    ///
    /// `schema_name` / `template_name` are k8s object names — they use
    /// `[a-z0-9.-]`, and hyphens/dots are NOT valid *unquoted* SQL
    /// identifier characters, which is exactly why every call site here
    /// used to build the table name via unquoted `format!()`
    /// interpolation (`{schema_name}.{template_name}_states` dropped
    /// straight into the query string) — any k8s name is SQL-identifier
    /// syntax the moment it's not quoted, and a name containing `;` or an
    /// embedded quote could break out of the identifier position
    /// entirely. The sibling `StateBackend` impls
    /// (`ArtifactStore::live_state_schema`, `TofuPgStateBackend::states_table`)
    /// already close this class by quoting; this mirrors that pattern for
    /// `StateStore`'s own `schema."table"` shape (its table is a
    /// `{template}_states` table INSIDE the schema, distinct from
    /// `TofuPgStateBackend`'s folded `"{schema}_{template}_states".states`
    /// layout).
    ///
    /// Same injection guard as `live_state_schema`: validate the
    /// sanitized projection (`-`/`.` → `_`) through [`is_valid_identifier`]
    /// — a name that still fails after that substitution carries a
    /// character no k8s name can (an embedded quote, semicolon,
    /// whitespace, control char), and is REJECTED outright (`Err`), never
    /// silently sanitized. The subsequent `.replace('"', "\"\"")` is
    /// belt-and-suspenders defense-in-depth: since `"` already fails
    /// `is_valid_identifier`, no accepted input can reach it carrying an
    /// unescaped quote, but it keeps the assembled identifier well-formed
    /// even if that guard's contract ever loosens.
    fn qualified_state_table(schema_name: &str, template_name: &str) -> Result<String> {
        let sanitize = |s: &str| s.replace(['-', '.'], "_");
        if !is_valid_identifier(&sanitize(schema_name)) {
            return Err(Error::Config(format!(
                "invalid schema identifier for state table: {schema_name}"
            )));
        }
        let table = format!("{template_name}_states");
        if !is_valid_identifier(&sanitize(&table)) {
            return Err(Error::Config(format!(
                "invalid template identifier for state table: {template_name}"
            )));
        }
        let schema_q = schema_name.replace('"', "\"\"");
        let table_q = table.replace('"', "\"\"");
        Ok(format!("\"{schema_q}\".\"{table_q}\""))
    }

    /// Get the current state for a template.
    pub async fn get_state(
        &self,
        schema_name: &str,
        template_name: &str,
        state_name: &str,
    ) -> Result<Option<StateEntry>> {
        let table_name = Self::qualified_state_table(schema_name, template_name)?;

        let query = format!(
            r#"
            SELECT id, name, data, created_at
            FROM {}
            WHERE name = $1
            ORDER BY id DESC
            LIMIT 1
            "#,
            table_name
        );

        let row: Option<(i64, String, Option<Vec<u8>>, DateTime<Utc>)> = sqlx::query_as(&query)
            .bind(state_name)
            .fetch_optional(self.pool.as_ref())
            .await
            .map_err(|e| Error::Database(e))?;

        Ok(row.map(|(id, name, data, created_at)| StateEntry {
            id,
            name,
            data,
            created_at,
        }))
    }

    /// Get and parse the Terraform state.
    pub async fn get_parsed_state(
        &self,
        schema_name: &str,
        template_name: &str,
        state_name: &str,
    ) -> Result<Option<TerraformState>> {
        let entry = self.get_state(schema_name, template_name, state_name).await?;

        match entry {
            Some(e) if e.data.is_some() => {
                let data = e.data.unwrap();
                let state: TerraformState = serde_json::from_slice(&data)
                    .map_err(|e| Error::Serialization(e))?;
                Ok(Some(state))
            }
            _ => Ok(None),
        }
    }

    /// Save state for a template.
    pub async fn save_state(
        &self,
        schema_name: &str,
        template_name: &str,
        state_name: &str,
        data: &[u8],
    ) -> Result<i64> {
        let table_name = Self::qualified_state_table(schema_name, template_name)?;

        debug!(
            table_name,
            state_name,
            data_size = data.len(),
            "Saving state"
        );

        // Use upsert to handle both insert and update
        let query = format!(
            r#"
            INSERT INTO {} (name, data)
            VALUES ($1, $2)
            ON CONFLICT (name) DO UPDATE SET
                data = EXCLUDED.data,
                created_at = NOW()
            RETURNING id
            "#,
            table_name
        );

        let row: (i64,) = sqlx::query_as(&query)
            .bind(state_name)
            .bind(data)
            .fetch_one(self.pool.as_ref())
            .await
            .map_err(|e| Error::Database(e))?;

        info!(
            table_name,
            state_name,
            id = row.0,
            "State saved"
        );

        Ok(row.0)
    }

    /// Delete state for a template.
    pub async fn delete_state(
        &self,
        schema_name: &str,
        template_name: &str,
        state_name: &str,
    ) -> Result<bool> {
        let table_name = Self::qualified_state_table(schema_name, template_name)?;

        let query = format!("DELETE FROM {} WHERE name = $1", table_name);

        let result = sqlx::query(&query)
            .bind(state_name)
            .execute(self.pool.as_ref())
            .await
            .map_err(|e| Error::Database(e))?;

        Ok(result.rows_affected() > 0)
    }

    /// List all states for a template.
    pub async fn list_states(
        &self,
        schema_name: &str,
        template_name: &str,
    ) -> Result<Vec<StateEntry>> {
        let table_name = Self::qualified_state_table(schema_name, template_name)?;

        let query = format!(
            r#"
            SELECT id, name, data, created_at
            FROM {}
            ORDER BY created_at DESC
            "#,
            table_name
        );

        let rows: Vec<(i64, String, Option<Vec<u8>>, DateTime<Utc>)> = sqlx::query_as(&query)
            .fetch_all(self.pool.as_ref())
            .await
            .map_err(|e| Error::Database(e))?;

        Ok(rows
            .into_iter()
            .map(|(id, name, data, created_at)| StateEntry {
                id,
                name,
                data,
                created_at,
            })
            .collect())
    }

    /// Get resource count from state.
    pub async fn get_resource_count(
        &self,
        schema_name: &str,
        template_name: &str,
        state_name: &str,
    ) -> Result<u32> {
        match self.get_parsed_state(schema_name, template_name, state_name).await? {
            Some(state) => Ok(state.resources.len() as u32),
            None => Ok(0),
        }
    }

    /// Get state outputs.
    pub async fn get_outputs(
        &self,
        schema_name: &str,
        template_name: &str,
        state_name: &str,
    ) -> Result<Option<serde_json::Value>> {
        match self.get_parsed_state(schema_name, template_name, state_name).await? {
            Some(state) => Ok(Some(state.outputs)),
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn qualified_state_table_quotes_k8s_hyphenated_names() {
        // The bug this guards: the old `format!("{}.{}_states", ...)`
        // dropped these hyphenated k8s names straight into the query as
        // UNQUOTED SQL identifiers. A bare hyphen inside an unquoted
        // Postgres identifier is a parse error (it's the subtraction
        // operator), so this exact realistic input never even reached the
        // database cleanly before — it's not just an injection risk, it's
        // a straightforwardly broken query for any real template name.
        let t =
            StateStore::qualified_state_table("pangea_cloudflare-pleme", "cloudflare-pleme").unwrap();
        assert_eq!(t, "\"pangea_cloudflare-pleme\".\"cloudflare-pleme_states\"");
    }

    #[test]
    fn qualified_state_table_allows_dotted_k8s_names() {
        let t = StateStore::qualified_state_table("pangea_a.b", "c.d").unwrap();
        assert_eq!(t, "\"pangea_a.b\".\"c.d_states\"");
    }

    #[test]
    fn qualified_state_table_rejects_embedded_quotes() {
        // `"` isn't a lowercase/digit/underscore, so `is_valid_identifier`
        // refuses it before the (defense-in-depth) doubling step is ever
        // reached — a stricter outcome than merely escaping it through.
        assert!(StateStore::qualified_state_table("pangea_x\"y", "t").is_err());
    }

    /// The load-bearing security property: no schema/template name a k8s
    /// object can carry lets a caller break out of the identifier
    /// position and inject arbitrary SQL. Every one of these strings
    /// would have flowed straight into a `format!("... FROM {} ...")`
    /// query string unquoted and unchecked before this fix.
    #[test]
    fn qualified_state_table_rejects_injection_attempts() {
        assert!(StateStore::qualified_state_table("a\"b", "t").is_err());
        assert!(StateStore::qualified_state_table("a; DROP TABLE x", "t").is_err());
        assert!(StateStore::qualified_state_table("a b", "t").is_err());
        assert!(StateStore::qualified_state_table("a", "t' OR '1'='1").is_err());
        assert!(StateStore::qualified_state_table("a", "t; DROP SCHEMA public CASCADE").is_err());
    }

    #[test]
    fn qualified_state_table_accepts_plain_lowercase_names() {
        let t = StateStore::qualified_state_table("pangea_prod", "my_template").unwrap();
        assert_eq!(t, "\"pangea_prod\".\"my_template_states\"");
    }
}
